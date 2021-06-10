package cluster

import (
	"fmt"
	"encoding/json"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"github.com/cenkalti/backoff"
	"k8s.io/klog/v2"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/utils"
)

// Restore k8s resources
func (c *Cluster) Restore(
	restore *vsv1alpha1.VolumeRestore,
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {

	// kubeClient for external cluster.
	kubeClient, err := buildKubeClient(restore.Spec.Kubeconfig)
	if err != nil {
		return err
	}

	return restoreResources(restore, snapshot, bucket, kubeClient, localKubeClient)
}

func restoreResources(
	restore *vsv1alpha1.VolumeRestore,
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	kubeClient kubernetes.Interface,
	localKubeClient kubernetes.Interface) error {

	// Restore log
	rlog := utils.NewNamedLog("restore:" + restore.ObjectMeta.Name)

	// get clusterId (= UID of kube-system namespace)
	restoreClusterId, err := getNamespaceUID("kube-system", kubeClient)
	if err != nil {

		// This is the first time that k8s api of target cluster accessed
		if apiPermError(err.Error()) {
			return backoff.Permanent(fmt.Errorf("Getting clusterId(=kube-system UID) failed : %s", err.Error()))
		}
		return fmt.Errorf("Getting clusterId(=kube-system UID) failed : %s", err.Error())
	}
	clusterId := snapshot.Spec.ClusterId
	resticPassword := utils.MakePassword(clusterId, 16)

	rlog.Infof("Restoring volumes on cluster:%s from snapshot:%s", restoreClusterId, clusterId)

	// get 1h valid credentials to access objectstore
	creds, err := bucket.CreateAssumeRole(clusterId, 7200)
	if err != nil {
		return fmt.Errorf("Getting temporaly credentials failed : %s", err.Error())
	}

	// prepare user restic / admin restic
	r := NewRestic(bucket.GetEndpoint(), bucket.GetBucketName(), clusterId, resticPassword,
		*creds.AccessKeyId, *creds.SecretAccessKey, *creds.SessionToken,
		snapshot.GetNamespace(), snapshot.GetName())
	adminRestic := NewRestic(bucket.GetEndpoint(), bucket.GetBucketName(), clusterId, resticPassword,
		bucket.GetAccessKey(), bucket.GetSecretKey(), "",
		snapshot.GetNamespace(), snapshot.GetName())

	// get snapshot list
	chkJob := adminRestic.resticJobListSnapshots()
	output, err := DoResticJob(chkJob, localKubeClient, 5)
	if err != nil {
		return fmt.Errorf("List snapshots failed : %s", err.Error())
	}
	// Perse snapshot list
	jsonBytes := []byte(output)
	snapshotList := new([]ResticSnapshot)
	err = json.Unmarshal(jsonBytes, snapshotList)
	if err != nil {
		return fmt.Errorf("Error persing restic snapshot : %s : %s", err.Error(), output)
	}

	restore.Status.NumVolumeClaims = int32(len(snapshot.Spec.VolumeClaims))

	// restore volumes
	for _, snapPvc := range(snapshot.Spec.VolumeClaims) {

		rlog.Infof("Restoring pvc : %s/%s", snapPvc.Namespace, snapPvc.Name)

		// check snapshot exists
		var snap *ResticSnapshot = nil
		for _, snp := range(*snapshotList) {
			if snp.ShortId == snapPvc.SnapshotId {
				snap = &snp
				break
			}
		}
		if snap == nil {
			volumeRestoreFailedWith(
				fmt.Errorf("Snapshot %s not found", snapPvc.SnapshotId),
				restore, snapPvc.Name, snapPvc.Namespace, rlog)
			continue
		}

		// check namespace exists
		_, err := kubeClient.CoreV1().Namespaces().Get(snapPvc.Namespace, metav1.GetOptions{})
		if err != nil {
			// create namespace if not exist
			if errors.IsNotFound(err) {
				rlog.Infof("Creating namespace:%s", snapPvc.Namespace)
				newNs := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: snapPvc.Namespace,
					},
				}
				_, err = kubeClient.CoreV1().Namespaces().Create(newNs)
				if err != nil {
					return fmt.Errorf("Creating namespace failed : %s", err.Error())
				}
			} else {
				return fmt.Errorf("Checking namespace failed : %s", err.Error())
			}
		}

		// create a pvc/pv to be restored
		newPvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: snapPvc.Name,
				Namespace: snapPvc.Namespace,
			},
			Spec: snapPvc.ClaimSpec,
		}
		newPvc.Spec.VolumeName = ""
		_, err = kubeClient.CoreV1().PersistentVolumeClaims(snapPvc.Namespace).Create(newPvc)
		if err != nil {
			volumeRestoreFailedWith(
				fmt.Errorf("Creating PVC failed : %s", err.Error()),
				restore, snapPvc.Name, snapPvc.Namespace, rlog)
			continue
		}

		// wait for pvc bound
		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = time.Duration(30) * time.Minute
		b.RandomizationFactor = 0.2
		b.Multiplier = 2.0
		b.InitialInterval = time.Duration(5) * time.Second
		chkPvcBound := func() error {
			chkPvc, err := kubeClient.CoreV1().PersistentVolumeClaims(snapPvc.Namespace).Get(snapPvc.Name, metav1.GetOptions{})
			if err != nil {
				return backoff.Permanent(err)
			}
			if chkPvc.Status.Phase == "Bound" {
				return nil
			} else if chkPvc.Status.Phase == "Pending" {
				return fmt.Errorf("PVC %s is Pending", snapPvc.Name)
			}
			return backoff.Permanent(fmt.Errorf("Unknown phase on bound PVC %s", chkPvc.Status.Phase))
		}
		err = backoff.RetryNotify(chkPvcBound, b, retryNotifyPvc)
		if err != nil {
			volumeRestoreFailedWith(
				fmt.Errorf("Bound PVC failed : %s", err.Error()),
				restore, snapPvc.Name, snapPvc.Namespace, rlog)
			continue
		}

		// restic restore job
		restoreJob := r.resticJobRestore(snapPvc.SnapshotId, snap.GetSourceVolumeId(), snapPvc.Name, snapPvc.Namespace)
		output, err = DoResticJob(restoreJob, kubeClient, 30)
		if err != nil {
			volumeRestoreFailedWith(
				fmt.Errorf("Error running restore snapshot job : %s : %s", err.Error(), output),
				restore, snapPvc.Name, snapPvc.Namespace, rlog)
			continue
		}
	}

	// Timestamp and resource version
	restore.Status.RestoreTimestamp = metav1.Now()

	// result
	rlog.Info("Restore completed")
	rlog.Infof("-- timestamp         : %s", restore.Status.RestoreTimestamp)
	rlog.Infof("-- num volumes       : %d", restore.Status.NumVolumeClaims)
	rlog.Infof("-- num failed volume : %d", restore.Status.NumFailedVolumeClaims)
	for _, failed := range(restore.Status.FailedVolumeClaims) {
		rlog.Infof("----- %s", failed)
	}

	return nil
}

func retryNotifyPvc(err error, wait time.Duration) {
	klog.V(4).Infof("%s : will be checked again in %.2f seconds", err.Error(), wait.Seconds())
}

func volumeRestoreFailedWith(err error, restore *vsv1alpha1.VolumeRestore,
	name, namespace string, rlog *utils.NamedLog) {

	rlog.Warning(err.Error())
	restore.Status.FailedVolumeClaims = append(restore.Status.FailedVolumeClaims, namespace + "/" + name + ":" + err.Error())
	restore.Status.NumFailedVolumeClaims++
}
