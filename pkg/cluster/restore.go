package cluster

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-snap/pkg/objectstore"
	"github.com/ryo-watanabe/k8s-snap/pkg/utils"
)

// Restore k8s resources
func Restore(
	restore *vsv1alpha1.Restore,
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {

	// kubeClient for external cluster.
	kubeClient, err := buildKubeClient(restore.Spec.Kubeconfig)
	if err != nil {
		return err
	}

	return restoreResources(restore, snapshot, kubeClient, localKubeClient)
}

func restoreResources(
	restore *cbv1alpha1.Restore,
	snapshot *vsv1alpha1.VolumeSnapshot,
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
	resticPassword := util.MakePassword(clusterId, 16)

	rlog.Info("Restoring volumes on cluster:%s from snapshot:%s", restoreClusterId, clusterId)

	// get 1h valid credentials to access objectstore
	creds, err := bucket.CreateAssumeRole(clusterId, 7200)
	if err != nil {
		return fmt.Errorf("Getting tempraly credentials failed : %s", err.Error())
	}

	// prepare user restic / admin restic
	r := restic.NewRestic(bucket.GetEndpoint(), bucket.GetBucketName(), clusterId, resticPassword,
		*creds.AccessKeyId, *creds.SecretAccessKey, *creds.SessionToken,
		snapshot.GetNamespace(), snapshot.GetName())
	adminRestic := restic.NewRestic(bucket.GetEndpoint(), bucket.GetBucketName(), clusterId, resticPassword,
		bucket.AccessKey, bucket.SecretKey, "",
		snapshot.GetNamespace(), snapshot.GetName())

	// get snapshot list
	chkJob := adminRestic.resticJobListSnapshots()
	output, err = restic.DoResticJob(chkJob, localKubeClient, 5)
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

	restore.Status.NumVolumeClaims = len(snapshot.Spec.VolumeClaims)

	// restore volumes
	for _, snapPvc := range(snapshot.Spec.VolumeClaims) {

		blog.Infof("Restoring pvc : %s/%s", snapPvc.Spec.Namespace, snapPvc.Spec.Name)

		// check snapshot exists
		var snap *ResticSnapshot = nil
		for _, snp := range(*snapshotList) {
			if snp.ShortId == snapPvc.snapshotId {
				snap = &snp
				break
			}
		}
		if snap == nil {
			volumeRestoreFailedWith(
				fmt.Errorf("Snapshot %d not found", snapPvc.snapshotId),
				restore, snapPvc.Name, snapPvc.Namespace)
			continue
		}

		// check namespace exists
		_, err := kubeClient.CoreV1().Namespaces().Get(snapPvc.Spec.Namespace, metav1.GetOptions{})
		if err != nil {
			// create namespace if not exist
			if errors.IsNotFound(err) {
				rlog.Info("Creating namespace:%s", snapPvc.Spec.Namespace)
				newNs := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: snapPvc.Spec.Namespace,
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
				Name: snapPvc.Spec.Name,
				Namespace: snapPvc.Spec.Namespace,
			},
			Spec: snapPvc.ClaimSpec,
		}
		_, err = kubeClient.CoreV1().PersistentVolumeClaims(snapPvc.Spec.Namespace).Create(newNs)
		if err != nil {
			volumeRestoreFailedWith(
				fmt.Errorf("Creating PVC failed : %s", err.Error()),
				restore, snapPvc.Name, snapPvc.Namespace)
			continue
		}

		// wait for pvc bound
		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = time.Duration(30) * time.Minute
		b.RandomizationFactor = 0.2
		b.Multiplier = 2.0
		b.InitialInterval = time.Duration(5) * time.Second
		chkPvcBound := func() error {
			chkPvc, err := kubeClient.PersistentVolumeClaims(snapPvc.Spec.Namespace).Get(snapPvc.Spec.Name, metav1.GetOptions{})
			if err != nil {
				return backoff.Permanent(err)
			}
			if chkPvc.Status.Phase == "Bound" {
				return nil
			} else if chkPvc.Status.Phase == "Pending" {
				return fmt.Errorf("PVC %s is Pending", snapPvc.Spec.Name)
			}
			return backoff.Permanent(fmt.Errorf("Unknown phase on bound PVC %s", chkPvc.Status.Phase))
		}
		err = backoff.RetryNotify(chkPvcBound, b, retryNotifyPvc)
		if err != nil {
			volumeRestoreFailedWith(
				fmt.Errorf("Bound PVC failed : %s", err.Error()),
				restore, snapPvc.Name, snapPvc.Namespace)
			continue
		}

		// restic restore job
		restoreJob := r.resticJobRestore(snapPvc.SnapshotId, snap.GetSourceVolumeId(), snapPvc.Name, snapPvc.Namespace)
		output, err = restic.DoResticJob(restoreJob, kubeClient, 30)
		if err != nil {
			volumeRestoreFailedWith(
				fmt.Errorf("Error running restore snapshot job : %s : %s", err.Error(), output),
				restore, snapPvc.Name, snapPvc.Namespace)
			continue
		}
	}

	// Timestamp and resource version
	restore.Status.RestoreTimestamp = metav1.Now()

	// result
	rlog.Info("Restore completed")
	rlog.Infof("-- timestamp         : %s", restore.Status.RestoreTimestamp)
	rlog.Infof("-- available until   : %s", restore.Status.AvailableUntil)
	rlog.Infof("-- num volumes       : %d", restore.Status.NumVolumeClaims)
	rlog.Infof("-- num failed volume : %d", restore.Status.NumFailedVolumeClaims)
	for _, failed := range(restore.Status.FailedVolumeClaims) {
		rlog.Infof("----- %s", failed)
	}

	return nil
}

func volumeRestoreFailedWith(err error, restore *cbv1alpha1.Restore, name, namespace string) {
	rlog.Warning(err.Error())
	restore.Status.FailedVolumeClaims = append(restore.Status.FailedVolumeClaims, namespace + "/" + name + ":" + err.Error())
	restore.Status.NumFailedVolumeClaims++
}
