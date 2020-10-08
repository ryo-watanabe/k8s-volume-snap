package cluster

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//corev1 "k8s.io/api/core/v1"
	"github.com/cenkalti/backoff"
	"k8s.io/client-go/kubernetes"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/utils"
)

// k8s api errors not to retry
var apiPermErrors = []string{
	"Unauthorized",
}
func apiPermError(error string) bool {
	for _, e := range apiPermErrors {
		if strings.Contains(error, e) {
			return true
		}
	}
	return false
}

// Object store errors not to retry
var obstPermErrors = []string{
	"SignatureDoesNotMatch",
	"InvalidAccessKeyId",
	"NoSuchBucket",
}
func objectstorePermError(error string) bool {
	for _, e := range obstPermErrors {
		if strings.Contains(error, e) {
			return true
		}
	}
	return false
}

// Snapshot k8s volumes
func Snapshot(
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {

	// kubeClient for external cluster.
	kubeClient, err := buildKubeClient(snapshot.Spec.Kubeconfig)
	if err != nil {
		return err
	}

	return snapshotVolumes(snapshot, bucket, kubeClient, localKubeClient)
}

func snapshotVolumes(
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	kubeClient, localKubeClient kubernetes.Interface) error {

	// Snapshot log
	blog := utils.NewNamedLog("snapshot:" + snapshot.ObjectMeta.Name)

	// get clusterId (= UID of kube-system namespace)
	clusterId, err := getNamespaceUID("kube-system", kubeClient)
	if err != nil {
		// This is the first time that k8s api of target cluster accessed
		if apiPermError(err.Error()) {
			return backoff.Permanent(fmt.Errorf("Getting clusterId(=kube-system UID) failed : %s", err.Error()))
		}
		return fmt.Errorf("Getting clusterId(=kube-system UID) failed : %s", err.Error())
	}
	snapshot.Spec.ClusterId = clusterId
	resticPassword := utils.MakePassword(clusterId, 16)

	// get 1h valid credentials to access objectstore
	creds, err := bucket.CreateAssumeRole(clusterId, 3600)
	if err != nil {
		// This is the first time that objectstore accessed
		if objectstorePermError(err.Error()) {
			return backoff.Permanent(fmt.Errorf("Getting tempraly credentials failed : %s", err.Error()))
		}
		return fmt.Errorf("Getting tempraly credentials failed : %s", err.Error())
	}

	blog.Infof("Backing up volumes from cluster:%s", clusterId)

	// prepare user restic / admin restic
	r := NewRestic(bucket.GetEndpoint(), bucket.GetBucketName(), clusterId, resticPassword,
		*creds.AccessKeyId, *creds.SecretAccessKey, *creds.SessionToken,
		snapshot.GetNamespace(), snapshot.GetName())
	adminRestic := NewRestic(bucket.GetEndpoint(), bucket.GetBucketName(), clusterId, resticPassword,
		bucket.GetAccessKey(), bucket.GetSecretKey(), "",
		snapshot.GetNamespace(), snapshot.GetName())

	// check repository
	chkJob := adminRestic.resticJobListSnapshots()
	_, err = DoResticJob(chkJob, localKubeClient, 5)
	if err != nil {
		if strings.Contains(err.Error(), "specified key does not exist") {
			// first snapshot for the cluster, create repository
			initJob := adminRestic.resticJobInit()
			_, err = DoResticJob(initJob, localKubeClient, 5)
			if err != nil {
				return fmt.Errorf("Initializing repository failed : %s", err.Error())
			}
		} else {
			return fmt.Errorf("Checking repository failed : %s", err.Error())
		}
	}

	// Get PVCs list
	pvcs, err := kubeClient.CoreV1().PersistentVolumeClaims("").List(metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("Error getting PVC list : %s", err.Error())
	}

	// Get nodes list
	nodes, err := kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("Error getting node list : %s", err.Error())
	}

	snapshot.Status.SnapshotStartTime = metav1.Now()

	// take snapshots of PVCs
	for _, pvc := range(pvcs.Items) {

		blog.Infof("Backing up pvc : %s/%s", pvc.GetNamespace(), pvc.GetName())

		// skip not bound PVs
		if pvc.Status.Phase != "Bound" {
			blog.Infof("Skipping - PVC not bound")
			continue
		}

		// get pv
		pv, err := kubeClient.CoreV1().PersistentVolumes().Get(pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			blog.Infof("Skipping - Error getting PV : %s", err.Error())
			continue
		}

		// glob pv mount path on node
		volumePath := ""
		nodeName := ""
		for _, node := range(nodes.Items) {
			globJob := r.globberJob(pvc.Spec.VolumeName, node.GetName())
			volumePath, err = DoResticJob(globJob, localKubeClient, 5)
			if err == nil {
				nodeName = node.GetName()
				break
			}
		}
		if nodeName == "" {
			blog.Infof("Skipping - PV not mounted on any node (not in use)")
			continue
		}

		// take snapshot
		snapPvc := vsv1alpha1.VolumeClaim{
			Name: pvc.GetName(),
			Namespace: pvc.GetNamespace(),
			ClaimSpec: pvc.Spec,
			SnapshotReady: false,
		}
		if pv.Spec.CSI != nil {
			volumePath = volumePath + "/mount"
		}
		snapJob := r.resticJobBackup(pvc.Spec.VolumeName, volumePath, nodeName)
		output, err := DoResticJob(snapJob, kubeClient, 30)
		if err != nil {
			blog.Warningf("Error taking snapshot PVC %s : %s", pvc.GetName(), err.Error())
		} else {
			// Perse backup summary
			jsonBytes := []byte(output)
			summary := new(ResticBackupSummary)
			err = json.Unmarshal(jsonBytes, summary)
			if err != nil {
				blog.Warningf("Error persing restic backup summary : %s", err.Error())
			}
			snapPvc.SnapshotId = summary.SnapshotId
			snapPvc.SnapshotSize = summary.TotalBytesProcessed
			snapPvc.SnapshotTime = metav1.Now()
			snapPvc.SnapshotReady = true
			blog.Infof("Backing up pvc completed : %s/%s", pvc.GetNamespace(), pvc.GetName())
			snapshot.Status.ReadyVolumeClaims++
		}
		snapshot.Spec.VolumeClaims = append(snapshot.Spec.VolumeClaims, snapPvc)
		snapshot.Status.NumVolumeClaims++
	}

	snapshot.Status.SnapshotEndTime = metav1.Now()

	// prerare file for backup custom resource
	snapshotFile, err := os.Create("/tmp/" + snapshot.ObjectMeta.Name + ".tgz")
	if err != nil {
		return fmt.Errorf("Creating tgz file failed : %s", err.Error())
	}
	tgz := gzip.NewWriter(snapshotFile)
	defer tgz.Close()

	tarWriter := tar.NewWriter(tgz)
	defer tarWriter.Close()

	blog.Info("Making volumesnapshot.json")

	snapshotCopy := snapshot.DeepCopy()
	snapshotCopy.Status.Phase = ""
	snapshotCopy.TypeMeta.SetGroupVersionKind(vsv1alpha1.SchemeGroupVersion.WithKind("VolumeSnapshot"))
	snapshotCopy.ObjectMeta.SetResourceVersion("")
	snapshotCopy.ObjectMeta.SetUID("")

	// Store volumesnapshot resource as volumesnapshot.json
	snapshotResource, err := json.Marshal(snapshotCopy)
	if err != nil {
		return fmt.Errorf("Marshalling volumesnapshot.json failed : %s", err.Error())
	}
	hdr := &tar.Header{
		Name:     filepath.Join(snapshot.ObjectMeta.Name, "volumesnapshot.json"),
		Size:     int64(len(snapshotResource)),
		Typeflag: tar.TypeReg,
		Mode:     0755,
		ModTime:  time.Now(),
	}
	if err := tarWriter.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar writer volumesnapshot.json header failed : %s", err.Error())
	}
	if _, err := tarWriter.Write(snapshotResource); err != nil {
		return fmt.Errorf("tar writer volumesnapshot.json content failed : %s", err.Error())
	}

	tarWriter.Close()
	tgz.Close()
	snapshotFile.Close()

	// upload custom resource json
	snapshotUploadFile, err := os.Open("/tmp/" + snapshot.ObjectMeta.Name + ".tgz")
	defer snapshotUploadFile.Close()
	if err != nil {
		return backoff.Permanent(fmt.Errorf("Re-opening tgz file failed : %s", err.Error()))
	}
	blog.Infof("Uploading file %s", snapshot.ObjectMeta.Name+".tgz")
	err = bucket.Upload(snapshotUploadFile, snapshot.ObjectMeta.Name+".tgz")
	if err != nil {
		return fmt.Errorf("Uploading tgz file failed : %s", err.Error())
	}

	// Timestamps and size
	blog.Info("VolumeSnapshot completed")
	blog.Infof("-- snapshot start time : %s", snapshot.Status.SnapshotStartTime)
	blog.Infof("-- snapshot end time   : %s", snapshot.Status.SnapshotEndTime)
	blog.Infof("-- num volume claims   : %d", snapshot.Status.NumVolumeClaims)
	blog.Infof("-- ready volume claims : %d", snapshot.Status.ReadyVolumeClaims)

	return nil
}
