package cluster

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/utils"
)

// Snapshot k8s volumes
func (c *Cluster) Snapshot(
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

	if snapshot == nil {
		return fmt.Errorf("nil snapshot passed")
	}

	ctx := context.TODO()
	// Snapshot log
	blog := utils.NewNamedLog("snapshot:" + snapshot.ObjectMeta.Name)

	// get clusterId (= UID of kube-system namespace)
	clusterID, err := getNamespaceUID(ctx, "kube-system", kubeClient)
	if err != nil {
		// This is the first time that k8s api of target cluster accessed
		if apiPermError(err.Error()) {
			return backoff.Permanent(fmt.Errorf("Getting clusterId(=kube-system UID) failed : %s", err.Error()))
		}
		return fmt.Errorf("Getting clusterId(=kube-system UID) failed : %s", err.Error())
	}
	snapshot.Spec.ClusterId = clusterID
	resticPassword := utils.MakePassword(clusterID, 16)

	// get 1h valid credentials to access objectstore
	creds, err := bucket.CreateAssumeRole(clusterID, 3600)
	if err != nil {
		// This is the first time that objectstore accessed
		if objectstorePermError(err.Error()) {
			return backoff.Permanent(fmt.Errorf("Getting temporaly credentials failed (do not retry) : %s", err.Error()))
		}
		return fmt.Errorf("Getting temporaly credentials failed (do retry) : %s", err.Error())
	}

	blog.Infof("Backing up volumes from cluster:%s", clusterID)

	// prepare user restic / admin restic
	r := NewRestic(bucket.GetEndpoint(), bucket.GetBucketName(), clusterID, resticPassword,
		*creds.AccessKeyId, *creds.SecretAccessKey, *creds.SessionToken,
		snapshot.GetNamespace(), snapshot.GetName())
	adminRestic := NewRestic(bucket.GetEndpoint(), bucket.GetBucketName(), clusterID, resticPassword,
		bucket.GetAccessKey(), bucket.GetSecretKey(), "",
		snapshot.GetNamespace(), snapshot.GetName())

	// check repository
	chkJob := adminRestic.resticJobListSnapshots()
	_, err = DoResticJob(ctx, chkJob, localKubeClient, 5)
	if err != nil {
		if strings.Contains(err.Error(), "specified key does not exist") {
			// first snapshot for the cluster, create repository
			initJob := adminRestic.resticJobInit()
			_, err = DoResticJob(ctx, initJob, localKubeClient, 5)
			if err != nil {
				return fmt.Errorf("Initializing repository failed : %s", err.Error())
			}
		} else {
			return fmt.Errorf("Checking repository failed : %s", err.Error())
		}
	}

	// Get PVCs list
	pvcs, err := kubeClient.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("Error getting PVC list : %s", err.Error())
	}

	// snapshot status init
	snapshot.Status.TotalBytes = 0
	snapshot.Status.TotalFiles = 0
	snapshot.Status.NumVolumeClaims = 0
	snapshot.Status.ReadyVolumeClaims = 0
	snapshot.Status.SkippedClaims = 0
	snapshot.Status.SnapshotStartTime = metav1.Now()

	// take snapshots of PVCs
	for i, pvc := range pvcs.Items {

		blog.Infof("Backing up pvc : %s/%s", pvc.GetNamespace(), pvc.GetName())

		// skip not bound PVs
		if pvc.Status.Phase != "Bound" {
			snapshot.Status.SkippedClaims++
			snapshot.Status.SkippedMessages = append(
				snapshot.Status.SkippedMessages,
				fmt.Sprintf("%s/%s - PVC not bound", pvc.GetNamespace(), pvc.GetName()),
			)
			blog.Infof("  Skipping - PVC not bound")
			continue
		}

		// get pv
		pv, err := kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			snapshot.Status.SkippedClaims++
			snapshot.Status.SkippedMessages = append(
				snapshot.Status.SkippedMessages,
				fmt.Sprintf("%s/%s - Error getting PV : %s", pvc.GetNamespace(), pvc.GetName(), err.Error()),
			)
			blog.Infof("  Skipping - Error getting PV : %s", err.Error())
			continue
		}

		// get mounted node
		nodeName, err := getPVCMountedNode(ctx, &pvcs.Items[i], kubeClient)
		if err != nil {
			snapshot.Status.SkippedClaims++
			snapshot.Status.SkippedMessages = append(
				snapshot.Status.SkippedMessages,
				fmt.Sprintf("%s/%s - PV not in use : %s", pvc.GetNamespace(), pvc.GetName(), err.Error()),
			)
			blog.Infof("  Skipping - PV not in use : %s", err.Error())
			continue
		}
		blog.Infof("  Mounted node : %s", nodeName)

		// Glob volume path
		globJob := r.globberJob(pvc.Spec.VolumeName, nodeName)
		volumePath, err := DoResticJob(ctx, globJob, kubeClient, 5)
		if err != nil {
			snapshot.Status.SkippedClaims++
			snapshot.Status.SkippedMessages = append(
				snapshot.Status.SkippedMessages,
				fmt.Sprintf("%s/%s - Error globbing volume path : %s", pvc.GetNamespace(), pvc.GetName(), err.Error()),
			)
			blog.Infof("  Skipping - Error globbing volume path : %s", err.Error())
			continue
		}
		blog.Infof("  Volume Path : %s", volumePath)

		// take snapshot
		volumePath = strings.TrimSuffix(volumePath, "\n")
		if pv.Spec.CSI != nil {
			volumePath = volumePath + "/mount"
		}
		snapJob := r.resticJobBackup(pvc.Spec.VolumeName, volumePath, nodeName)
		output, err := DoResticJob(ctx, snapJob, kubeClient, 30)
		if err != nil {
			snapshot.Status.SkippedClaims++
			snapshot.Status.SkippedMessages = append(
				snapshot.Status.SkippedMessages,
				fmt.Sprintf("%s/%s - Error taking snapshot : %s", pvc.GetNamespace(), pvc.GetName(), err.Error()),
			)
			blog.Warningf("!! Error taking snapshot PVC %s : %s", pvc.GetName(), err.Error())
			continue
		}

		// Perse backup summary
		jsonBytes := []byte(output)
		summary := new(ResticBackupSummary)
		err = json.Unmarshal(jsonBytes, summary)
		if err != nil {
			snapshot.Status.SkippedClaims++
			snapshot.Status.SkippedMessages = append(
				snapshot.Status.SkippedMessages,
				fmt.Sprintf("%s/%s - Error persing restic backup summary : %s", pvc.GetNamespace(), pvc.GetName(), err.Error()),
			)
			blog.Warningf("!! Error persing restic backup summary : %s", err.Error())
			continue
		}

		snapPvc := vsv1alpha1.VolumeClaim{
			Name:          pvc.GetName(),
			Namespace:     pvc.GetNamespace(),
			ClaimSpec:     pvc.Spec,
			SnapshotId:    summary.SnapshotID,
			SnapshotSize:  summary.TotalBytesProcessed,
			SnapshotFiles: summary.TotalFilesProcessed,
			SnapshotTime:  metav1.Now(),
			SnapshotReady: true,
		}

		blog.Infof("-- completed")

		snapshot.Status.ReadyVolumeClaims++
		snapshot.Status.TotalBytes += snapPvc.SnapshotSize
		snapshot.Status.TotalFiles += snapPvc.SnapshotFiles
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
	defer func() { _ = tgz.Close() }()

	tarWriter := tar.NewWriter(tgz)
	defer func() { _ = tarWriter.Close() }()

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

	_ = tarWriter.Close()
	_ = tgz.Close()
	_ = snapshotFile.Close()

	// upload custom resource json
	snapshotUploadFile, err := os.Open("/tmp/" + snapshot.ObjectMeta.Name + ".tgz")
	defer func() { _ = snapshotUploadFile.Close() }()
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
	blog.Infof("-- skipped claims      : %d", snapshot.Status.SkippedClaims)
	blog.Infof("-- total files         : %d", snapshot.Status.TotalFiles)
	blog.Infof("-- total bytes         : %d", snapshot.Status.TotalBytes)

	return nil
}

func getPVCMountedNode(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	kubeClient kubernetes.Interface) (string, error) {

	// check volume attachment
	attaches, err := kubeClient.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("Error getting volumeattachments : %s", err.Error())
	}
	for _, attach := range attaches.Items {
		if *attach.Spec.Source.PersistentVolumeName == pvc.Spec.VolumeName {
			if attach.Status.Attached {
				return attach.Spec.NodeName, nil
			}
		}
	}

	// Check mounts not informed in volumeattachments
	pods, err := kubeClient.CoreV1().Pods(pvc.GetNamespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("Error getting pods : %s", err.Error())
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != "Running" {
			continue
		}
		volumeName := ""
		for _, vol := range pod.Spec.Volumes {
			if vol.VolumeSource.PersistentVolumeClaim != nil &&
				vol.VolumeSource.PersistentVolumeClaim.ClaimName == pvc.GetName() {
				volumeName = vol.Name
				break
			}
		}
		if volumeName != "" {
			for _, container := range pod.Spec.Containers {
				for _, mount := range container.VolumeMounts {
					if mount.Name == volumeName {
						return pod.Spec.NodeName, nil
					}
				}
			}
		}
	}

	// Mount not found
	return "", fmt.Errorf("Mount not found")
}
