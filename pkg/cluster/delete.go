package cluster

import (
	"fmt"
	"encoding/json"

	"k8s.io/client-go/kubernetes"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/utils"
)

// DeleteSnapshot deletes a volume snapshot
func DeleteSnapshot(
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {

	return deleteVolumeSnapshots(snapshot, bucket, localKubeClient)
}

func deleteVolumeSnapshots(
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {

	// Delete log
	dlog := utils.NewNamedLog("delete:" + snapshot.ObjectMeta.Name)

	// clusterId
	clusterId := snapshot.Spec.ClusterId
	resticPassword := utils.MakePassword(clusterId, 16)

	dlog.Infof("Deleting snapshot")

	// prepare admin restic
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

	// delete volumes
	for _, snapPvc := range(snapshot.Spec.VolumeClaims) {

		dlog.Infof(" - PVC : %s/%s", snapPvc.Namespace, snapPvc.Name)

		// check snapshot exists
		var snap *ResticSnapshot = nil
		for _, snp := range(*snapshotList) {
			if snp.ShortId == snapPvc.SnapshotId {
				snap = &snp
				break
			}
		}
		if snap == nil {
			dlog.Warningf("Snapshot %d not found in repository", snapPvc.SnapshotId)
			continue
		}

		// restic delete job
		dlog.Infof(" -- Deleting snapshot id:%s from repository", snapPvc.SnapshotId)
		deleteJob := adminRestic.resticJobDelete(snapPvc.SnapshotId)
		output, err = DoResticJob(deleteJob, localKubeClient, 10)
		if err != nil {
			return fmt.Errorf("Error running delete snapshot job : %s : %s", err.Error(), output)
		}
	}

	// Delete CR on bucket
	err = bucket.Delete(snapshot.GetName() + ".tgz")
	if err != nil {
		return fmt.Errorf("Deleting tgz file failed : %s", err.Error())
	}

	dlog.Info(" - Delete completed")
	return nil
}
