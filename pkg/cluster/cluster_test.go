package cluster

import (
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/cenkalti/backoff"
	corev1 "k8s.io/api/core/v1"
	batchv1 "k8s.io/api/batch/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/klog"
	"github.com/aws/aws-sdk-go/service/sts"

	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
	volumesnapshot "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
)

func TestCreateSnapshot(t *testing.T) {

	// Init klog
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Parse()
	klog.Infof("k8s-volume-snap pkg cluster test")
	klog.Flush()

	// special param values
	notBoundedPVC := newPVC("default", "TESTPVC", "TESTPV")
	notBoundedPVC.Status.Phase = "Lost"

	cases := map[string]struct {
		snap *volumesnapshot.VolumeSnapshot
		omitClusterID bool
		kubeobjects []runtime.Object
		localobjects []runtime.Object
		podLogs map[string]string
		failedJob string
		expSnap *volumesnapshot.VolumeSnapshot
		snapedClaims []volumesnapshot.VolumeClaim
		snapedStatus *volumesnapshot.VolumeSnapshotStatus
		expMsg string
		bucketErr error
	}{
		"Snapshot 1 PVC successfully": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPV("TESTPV", "default", "TESTPVC"),
				newPVC("default", "TESTPVC", "TESTPV"),
				newPodWithPVC("default", "TESTPOD", "TESTPVC", "TESTNODE01"),
				newPodWithOwner("jobpod-glob", "default", "glob-TESTPV-TESTNODE01"),
				newPodWithOwner("jobpod-backup", "default", "restic-job-backup-TESTPV"),
			},
			podLogs: map[string]string{
				"glob-TESTPV-TESTNODE01": "/path/for/TESTPV",
				"restic-job-backup-TESTPV": "{\"snapshot_id\":\"testSnapshotId\"," +
					"\"total_bytes_processed\":131072}",
			},
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV"},
					SnapshotId: "testSnapshotId", SnapshotSize: 131072, SnapshotReady: true,
				},
			},
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", NumVolumeClaims: 1, SkippedClaims: 0,
				ReadyVolumeClaims: 1, TotalBytes: 131072, TotalFiles: 0,
			},
		},
		"Snapshot 1 PVC with attachment": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPV("TESTPV", "default", "TESTPVC"),
				newPVC("default", "TESTPVC", "TESTPV"),
				newAttachment("attach-TESTPV-NODE01", "TESTNODE01", "TESTPV"),
				newPodWithPVC("default", "TESTPOD", "TESTPVC", ""),
				newPodWithOwner("jobpod-glob", "default", "glob-TESTPV-TESTNODE01"),
				newPodWithOwner("jobpod-backup", "default", "restic-job-backup-TESTPV"),
			},
			podLogs: map[string]string{
				"glob-TESTPV-TESTNODE01": "/path/for/TESTPV",
				"restic-job-backup-TESTPV": "{\"snapshot_id\":\"testSnapshotId\"," +
					"\"total_bytes_processed\":131072}",
			},
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV"},
					SnapshotId: "testSnapshotId", SnapshotSize: 131072, SnapshotReady: true,
				},
			},
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", NumVolumeClaims: 1, SkippedClaims: 0,
				ReadyVolumeClaims: 1, TotalBytes: 131072, TotalFiles: 0,
			},
		},
		"Snapshot 2 PVCs successfully": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPV("TESTPV1", "default", "TESTPVC1"),
				newPVC("default", "TESTPVC1", "TESTPV1"),
				newPV("TESTPV2", "default", "TESTPVC2"),
				newPVC("default", "TESTPVC2", "TESTPV2"),
				newPodWithPVC("default", "TESTPOD1", "TESTPVC1", "TESTNODE01"),
				newPodWithPVC("default", "TESTPOD2", "TESTPVC2", "TESTNODE02"),
				newPodWithOwner("jobpod-glob1", "default", "glob-TESTPV1-TESTNODE01"),
				newPodWithOwner("jobpod-glob2", "default", "glob-TESTPV2-TESTNODE02"),
				newPodWithOwner("jobpod-backup1", "default", "restic-job-backup-TESTPV1"),
				newPodWithOwner("jobpod-backup2", "default", "restic-job-backup-TESTPV2"),
			},
			podLogs: map[string]string{
				"glob-TESTPV1-TESTNODE01": "/path/for/TESTPV1",
				"glob-TESTPV2-TESTNODE02": "/path/for/TESTPV2",
				"restic-job-backup-TESTPV1": "{\"snapshot_id\":\"testSnapshotId1\"," +
					"\"total_bytes_processed\":131072,\"total_files_processed\":128}",
				"restic-job-backup-TESTPV2": "{\"snapshot_id\":\"testSnapshotId2\"," +
					"\"total_bytes_processed\":1024,\"total_files_processed\":32}",
			},
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC1", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV1"}, SnapshotFiles: 128,
					SnapshotId: "testSnapshotId1", SnapshotSize: 131072, SnapshotReady: true,
				},
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC2", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV2"}, SnapshotFiles: 32,
					SnapshotId: "testSnapshotId2", SnapshotSize: 1024, SnapshotReady: true,
				},
			},
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", NumVolumeClaims: 2, SkippedClaims: 0,
				ReadyVolumeClaims: 2, TotalBytes: 132096, TotalFiles: 160,
			},
		},
		"Snapshot 1 PVC successfull and 1 not mounted": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPV("TESTPV1", "default", "TESTPVC1"),
				newPVC("default", "TESTPVC1", "TESTPV1"),
				newPV("TESTPV2", "default", "TESTPVC2"),
				newPVC("default", "TESTPVC2", "TESTPV2"),
				newPodWithPVC("default", "TESTPOD2", "TESTPVC2", "TESTNODE02"),
				newPodWithOwner("jobpod-glob2", "default", "glob-TESTPV2-TESTNODE02"),
				newPodWithOwner("jobpod-backup2", "default", "restic-job-backup-TESTPV2"),
			},
			podLogs: map[string]string{
				"glob-TESTPV2-TESTNODE02": "/path/for/TESTPV2",
				"restic-job-backup-TESTPV2": "{\"snapshot_id\":\"testSnapshotId2\"," +
					"\"total_bytes_processed\":1024,\"total_files_processed\":32}",
			},
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC2", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV2"}, SnapshotFiles: 32,
					SnapshotId: "testSnapshotId2", SnapshotSize: 1024, SnapshotReady: true,
				},
			},
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", NumVolumeClaims: 1, SkippedClaims: 1,
				SkippedMessages: []string{"default/TESTPVC1 - PV not in use : Mount not found"},
				ReadyVolumeClaims: 1, TotalBytes: 1024, TotalFiles: 32,
			},
		},
		"Snapshot 1 PVC not bound": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{ notBoundedPVC },
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", SkippedClaims: 1,
				SkippedMessages: []string{"default/TESTPVC - PVC not bound"},
			},
		},
		"Snapshot 1 PVC PV not found": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPVC("default", "TESTPVC", "TESTPV"),
			},
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", SkippedClaims: 1,
				SkippedMessages: []string{"default/TESTPVC - Error getting PV : persistentvolumes \"TESTPV\" not found"},
			},
		},
		"Snapshot 1 PVC grob error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPV("TESTPV", "default", "TESTPVC"),
				newPVC("default", "TESTPVC", "TESTPV"),
				newPodWithPVC("default", "TESTPOD", "TESTPVC", "TESTNODE01"),
				newPodWithOwner("jobpod-glob", "default", "glob-TESTPV-TESTNODE01"),
			},
			podLogs: map[string]string{
				"glob-TESTPV-TESTNODE01": "some grob error",
			},
			failedJob: "glob-TESTPV-TESTNODE01",
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", SkippedClaims: 1,
				SkippedMessages: []string{
					"default/TESTPVC - Error globbing volume path : some grob error : "  +
						"Error doing restic job - Job glob-TESTPV-TESTNODE01 failed",
				},
			},
		},
		"Snapshot 1 PVC snapshot error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPV("TESTPV", "default", "TESTPVC"),
				newPVC("default", "TESTPVC", "TESTPV"),
				newPodWithPVC("default", "TESTPOD", "TESTPVC", "TESTNODE01"),
				newPodWithOwner("jobpod-glob", "default", "glob-TESTPV-TESTNODE01"),
				newPodWithOwner("jobpod-backup", "default", "restic-job-backup-TESTPV"),
			},
			podLogs: map[string]string{
				"glob-TESTPV-TESTNODE01": "/path/for/TESTPV",
				"restic-job-backup-TESTPV": "some snapshot error",
			},
			failedJob: "restic-job-backup-TESTPV",
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", SkippedClaims: 1,
				SkippedMessages: []string{
					"default/TESTPVC - Error taking snapshot : some snapshot error : " +
						"Error doing restic job - Job restic-job-backup-TESTPV failed",
				},
			},
		},
		"Snapshot 1 PVC json parse error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPV("TESTPV", "default", "TESTPVC"),
				newPVC("default", "TESTPVC", "TESTPV"),
				newPodWithPVC("default", "TESTPOD", "TESTPVC", "TESTNODE01"),
				newPodWithOwner("jobpod-glob", "default", "glob-TESTPV-TESTNODE01"),
				newPodWithOwner("jobpod-backup", "default", "restic-job-backup-TESTPV"),
			},
			podLogs: map[string]string{
				"glob-TESTPV-TESTNODE01": "/path/for/TESTPV",
				"restic-job-backup-TESTPV": "not JSON string",
			},
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
			snapedStatus: &volumesnapshot.VolumeSnapshotStatus{
				Phase: "InProgress", SkippedClaims: 1,
				SkippedMessages: []string{
					"default/TESTPVC - Error persing restic backup summary : " +
						"invalid character 'o' in literal null (expecting 'u')",
				},
			},
		},
		"ClusterID not found": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			omitClusterID: true,
			expMsg: "Getting clusterId(=kube-system UID) failed",
		},
		"Assume role perm error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			bucketErr: fmt.Errorf("InvalidAccessKeyId"),
			expMsg: "Getting temporaly credentials failed (do not retry)",
		},
		"Assume role not perm error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			bucketErr: fmt.Errorf("some bucket error"),
			expMsg: "Getting temporaly credentials failed (do retry)",
		},
		"List snapshot error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "some list snapshot error",
			},
			failedJob: "restic-job-list-snapshots-test01",
			expMsg: "Checking repository failed",
		},
		"Create restic repository (error)": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
				newPodWithOwner("jobpod-init-repo", "default", "restic-job-init-repo-TESTCLUSTERID"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "specified key does not exist",
				"restic-job-init-repo-TESTCLUSTERID": "some create repo error",
			},
			failedJob: "all",
			expMsg: "Error doing restic job - Job restic-job-init-repo-TESTCLUSTERID failed",
		},
	}

	// Do test cases
	for name, c := range(cases) {
		t.Logf("Test case : %s", name)
		clusterID := "TESTCLUSTERID"
		if c.omitClusterID {
			clusterID = ""
		}
		kubeClient := initKubeClient(c.kubeobjects, clusterID, c.failedJob, "")
		localKubeClient := initKubeClient(c.localobjects, "LOCALCLUSTERID", c.failedJob, "")
		bucket := &bucketMock{}
		testResticPodLog = c.podLogs
		bucketErr = c.bucketErr

		err := snapshotVolumes(c.snap, bucket, kubeClient, localKubeClient)

		chkMsg(t, c.expMsg, err)
		if c.expSnap != nil {
			c.expSnap.Spec.VolumeClaims = c.snapedClaims
			c.expSnap.Spec.ClusterId = "TESTCLUSTERID"
			if c.snapedStatus != nil {
				c.expSnap.Status = *c.snapedStatus
			}
			snapshotCopyTimestamps(c.expSnap, c.snap)
			if !reflect.DeepEqual(c.expSnap, c.snap) {
				t.Errorf("snapshot not matched in case [%s]\nexpected : %v\nbut got  : %v", name, c.expSnap, c.snap)
			}
		}
	}
}

func TestRestore(t *testing.T) {

	cases := map[string]struct {
		restore *volumesnapshot.VolumeRestore
		snap *volumesnapshot.VolumeSnapshot
		snapClaims []volumesnapshot.VolumeClaim
		kubeobjects []runtime.Object
		localobjects []runtime.Object
		podLogs map[string]string
		failedJob string
		expRestore *volumesnapshot.VolumeRestore
		restoreStatus *volumesnapshot.VolumeRestoreStatus
		omitClusterID bool
		expMsg string
		bucketErr error
		pvcPhase string
	}{
		"Restore 2 PVCs successfully": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "Completed"),
			snapClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC1", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV1"},
					SnapshotId: "testSnapshotId1", SnapshotSize: 131072, SnapshotReady: true,
				},
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC2", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV2"},
					SnapshotId: "testSnapshotId2", SnapshotSize: 1024, SnapshotReady: true,
				},
			},
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPodWithOwner("jobpod-restore1", "default", "restic-job-restore-testSnapshotId1"),
				newPodWithOwner("jobpod-restore2", "default", "restic-job-restore-testSnapshotId2"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "[{" +
						"\"short_id\":\"testSnapshotId1\"," +
						"\"time\":\"2021-01-01T12:00:00.00Z\"," +
						"\"paths\":[\"TESTCLUSTERUID/TESTPVC1UID\"]" +
					"},{" +
						"\"short_id\":\"testSnapshotId2\"," +
						"\"time\":\"2021-01-01T12:00:00.00Z\"," +
						"\"paths\":[\"TESTCLUSTERUID/TESTPVC2UID\"]" +
					"}]",
			},
			expRestore: newConfiguredRestore("test01", "test01", "InProgress"),
			restoreStatus: &volumesnapshot.VolumeRestoreStatus{
				Phase: "InProgress", NumVolumeClaims: 2, NumFailedVolumeClaims: 0,
			},
		},
		"ClusterID not found": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "InProgress"),
			omitClusterID: true,
			expMsg: "Getting clusterId(=kube-system UID) failed",
		},
		"Assume role error": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "InProgress"),
			bucketErr: fmt.Errorf("InvalidAccessKeyId"),
			expMsg: "Getting temporaly credentials failed",
		},
		"List snapshot error": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "some list snapshot error",
			},
			failedJob: "restic-job-list-snapshots-test01",
			expMsg: "List snapshots failed",
		},
		"List snapshot parse error": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "not a JSON string",
			},
			expMsg: "Error persing restic snapshot",
		},
		"Restore 1 PVC error snapshot not found": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "Completed"),
			snapClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC1", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV1"},
					SnapshotId: "testSnapshotId1", SnapshotSize: 131072, SnapshotReady: true,
				},
			},
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "[]",
			},
			expRestore: newConfiguredRestore("test01", "test01", "InProgress"),
			restoreStatus: &volumesnapshot.VolumeRestoreStatus{
				Phase: "InProgress", NumVolumeClaims: 1, NumFailedVolumeClaims: 1,
				FailedVolumeClaims: []string{"default/TESTPVC1:Snapshot testSnapshotId1 not found"},
			},
		},
		"Restore 1 PVC error create PVC": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "Completed"),
			snapClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC1", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV1"},
					SnapshotId: "testSnapshotId1", SnapshotSize: 131072, SnapshotReady: true,
				},
			},
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPVC("default", "TESTPVC1", "TESTPV1"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "[{" +
						"\"short_id\":\"testSnapshotId1\"," +
						"\"time\":\"2021-01-01T12:00:00.00Z\"," +
						"\"paths\":[\"TESTCLUSTERUID/TESTPVC1UID\"]" +
					"}]",
			},
			expRestore: newConfiguredRestore("test01", "test01", "InProgress"),
			restoreStatus: &volumesnapshot.VolumeRestoreStatus{
				Phase: "InProgress", NumVolumeClaims: 1, NumFailedVolumeClaims: 1,
				FailedVolumeClaims: []string{"default/TESTPVC1:Creating PVC failed : persistentvolumeclaims \"TESTPVC1\" already exists"},
			},
		},
		"Restore 1 PVC error bound PVC": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "Completed"),
			snapClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC1", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV1"},
					SnapshotId: "testSnapshotId1", SnapshotSize: 131072, SnapshotReady: true,
				},
			},
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			pvcPhase: "Lost",
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "[{" +
						"\"short_id\":\"testSnapshotId1\"," +
						"\"time\":\"2021-01-01T12:00:00.00Z\"," +
						"\"paths\":[\"TESTCLUSTERUID/TESTPVC1UID\"]" +
					"}]",
			},
			expRestore: newConfiguredRestore("test01", "test01", "InProgress"),
			restoreStatus: &volumesnapshot.VolumeRestoreStatus{
				Phase: "InProgress", NumVolumeClaims: 1, NumFailedVolumeClaims: 1,
				FailedVolumeClaims: []string{"default/TESTPVC1:Bound PVC failed : Unknown phase on bound PVC Lost"},
			},
		},
		"Restore 1 PVC error restore": {
			restore: newConfiguredRestore("test01", "test01", "InProgress"),
			snap: newConfiguredSnapshot("test01", "Completed"),
			snapClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC1", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV1"},
					SnapshotId: "testSnapshotId1", SnapshotSize: 131072, SnapshotReady: true,
				},
			},
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPodWithOwner("jobpod-restore1", "default", "restic-job-restore-testSnapshotId1"),
			},
			failedJob: "restic-job-restore-testSnapshotId1",
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "[{" +
						"\"short_id\":\"testSnapshotId1\"," +
						"\"time\":\"2021-01-01T12:00:00.00Z\"," +
						"\"paths\":[\"TESTCLUSTERUID/TESTPVC1UID\"]" +
					"}]",
				"restic-job-restore-testSnapshotId1": "some restore error",
			},
			expRestore: newConfiguredRestore("test01", "test01", "InProgress"),
			restoreStatus: &volumesnapshot.VolumeRestoreStatus{
				Phase: "InProgress", NumVolumeClaims: 1, NumFailedVolumeClaims: 1,
				FailedVolumeClaims: []string{
					"default/TESTPVC1:Error running restore snapshot job : " +
					"some restore error : Error doing restic job - " +
					"Job restic-job-restore-testSnapshotId1 failed : ",
				},
			},
		},
	}

	// Do test cases
	for name, c := range(cases) {
		t.Logf("Test case : %s", name)
		clusterID := "TESTCLUSTERID"
		if c.omitClusterID {
			clusterID = ""
		}
		kubeClient := initKubeClient(c.kubeobjects, clusterID, c.failedJob, c.pvcPhase)
		localKubeClient := initKubeClient(c.localobjects, "LOCALCLUSTERID", c.failedJob, c.pvcPhase)
		bucket := &bucketMock{}
		c.snap.Spec.VolumeClaims = c.snapClaims
		c.snap.Spec.ClusterId = clusterID
		testResticPodLog = c.podLogs
		bucketErr = c.bucketErr

		err := restoreVolumes(c.restore, c.snap, bucket, kubeClient, localKubeClient)

		chkMsg(t, c.expMsg, err)
		if c.expRestore != nil {
			if c.restoreStatus != nil {
				c.expRestore.Status = *c.restoreStatus
				c.restore.Status.RestoreTimestamp = c.restoreStatus.RestoreTimestamp
			}
			if !reflect.DeepEqual(c.expRestore, c.restore) {
				t.Errorf("restore not matched in case [%s]\nexpected : %v\nbut got  : %v", name, c.expRestore, c.restore)
			}
		}
	}
}

func TestDeleteSnapshot(t *testing.T) {

	cases := map[string]struct {
		snap *volumesnapshot.VolumeSnapshot
		snapClaims []volumesnapshot.VolumeClaim
		localobjects []runtime.Object
		podLogs map[string]string
		failedJob string
		expMsg string
		bucketErr error
	}{
		"Delete 2 volume snapshots successfully": {
			snap: newConfiguredSnapshot("test01", "Completed"),
			snapClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC1", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV1"},
					SnapshotId: "testSnapshotId1", SnapshotSize: 131072, SnapshotReady: true,
				},
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC2", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV2"},
					SnapshotId: "testSnapshotId2", SnapshotSize: 1024, SnapshotReady: true,
				},
			},
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
				newPodWithOwner("jobpod-delete1", "default", "restic-job-delete-testSnapshotId1"),
				newPodWithOwner("jobpod-delete2", "default", "restic-job-delete-testSnapshotId2"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "[{" +
						"\"short_id\":\"testSnapshotId1\"," +
						"\"time\":\"2021-01-01T12:00:00.00Z\"," +
						"\"paths\":[\"TESTCLUSTERUID/TESTPVC1UID\"]" +
					"},{" +
						"\"short_id\":\"testSnapshotId2\"," +
						"\"time\":\"2021-01-01T12:00:00.00Z\"," +
						"\"paths\":[\"TESTCLUSTERUID/TESTPVC2UID\"]" +
					"}]",
			},
		},
		"List snapshot error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "some list snapshot error",
			},
			failedJob: "restic-job-list-snapshots-test01",
			expMsg: "List snapshots failed",
		},
		"List snapshot parse error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "not a JSON string",
			},
			expMsg: "Error persing restic snapshot",
		},
		"Delete 1 PVC error delete": {
			snap: newConfiguredSnapshot("test01", "Completed"),
			snapClaims: []volumesnapshot.VolumeClaim{
				volumesnapshot.VolumeClaim{
					Name: "TESTPVC1", Namespace: "default",
					ClaimSpec: corev1.PersistentVolumeClaimSpec{VolumeName: "TESTPV1"},
					SnapshotId: "testSnapshotId1", SnapshotSize: 131072, SnapshotReady: true,
				},
			},
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
				newPodWithOwner("jobpod-delete1", "default", "restic-job-delete-testSnapshotId1"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "[{" +
						"\"short_id\":\"testSnapshotId1\"," +
						"\"time\":\"2021-01-01T12:00:00.00Z\"," +
						"\"paths\":[\"TESTCLUSTERUID/TESTPVC1UID\"]" +
					"}]",
				"restic-job-delete-testSnapshotId1": "some delete error",
			},
			failedJob: "restic-job-delete-testSnapshotId1",
			expMsg: "Error running delete snapshot job : some delete error",
		},
		"Bucket delete error": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			podLogs: map[string]string{
				"restic-job-list-snapshots-test01": "[]",
			},
			bucketErr: fmt.Errorf("some bucket delete error"),
			expMsg: "Deleting tgz file failed : some bucket delete error",
		},
	}

	// Do test cases
	for name, c := range(cases) {
		t.Logf("Test case : %s", name)
		localKubeClient := initKubeClient(c.localobjects, "LOCALCLUSTERID", c.failedJob, "")
		bucket := &bucketMock{}
		c.snap.Spec.VolumeClaims = c.snapClaims
		c.snap.Spec.ClusterId = "TESTCLUSTERID"
		testResticPodLog = c.podLogs
		bucketErr = c.bucketErr

		err := deleteVolumeSnapshots(c.snap, bucket, localKubeClient)
		chkMsg(t, c.expMsg, err)
	}
}

// check util

func chkMsg(t *testing.T, expMsg string, err error) {
	if expMsg != "" {
		if err != nil {
			if !strings.Contains(err.Error(), expMsg) {
				t.Errorf("message not matched \nexpected : %s\nbut got  : %s", expMsg, err.Error())
			}
		} else {
			t.Errorf("expected error : %s  - not occurred", expMsg)
		}
	} else {
		if err != nil {
			t.Errorf("unexpected error occurred : %s", err.Error())
		}
	}
}

// Mock bucket

type bucketMock struct {
	objectstore.Objectstore
}

var bucketErr error

func (b *bucketMock) GetEndpoint() string {
	return "example.com"
}

func (b *bucketMock) GetBucketName() string {
	return "bucket"
}

func (b *bucketMock) GetAccessKey() string {
	return "accesskey"
}

func (b *bucketMock) GetSecretKey() string {
	return "secretkey"
}

var accesskey = "ACCESSKEY"
var secretkey = "SECRETKEY"
var sessiontoken = "SESSIONTOKEN"

func (b *bucketMock) CreateAssumeRole(clusterID string,
	durationSeconds int64) (*sts.Credentials, error) {
	if bucketErr != nil {
		return nil, bucketErr
	}
	return &sts.Credentials{
		AccessKeyId: &accesskey,
		SecretAccessKey: &secretkey,
		SessionToken: &sessiontoken,
	}, nil
}

var uploadFilename string

func (b *bucketMock) Upload(file *os.File, filename string) error {
	if bucketErr != nil {
		return bucketErr
	}
	uploadFilename = filename
	return nil
}

var downloadFilename string

func (b *bucketMock) Download(file *os.File, filename string) error {
	if bucketErr != nil {
		return bucketErr
	}
	downloadFilename = filename
	return nil
}

var deleteFilename string

func (b *bucketMock) Delete(filename string) error {
	if bucketErr != nil {
		return bucketErr
	}
	deleteFilename = filename
	return nil
}

var objectInfo *objectstore.ObjectInfo
var getObjectInfoFilename string

func (b bucketMock) GetObjectInfo(filename string) (*objectstore.ObjectInfo, error) {
	if bucketErr != nil {
		return nil, bucketErr
	}
	getObjectInfoFilename = filename
	return objectInfo, nil
}

// Mock k8s API

func initKubeClient(objects []runtime.Object, clusterID string, failedJob, pvcPhase string) (kubernetes.Interface) {

	// test k8s
	kubeobjects := []runtime.Object{}
	if clusterID != "" {
		kubeobjects = append(kubeobjects, newNamespace("kube-system", clusterID))
	}
	kubeobjects = append(kubeobjects, objects...)
	kubeClient := k8sfake.NewSimpleClientset(kubeobjects...)

	// create job reaction
	kubeClient.Fake.PrependReactor("create", "jobs", func(action core.Action) (bool, runtime.Object, error) {
		obj := action.(core.CreateAction).GetObject()
		job, _ := obj.(*batchv1.Job)
		if job.GetName() == failedJob || failedJob == "all" {
			job.Status.Conditions = []batchv1.JobCondition{
				batchv1.JobCondition{Type: batchv1.JobFailed},
			}
		} else {
			job.Status.Conditions = []batchv1.JobCondition{
				batchv1.JobCondition{Type: batchv1.JobComplete},
			}
		}
		return false, job, nil
	})

	// create PVC reaction
	kubeClient.Fake.PrependReactor("create", "persistentvolumeclaims", func(action core.Action) (bool, runtime.Object, error) {
		obj := action.(core.CreateAction).GetObject()
		pvc, _ := obj.(*corev1.PersistentVolumeClaim)
		if pvcPhase == "" {
			pvc.Status.Phase = "Bound"
		} else {
			pvc.Status.Phase = "Lost"
		}
		return false, pvc, nil
	})

	return kubeClient
}

// Test resources

func newNamespace(name, uid string) *corev1.Namespace {
	return &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID(uid),
		},
	}
}

func newConfiguredSnapshot(name, phase string) *volumesnapshot.VolumeSnapshot {
	return &volumesnapshot.VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{APIVersion: volumesnapshot.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: volumesnapshot.VolumeSnapshotSpec{
			ClusterName:       name,
			Kubeconfig:        "kubeconfig",
			ObjectstoreConfig: "objectstoreConfig",
		},
		Status: volumesnapshot.VolumeSnapshotStatus{
			Phase: phase,
		},
	}
}

func snapshotCopyTimestamps(src, dst *volumesnapshot.VolumeSnapshot) {
	if len(dst.Spec.VolumeClaims) == len(src.Spec.VolumeClaims) {
		for i, _ := range(dst.Spec.VolumeClaims) {
			dst.Spec.VolumeClaims[i].SnapshotTime = src.Spec.VolumeClaims[i].SnapshotTime
		}
	}
	dst.Status.SnapshotStartTime = src.Status.SnapshotStartTime
	dst.Status.SnapshotEndTime = src.Status.SnapshotEndTime
}

func newConfiguredRestore(name, snapshot, phase string) *volumesnapshot.VolumeRestore {
	return &volumesnapshot.VolumeRestore{
		TypeMeta: metav1.TypeMeta{APIVersion: volumesnapshot.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: volumesnapshot.VolumeRestoreSpec{
			ClusterName:           name,
			Kubeconfig:            "kubeconfig",
			VolumeSnapshotName:    snapshot,
		},
		Status: volumesnapshot.VolumeRestoreStatus{
			Phase: phase,
		},
	}
}

func newPV(name, claimns, claimname string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{
			Name:     name,
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: claimns, Name: claimname},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeHandle: "testVolumeHandle",
					Driver:       "testDriverName",
				},
			},
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: "Bound",
		},
	}
}

func newPVC(ns, name, pvname string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       pvname,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: "Bound",
		},
	}
}

func newAttachment(name, node, pvname string) *storagev1.VolumeAttachment {
	return &storagev1.VolumeAttachment{
		TypeMeta: metav1.TypeMeta{APIVersion: "storage/v1", Kind: "VolumeAttachment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:     name,
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvname,
			},
			NodeName: node,
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}
}

func newPodWithPVC(ns, name, pvcname, nodename string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: corev1.PodSpec{
			NodeName: nodename,
			Containers: []corev1.Container{
				corev1.Container{
					VolumeMounts: []corev1.VolumeMount{
						corev1.VolumeMount{
							Name: "mnt",
							MountPath: "/mnt",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				corev1.Volume{
					Name: "mnt",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcname,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: "Running",
		},
	}
}

func newPodWithOwner(name, namespace, owner string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				metav1.OwnerReference{Name: owner},
			},
		},
	}
}

const kubeconfigSrc = `apiVersion: v1
clusters:
- cluster:
    server: CLUSTER_URL
  name: kubernetes
contexts:
- context:
    cluster: kubernetes
    user: user
  name: user@kubernetes
current-context: user@kubernetes
kind: Config
preferences: {}
users:
- name: user
  token: TOKEN`

const responseSrc = `{
  "kind": "Status",
  "apiVersion": "v1",
  "metadata": {
	  },
  "status": "Failure",
  "message": "Unauthorized",
  "reason": "Unauthorized",
  "code": 401
}`

func TestSnapshotConfig(t *testing.T) {

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		fmt.Fprintln(w, responseSrc)
	}))

	// Test01 Unauthorized - Permanent error
	snap := newConfiguredSnapshot("test1", "InProgress")
	snap.Spec.Kubeconfig = strings.Replace(kubeconfigSrc, "CLUSTER_URL", ts.URL, 1)
	cluster := NewCluster()
	err := cluster.Snapshot(snap, nil, nil)
	fmt.Println(err.Error())
	_, ok := err.(*backoff.PermanentError)
	if !ok {
		t.Errorf("Error type is not backoff.PermanentError but is %T", reflect.TypeOf(err))
	}

	// Test02 Connection refused - Error for retry
	ts.Close()
	err = cluster.Snapshot(snap, nil, nil)
	fmt.Println(err.Error())
	_, ok = err.(*backoff.PermanentError)
	if ok {
		t.Errorf("Error type is %T", reflect.TypeOf(err))
	}
}
