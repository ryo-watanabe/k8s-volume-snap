package cluster

import (
	"flag"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	batchv1 "k8s.io/api/batch/v1"
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

	cases := map[string]struct {
		snap *volumesnapshot.VolumeSnapshot
		kubeobjects []runtime.Object
		localobjects []runtime.Object
		expSnap *volumesnapshot.VolumeSnapshot
		expMsg string
	}{
		"Snapshot Successfully": {
			snap: newConfiguredSnapshot("test01", "InProgress"),
			localobjects: []runtime.Object{
				newPodWithOwner("jobpod-list", "default", "restic-job-list-snapshots-test01"),
			},
			kubeobjects: []runtime.Object{
				newPV("TESTPV", "default", "TESTPVC"),
				newPVC("default", "TESTPVC", "TESTPV"),
				newPodWithPVC("default", "TESTPOD", "TESTPVC", "TESTNODE01"),
			},
			expSnap: newConfiguredSnapshot("test01", "InProgress"),
		},
	}
	for name, c := range(cases) {
		t.Logf("Test case : %s", name)
		kubeClient := initKubeClient(c.kubeobjects, "TESTCLUSTERID", false)
		localKubeClient := initKubeClient(c.localobjects, "LOCALCLUSTERID", false)
		bucket := &bucketMock{}
		err := snapshotVolumes(c.snap, bucket, kubeClient, localKubeClient)
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
	return &sts.Credentials{
		AccessKeyId: &accesskey,
		SecretAccessKey: &secretkey,
		SessionToken: &sessiontoken,
	}, nil
}

var uploadFilename string

func (b *bucketMock) Upload(file *os.File, filename string) error {
	uploadFilename = filename
	return nil
}

var downloadFilename string

func (b *bucketMock) Download(file *os.File, filename string) error {
	downloadFilename = filename
	return nil
}

var objectInfo *objectstore.ObjectInfo
var getObjectInfoFilename string

func (b bucketMock) GetObjectInfo(filename string) (*objectstore.ObjectInfo, error) {
	getObjectInfoFilename = filename
	return objectInfo, nil
}

// Mock k8s API

func initKubeClient(objects []runtime.Object, clusterID string, jobFailed bool) (kubernetes.Interface) {

	// test k8s
	kubeobjects := []runtime.Object{}
	kubeobjects = append(kubeobjects, newNamespace("kube-system", clusterID))
	kubeobjects = append(kubeobjects, objects...)
	kubeClient := k8sfake.NewSimpleClientset(kubeobjects...)

	// set status for all jobs
	jobCondition := batchv1.JobComplete
	if jobFailed {
		jobCondition = batchv1.JobFailed
	}
	kubeClient.Fake.PrependReactor("create", "jobs", func(action core.Action) (bool, runtime.Object, error) {
		obj := action.(core.CreateAction).GetObject()
		job, _ := obj.(*batchv1.Job)
		job.Status.Conditions = []batchv1.JobCondition{
			batchv1.JobCondition{Type: jobCondition},
		}
		return false, job, nil
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
			VolumeSnapshotName:          snapshot,
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
			ClaimRef:         &corev1.ObjectReference{Namespace: claimns, Name: claimname},
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
