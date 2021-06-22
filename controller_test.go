package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/diff"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"

	volumesnapshot "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	informers "github.com/ryo-watanabe/k8s-volume-snap/pkg/client/informers/externalversions"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	clientset "github.com/ryo-watanabe/k8s-volume-snap/pkg/client/clientset/versioned"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/client/clientset/versioned/fake"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/cluster"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
)

var (
	alwaysReady        = func() bool { return true }
	noResyncPeriodFunc = func() time.Duration { return 0 }
	snapshotNamespace  = "default"
)

type Case struct {
	snapshots        []*volumesnapshot.VolumeSnapshot
	restores         []*volumesnapshot.VolumeRestore
	configs          []*volumesnapshot.ObjectstoreConfig
	secrets          []*corev1.Secret
	updatedSnapshots []*volumesnapshot.VolumeSnapshot
	updatedRestores  []*volumesnapshot.VolumeRestore
	deleteSnapshots  []*volumesnapshot.VolumeSnapshot
	deleteRestores   []*volumesnapshot.VolumeRestore
	queueOnly        bool
	handleKey        string
	restoreerror     error
	snaperror        error
}

func newSnapshotCase(resultStatus, reason string) Case {
	c := Case{
		snapshots: []*volumesnapshot.VolumeSnapshot{
			newConfiguredSnapshot("test1", "InQueue"),
		},
		updatedSnapshots: []*volumesnapshot.VolumeSnapshot{
			newConfiguredSnapshot("test1", "InProgress"),
			newConfiguredSnapshot("test1", resultStatus),
		},
		configs: []*volumesnapshot.ObjectstoreConfig{
			newObjectstoreConfig(),
		},
		secrets: []*corev1.Secret{
			newCloudCredentialSecret(),
		},
		handleKey: "test1",
	}
	c.updatedSnapshots[1].Status.Reason = reason
	return c
}

func TestSnapshot(t *testing.T) {

	// Init klog
	flagVSet("4")

	cases := []Case{
		// 0 "create snapshot":
		Case{
			snapshots: []*volumesnapshot.VolumeSnapshot{
				newConfiguredSnapshot("test1", "Completed"),
				newConfiguredSnapshot("test2", ""),
			},
			updatedSnapshots: []*volumesnapshot.VolumeSnapshot{
				newConfiguredSnapshot("test2", "InQueue"),
			},
			queueOnly: true,
			handleKey: "test2",
		},
		// 1 "Completed":
		newSnapshotCase("Completed", ""),
		// 2 "Failed secret not found":
		newSnapshotCase("Failed", "secrets \"cloudCredentialSecret\" not found"),
		// 3 "Mark Failed to InProgress snapshot":
		Case{
			snapshots: []*volumesnapshot.VolumeSnapshot{
				newConfiguredSnapshot("test1", "InProgress"),
			},
			updatedSnapshots: []*volumesnapshot.VolumeSnapshot{
				newConfiguredSnapshot("test1", "Failed"),
			},
			handleKey: "test1",
		},
		// 4 "Failed cluster.Snapshot returns error":
		newSnapshotCase("Failed", "Mock cluster returns perm error"),
		// 5 "Failed cluster.Snapshot returns retry timeout":
		newSnapshotCase("Failed", "Mock cluster returns not perm error"),
		// 6 "Key not found (not error)":
		Case{handleKey: "test1"},
		// 7 "Invalid key":
		Case{handleKey: "test1/test1"},
	}

	// Additional test data:
	// 2 "Failed secret not found":
	cases[2].secrets = nil
	// 3 "Mark Failed to InProgress snapshot"
	cases[3].updatedSnapshots[0].Status.Reason = "Controller stopped while taking the snapshot"
	// 4 "Failed cluster.Snapshot returns error"
	cases[4].snaperror = backoff.Permanent(fmt.Errorf("Mock cluster returns perm error"))
	// 5 "Failed cluster.Snapshot returns retry timeout"
	cases[5].snaperror = fmt.Errorf("Mock cluster returns not perm error")

	for i := range cases {
		t.Logf("Test # %d", i)
		SnapshotTestCase(&cases[i], t)
	}
}

func newRestoreCase(resultStatus, reason string) Case {
	c := Case{
		snapshots: []*volumesnapshot.VolumeSnapshot{
			newConfiguredSnapshot("snapshot", "Completed"),
		},
		restores: []*volumesnapshot.VolumeRestore{
			newConfiguredRestore("test1", "InQueue"),
		},
		updatedRestores: []*volumesnapshot.VolumeRestore{
			newConfiguredRestore("test1", "InProgress"),
			newConfiguredRestore("test1", resultStatus),
		},
		configs: []*volumesnapshot.ObjectstoreConfig{
			newObjectstoreConfig(),
		},
		secrets: []*corev1.Secret{
			newCloudCredentialSecret(),
		},
		handleKey: "test1",
	}
	c.updatedRestores[1].Status.Reason = reason
	return c
}

func TestRestore(t *testing.T) {

	cases := []Case{
		// 0 "create restore":
		Case{
			restores: []*volumesnapshot.VolumeRestore{
				newConfiguredRestore("test1", "Completed"),
				newConfiguredRestore("test2", ""),
			},
			updatedRestores: []*volumesnapshot.VolumeRestore{
				newConfiguredRestore("test2", "InQueue"),
			},
			queueOnly: true,
			handleKey: "test2",
		},
		// 1 "Completed":
		newRestoreCase("Completed", ""),
		// 2 "Failed with restore error":
		newRestoreCase("Failed", "Mock cluster resturns a error"),
		// 3 "Failed with invalid snapshot":
		newRestoreCase("Failed", "Snapshot data is not status 'Completed'"),
		// 4 "Failed with snapshot not found":
		newRestoreCase("Failed", "volumesnapshots.volumesnapshot.rywt.io \"snapshot\" not found"),
		// 5 "Failed with config not found":
		newRestoreCase("Failed", "objectstoreconfigs.volumesnapshot.rywt.io \"objectstoreConfig\" not found"),
		// 6 "Failed with secret not found":
		newRestoreCase("Failed", "secrets \"cloudCredentialSecret\" not found"),
		// 7 "Invalid key":
		Case{handleKey: "test1/test1"},
		// 8 "Key not found (not error)":
		Case{handleKey: "test1"},
	}

	// Additional test data:
	// 2 "Failed with restore error"
	cases[2].restoreerror = fmt.Errorf("Mock cluster resturns a error")
	// 3 "Failed with invalid snapshot"
	cases[3].snapshots[0].Status.Phase = "Failed"
	// 4 "Failed with snapshot not found"
	cases[4].snapshots = nil
	// 5 "Failed with config not found"
	cases[5].configs = nil
	// 6 "Failed with secret not found"
	cases[6].secrets = nil

	for i := range cases {
		t.Logf("Test # %d", i)
		RestoreTestCase(&cases[i], t)
	}
}

func flagVSet(strValue string) {
	//err := flag.Lookup("v").Value.Set(valueStr)
	klog.InitFlags(nil)
	err := flag.Set("v", strValue)
	if err != nil {
		fmt.Printf("Error in flag v set : %s\n", err.Error())
	}
	err = flag.Set("logtostderr", "true")
	if err != nil {
		fmt.Printf("Error in flag.Set : %s\n", err.Error())
	}
	flag.Parse()
}

type fixture struct {
	t *testing.T

	client     *fake.Clientset
	dynamic    *dynamicfake.FakeDynamicClient
	kubeclient *k8sfake.Clientset
	// Objects to put in the store.
	snapshotLister []*volumesnapshot.VolumeSnapshot
	restoreLister  []*volumesnapshot.VolumeRestore
	// Actions expected to happen on the client.
	actions []core.Action
	// Objects from here preloaded into NewSimpleFake.
	kubeobjects []runtime.Object
	objects     []runtime.Object
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{}
	f.t = t
	f.objects = []runtime.Object{}
	f.kubeobjects = []runtime.Object{}
	return f
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

func newObjectstoreConfig() *volumesnapshot.ObjectstoreConfig {
	return &volumesnapshot.ObjectstoreConfig{
		TypeMeta: metav1.TypeMeta{APIVersion: volumesnapshot.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "objectstoreConfig",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: volumesnapshot.ObjectstoreConfigSpec{
			Region:                "refion",
			Endpoint:              "endpoint",
			Bucket:                "bucket",
			CloudCredentialSecret: "cloudCredentialSecret",
		},
	}
}

func newCloudCredentialSecret() *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudCredentialSecret",
			Namespace: metav1.NamespaceDefault,
		},
		Data: map[string][]byte{
			"accesskey": []byte("YWNjZXNza2V5"),
			"secretkey": []byte("c2VjcmV0a2V5"),
		},
	}
}

func newConfiguredRestore(name, phase string) *volumesnapshot.VolumeRestore {
	return &volumesnapshot.VolumeRestore{
		TypeMeta: metav1.TypeMeta{APIVersion: volumesnapshot.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: volumesnapshot.VolumeRestoreSpec{
			ClusterName:        name,
			Kubeconfig:         "kubeconfig",
			VolumeSnapshotName: "snapshot",
		},
		Status: volumesnapshot.VolumeRestoreStatus{
			Phase: phase,
		},
	}
}

// FakeCmd for fake command interface
type mockCluster struct {
	cluster.Interface
}

// Snapshot for fake cluster interface
var snapshotErr error

func (c *mockCluster) Snapshot(
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {
	return snapshotErr
}

// DeleteSnapshot for fake cluster interface
var deleteErr error

func (c *mockCluster) DeleteSnapshot(
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {
	return deleteErr
}

// Restore for fake cluster interface
var restoreErr error

func (c *mockCluster) Restore(
	restore *vsv1alpha1.VolumeRestore,
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {
	return restoreErr
}

func (f *fixture) newController() (*Controller, informers.SharedInformerFactory, kubeinformers.SharedInformerFactory) {
	f.client = fake.NewSimpleClientset(f.objects...)
	f.dynamic = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	f.kubeclient = k8sfake.NewSimpleClientset(f.kubeobjects...)

	i := informers.NewSharedInformerFactory(f.client, noResyncPeriodFunc())
	k8sI := kubeinformers.NewSharedInformerFactory(f.kubeclient, noResyncPeriodFunc())

	c := NewController(
		f.kubeclient, f.client,
		i.Volumesnapshot().V1alpha1().VolumeSnapshots(),
		i.Volumesnapshot().V1alpha1().VolumeRestores(),
		snapshotNamespace, true, true, 5,
		&mockCluster{},
	)

	c.snapshotsSynced = alwaysReady
	c.restoresSynced = alwaysReady
	c.recorder = &record.FakeRecorder{}

	return c, i, k8sI
}

func (f *fixture) initInformers(i informers.SharedInformerFactory, k8sI kubeinformers.SharedInformerFactory) {
	for _, p := range f.snapshotLister {
		_ = i.Volumesnapshot().V1alpha1().VolumeSnapshots().Informer().GetIndexer().Add(p)
	}

	for _, p := range f.restoreLister {
		_ = i.Volumesnapshot().V1alpha1().VolumeRestores().Informer().GetIndexer().Add(p)
	}
}

func (f *fixture) startInformers(i informers.SharedInformerFactory, k8sI kubeinformers.SharedInformerFactory) {
	stopCh := make(chan struct{})
	defer close(stopCh)
	i.Start(stopCh)
	//k8sI.Start(stopCh)
}

func (f *fixture) run(c *Controller, name, res string) {
	f.runTest(c, name, res, false, false)
}

func (f *fixture) runQueueOnly(c *Controller, name, res string) {
	f.runTest(c, name, res, false, true)
}

func (f *fixture) runTest(c *Controller, name, res string, expectError, queueOnly bool) {

	if name != "" {
		var err error
		if res == "restores" {
			err = c.restoreSyncHandler(name, queueOnly)
		} else {
			err = c.snapshotSyncHandler(name, queueOnly)
		}
		if !expectError && err != nil {
			f.t.Errorf("error syncing custom resource: %v", err)
		} else if expectError && err == nil {
			f.t.Error("expected error syncing custom resource, got nil")
		}
	}

	actions := filterInformerActions(f.client.Actions())
	for i, action := range actions {
		if len(f.actions) < i+1 {
			f.t.Errorf("%d unexpected actions: %+v", len(actions)-len(f.actions), actions[i:])
			break
		}

		expectedAction := f.actions[i]
		checkAction(expectedAction, action, f.t)
	}

	if len(f.actions) > len(actions) {
		f.t.Errorf("%d additional expected actions:%+v", len(f.actions)-len(actions), f.actions[len(actions):])
	}
}

// checkAction verifies that expected and actual actions are equal and both have
// same attached resources
func checkAction(expected, actual core.Action, t *testing.T) {
	if !(expected.Matches(actual.GetVerb(), actual.GetResource().Resource) &&
		actual.GetSubresource() == expected.GetSubresource()) {
		t.Errorf("Expected\n\t%#v\ngot\n\t%#v", expected, actual)
		return
	}

	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		t.Errorf("Action has wrong type. Expected: %t. Got: %t", expected, actual)
		return
	}

	if e, ok := expected.(core.CreateAction); ok {
		expObject := e.GetObject()
		a, _ := actual.(core.CreateAction)
		object := a.GetObject()

		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintDiff(expObject, object))
		}
	}
	if e, ok := expected.(core.UpdateAction); ok {
		expObject := e.GetObject()
		a, _ := actual.(core.UpdateAction)
		object := a.GetObject()

		if !reflect.DeepEqual(expObject, object) {
			t.Errorf("Action %s %s has wrong object\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintDiff(expObject, object))
		}
	}
	if e, ok := expected.(core.PatchAction); ok {
		expPatch := e.GetPatch()
		a, _ := actual.(core.PatchAction)
		patch := a.GetPatch()

		if !reflect.DeepEqual(expPatch, patch) {
			t.Errorf("Action %s %s has wrong patch\nDiff:\n %s",
				a.GetVerb(), a.GetResource().Resource, diff.ObjectGoPrintDiff(expPatch, patch))
		}
	}
}

// filterInformerActions filters list and watch actions for testing resources.
// Since list and watch don't change resource state we can filter it to lower
// nose level in our tests.
func filterInformerActions(actions []core.Action) []core.Action {
	ret := []core.Action{}
	for _, action := range actions {
		if (action.Matches("get", "objectstoreconfigs") ||
			action.Matches("list", "volumesnapshots") ||
			action.Matches("get", "volumesnapshots") ||
			action.Matches("watch", "volumesnapshots")) {
			continue
		}
		ret = append(ret, action)
	}

	return ret
}

func (f *fixture) expectUpdateSnapshotAction(s *volumesnapshot.VolumeSnapshot) {
	f.actions = append(f.actions, core.NewUpdateAction(
		schema.GroupVersionResource{Resource: "volumesnapshots"}, s.Namespace, s))
}

func (f *fixture) expectDeleteSnapshotAction(s *volumesnapshot.VolumeSnapshot) {
	f.actions = append(f.actions, core.NewDeleteAction(
		schema.GroupVersionResource{Resource: "volumesnapshots"}, s.Namespace, s.Name))
}

func (f *fixture) expectUpdateRestoreAction(s *volumesnapshot.VolumeRestore) {
	f.actions = append(f.actions, core.NewUpdateAction(
		schema.GroupVersionResource{Resource: "volumerestores"}, s.Namespace, s))
}

func (f *fixture) expectDeleteRestoreAction(s *volumesnapshot.VolumeRestore) {
	f.actions = append(f.actions, core.NewDeleteAction(
		schema.GroupVersionResource{Resource: "volumerestores"}, s.Namespace, s.Name))
}

func SnapshotTestCase(c *Case, t *testing.T) {
	f := newFixture(t)

	for _, s := range c.snapshots {
		f.snapshotLister = append(f.snapshotLister, s)
		f.objects = append(f.objects, s)
	}
	for _, config := range c.configs {
		f.objects = append(f.objects, config)
	}
	for _, secret := range c.secrets {
		f.kubeobjects = append(f.kubeobjects, secret)
	}

	cntl, i, k8sI := f.newController()

	for _, us := range c.updatedSnapshots {
		f.expectUpdateSnapshotAction(us)
	}
	for _, ds := range c.deleteSnapshots {
		f.expectDeleteSnapshotAction(ds)
	}

	snapshotErr = c.snaperror

	f.initInformers(i, k8sI)
	f.startInformers(i, k8sI)

	if c.queueOnly {
		f.runQueueOnly(cntl, "default/"+c.handleKey, "snapshots")
	} else {
		f.run(cntl, "default/"+c.handleKey, "snapshots")
	}

	snapshotErr = nil
}

func RestoreTestCase(c *Case, t *testing.T) {
	f := newFixture(t)

	for _, s := range c.snapshots {
		f.snapshotLister = append(f.snapshotLister, s)
		f.objects = append(f.objects, s)
	}
	for _, r := range c.restores {
		f.restoreLister = append(f.restoreLister, r)
		f.objects = append(f.objects, r)
	}
	for _, config := range c.configs {
		f.objects = append(f.objects, config)
	}
	for _, secret := range c.secrets {
		f.kubeobjects = append(f.kubeobjects, secret)
	}

	cntl, i, k8sI := f.newController()

	for _, ur := range c.updatedRestores {
		f.expectUpdateRestoreAction(ur)
	}
	for _, dr := range c.deleteRestores {
		f.expectDeleteRestoreAction(dr)
	}

	restoreErr = c.restoreerror

	f.initInformers(i, k8sI)
	f.startInformers(i, k8sI)

	if c.queueOnly {
		f.runQueueOnly(cntl, "default/"+c.handleKey, "restores")
	} else {
		f.run(cntl, "default/"+c.handleKey, "restores")
	}

	restoreErr = nil
}

func TestQueues(t *testing.T) {

	utilruntime.ErrorHandlers = []func(error){AddRuntimeError}
	num := 1

	cases := map[string]struct {
		snap       *volumesnapshot.VolumeSnapshot
		restore    *volumesnapshot.VolumeRestore
		objSnap    interface{}
		objRestore interface{}
		keySnap    interface{}
		keyRestore interface{}
		expSnap    *volumesnapshot.VolumeSnapshot
		expRestore *volumesnapshot.VolumeRestore
		expRErr    string
		dup        int
	}{
		"snap InQueue": {
			snap:    newConfiguredSnapshot("test1", ""),
			expSnap: newConfiguredSnapshot("test1", "InQueue"),
		},
		"restore InQueue": {
			restore:    newConfiguredRestore("test1", ""),
			expRestore: newConfiguredRestore("test1", "InQueue"),
		},
		"snap without meta": {
			objSnap: "objSnap",
			expRErr: "object has no meta",
		},
		"restore without meta": {
			objRestore: "objSnap",
			expRErr:    "object has no meta",
		},
		"snap key error": {
			keySnap: "a/b/c",
			expRErr: "invalid resource key",
		},
		"snap key is not a string": {
			keySnap: &num,
			expRErr: "expected string in workqueue but got",
		},
		"restore key error": {
			keyRestore: "a/b/c",
			expRErr:    "invalid resource key",
		},
		"restore key is not a string": {
			keyRestore: &num,
			expRErr:    "expected string in workqueue but got",
		},
	}

	for name, c := range cases {
		t.Logf("Test: %s", name)
		runtimeErrors = []error{}

		f := newFixture(t)
		if c.snap != nil {
			f.objects = append(f.objects, c.snap)
			f.snapshotLister = append(f.snapshotLister, c.snap)
		}
		if c.restore != nil {
			f.objects = append(f.objects, c.restore)
			f.restoreLister = append(f.restoreLister, c.restore)
		}
		cntl, i, k8sI := f.newController()
		f.initInformers(i, k8sI)

		if c.snap != nil {
			cntl.enqueueSnapshot(c.snap)
			cntl.processNextSnapshotItem(false)
		}
		if c.objSnap != nil {
			cntl.enqueueSnapshot(c.objSnap)
		}
		if c.keySnap != nil {
			cntl.snapshotQueue.AddRateLimited(c.keySnap)
			cntl.processNextSnapshotItem(false)
		}
		if c.expSnap != nil {
			hSnap, _ := cntl.vsclientset.VolumesnapshotV1alpha1().VolumeSnapshots(cntl.namespace).Get(
				context.TODO(), c.snap.GetName(), metav1.GetOptions{})
			if !reflect.DeepEqual(c.expSnap, hSnap) {
				t.Errorf("handled snapshot not matched in case [%s]\nexpected : %v\nbut got  : %v", name, c.expSnap, hSnap)
			}
		}

		if c.restore != nil {
			cntl.enqueueRestore(c.restore)
			cntl.processNextRestoreItem(false)
		}
		if c.objRestore != nil {
			cntl.enqueueRestore(c.objRestore)
		}
		if c.keyRestore != nil {
			cntl.restoreQueue.AddRateLimited(c.keyRestore)
			cntl.processNextRestoreItem(false)
		}
		if c.expRestore != nil {
			hRestore, _ := cntl.vsclientset.VolumesnapshotV1alpha1().VolumeRestores(cntl.namespace).Get(
				context.TODO(), c.restore.GetName(), metav1.GetOptions{})
			if !reflect.DeepEqual(c.expRestore, hRestore) {
				t.Errorf("handled restore not matched in case [%s]\nexpected : %v\nbut got  : %v", name, c.expRestore, hRestore)
			}
		}

		if c.expRErr != "" {
			if len(runtimeErrors) == 0 {
				t.Errorf("Expected error not occurred in case [%s] : %s", name, c.expRErr)
			} else if !strings.Contains(runtimeErrors[0].Error(), c.expRErr) {
				t.Errorf("error not matched in case [%s]\nexpected : %s\nbut got  : %s", name, c.expRErr, runtimeErrors[0].Error())
			}
		} else {
			if len(runtimeErrors) > 0 {
				t.Errorf("Unexpected error occurred in case [%s] : %s", name, runtimeErrors[0].Error())
			}
		}
	}
}

type bucketMock struct {
	objectstore.Objectstore
}

func (b *bucketMock) GetName() string {
	return "config"
}

func (b *bucketMock) GetEndpoint() string {
	return "example.com"
}

func (b *bucketMock) GetBucketName() string {
	return "bucket"
}

var bucketFound bool

func (b *bucketMock) ChkBucket() (bool, error) {
	return bucketFound, nil
}

func (b *bucketMock) CreateBucket() error {
	return nil
}

var deleteFilename string

func (b *bucketMock) Delete(filename string) error {
	deleteFilename = filename
	return nil
}

/*
var downloadFilename string

func (b *bucketMock) Download(file *os.File, filename string) error {
	downloadFilename = filename
	return nil
}
*/

var objectInfoList []objectstore.ObjectInfo

func (b *bucketMock) ListObjectInfo() ([]objectstore.ObjectInfo, error) {
	return objectInfoList, nil
}

var bucketMockErr error

func getBucketMock(namespace, objectstoreConfig string, kubeclient kubernetes.Interface,
	client clientset.Interface, insecure bool) (objectstore.Objectstore, error) {
	return &bucketMock{}, bucketMockErr
}

var runtimeErrors []error

func AddRuntimeError(err error) {
	runtimeErrors = append(runtimeErrors, err)
}

func TestDeleteSnapshot(t *testing.T) {

	utilruntime.ErrorHandlers = []func(error){AddRuntimeError}
	othersnap := newConfiguredSnapshot("test1", "Completed")
	othersnap.SetNamespace("otherns")

	cases := map[string]struct {
		snap      *volumesnapshot.VolumeSnapshot
		delSnap   interface{}
		delErr    error
		restored  *volumesnapshot.VolumeSnapshot
		bucketErr error
		expRErr   string
	}{
		"Successfully deleted": {
			delSnap: newConfiguredSnapshot("test1", "Completed"),
		},
		"Restored on delete error": {
			delSnap:  newConfiguredSnapshot("test1", "Completed"),
			delErr:   fmt.Errorf("Some delete snapshot error"),
			restored: newConfiguredSnapshot("test1", "DeleteFailed"),
		},
		"GetBucket error": {
			delSnap:   newConfiguredSnapshot("test1", "Completed"),
			bucketErr: fmt.Errorf("Mock Bucket error"),
			expRErr:   "Mock Bucket error",
		},
		"object error": {
			delSnap:   newConfiguredRestore("test1", "Completed"), // not a snapshot
			bucketErr: fmt.Errorf("Must not reach here"),
		},
		"other namespace": {
			delSnap:   othersnap,
			bucketErr: fmt.Errorf("Must not reach here"),
		},
		"create error": {
			snap:    newConfiguredSnapshot("test1", "Completed"),
			delSnap: newConfiguredSnapshot("test1", "Completed"),
			delErr:  fmt.Errorf("Some delete snapshot error"),
			expRErr: "already exists",
		},
	}

	cases["Restored on delete error"].restored.Status.Reason = "Some delete snapshot error"

	for name, c := range cases {
		t.Logf("Test: %s", name)
		runtimeErrors = []error{}
		f := newFixture(t)
		if c.snap != nil {
			f.objects = append(f.objects, c.snap)
			f.snapshotLister = append(f.snapshotLister, c.snap)
		}
		cntl, i, k8sI := f.newController()
		cntl.getBucket = getBucketMock
		bucketMockErr = c.bucketErr
		f.initInformers(i, k8sI)

		deleteErr = c.delErr
		cntl.deleteSnapshot(c.delSnap)
		if c.restored != nil {
			restored, err := cntl.vsclientset.VolumesnapshotV1alpha1().VolumeSnapshots(cntl.namespace).Get(
				context.TODO(), "test1", metav1.GetOptions{})
			if err != nil {
				t.Errorf("Error getting restored snapshot : %s", err.Error())
			} else if !reflect.DeepEqual(c.restored, restored) {
				t.Errorf("restored snapshot not matched in case [%s]\nexpected : %v\nbut got  : %v", name, c.restored, restored)
			}
		}
		if c.expRErr != "" {
			if len(runtimeErrors) == 0 {
				t.Errorf("Expected error not occurred in case [%s] : %s", name, c.expRErr)
			} else if !strings.Contains(runtimeErrors[0].Error(), c.expRErr) {
				t.Errorf("error not matched in case [%s]\nexpected : %s\nbut got  : %s", name, c.expRErr, runtimeErrors[0].Error())
			}
		} else {
			if len(runtimeErrors) > 0 {
				t.Errorf("Unexpected error occurred in case [%s] : %s", name, runtimeErrors[0].Error())
			}
		}
	}

	bucketMockErr = nil
}

func TestControllerRun(t *testing.T) {

	ns := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
		},
	}
	config := newObjectstoreConfig()
	objectInfoList = []objectstore.ObjectInfo{
		objectstore.ObjectInfo{
			Name:             "test1.tgz",
			Size:             int64(131072),
			Timestamp:        time.Date(2001, 5, 20, 23, 59, 59, 0, time.UTC),
			BucketConfigName: "bucket",
		},
	}
	snap := newConfiguredSnapshot("test1", "Completed")

	f := newFixture(t)
	f.kubeobjects = append(f.kubeobjects, ns)
	f.objects = append(f.objects, config)
	f.objects = append(f.objects, snap)
	f.snapshotLister = append(f.snapshotLister, snap)
	cntl, i, k8sI := f.newController()
	cntl.getBucket = getBucketMock
	f.initInformers(i, k8sI)

	// Run controller
	var err error
	bucketFound = true
	stopCh := make(chan struct{})
	go func() {
		err = cntl.Run(1, 1, stopCh)
	}()
	time.Sleep(1 * time.Second)
	close(stopCh)
	if err != nil {
		t.Errorf("Error in controller Run : %s", err.Error())
	}

	// Run controller bucket not found
	bucketFound = false
	stopCh = make(chan struct{})
	go func() {
		err = cntl.Run(1, 1, stopCh)
	}()
	time.Sleep(1 * time.Second)
	close(stopCh)
	if err != nil {
		t.Errorf("Error in controller Run : %s", err.Error())
	}
}
