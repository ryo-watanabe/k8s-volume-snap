package cluster

import (
	"flag"
	"os"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/klog"

	clustersnapshot "github.com/ryo-watanabe/k8s-snap/pkg/apis/clustersnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-snap/pkg/objectstore"
)

var kubeobjects []runtime.Object
var ukubeobjects []runtime.Object

func TestCluster(t *testing.T) {

	// Init klog
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Parse()
	klog.Infof("k8s-snap pkg cluster test")
	klog.Flush()

	// Define API Resources
	kubeClient := k8sfake.NewSimpleClientset(kubeobjects...)
	res := make([]*metav1.APIResourceList, 0)
	res = setAPIResourceList(res, "", "v1", "namespaces", "Namespace", true)
	res = setAPIResourceList(res, "", "v1", "secrets", "Secret", true)
	res = setAPIResourceList(res, "", "v1", "configmaps", "ConfigMaps", true)
	res = setAPIResourceList(res, "", "v1", "services", "Service", true)
	res = setAPIResourceList(res, "", "v1", "endpoints", "Endpoints", true)
	res = setAPIResourceList(res, "", "v1", "pods", "Pod", true)
	res = setAPIResourceList(res, "rbac.authorization.k8s.io", "v1", "clusterrolebindings", "ClusterRoleBinding", false)
	res = setAPIResourceList(res, "rbac.authorization.k8s.io", "v1", "clusterroles", "ClusterRole", false)
	res = setAPIResourceList(res, "apiextensions.k8s.io", "v1", "customresourcedefinitions", "CustomResourceDefinition", false)
	res = setAPIResourceList(res, "storage.k8s.io", "v1", "storageclasses", "StorageClass", false)
	res = setAPIResourceList(res, "", "v1", "componentstatuses", "ComponentStatus", false)
	res = setAPIResourceList(res, "", "v1", "persistentvolumes", "PersistentVolume", false)
	res = setAPIResourceList(res, "", "v1", "persistentvolumeclaims", "PersistentVoluemClaims", false)
	kubeClient.Discovery().(*discoveryfake.FakeDiscovery).Fake.Resources = res

	// Set resouces stored in snapshots
	ukubeobjects = append(ukubeobjects, unstrctrdResource("", "v1", "", "ns1", "Namespace", "namespaces").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newConfiguredSecret("secret1", corev1.SecretTypeOpaque)))
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newConfiguredSecret("token1", corev1.SecretTypeServiceAccountToken)))
	ukubeobjects = append(ukubeobjects, unstrctrdResource("", "v1", "default", "svc1", "Service", "services").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("", "v1", "default", "svc1", "Endpoints", "endpoints").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("", "v1", "kube-system", "secret1", "Secret", "secrets").DeepCopyObject())
	pod := unstrctrdResource("", "v1", "default", "pod1", "Pod", "pods")
	pod.SetOwnerReferences([]metav1.OwnerReference{metav1.OwnerReference{Name: "Pod-owner"}})
	ukubeobjects = append(ukubeobjects, pod.DeepCopyObject())
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newClusterRoleBinding("default")).DeepCopyObject())
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newClusterRoleBinding("kube-system")).DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("rbac.authorization.k8s.io", "v1", "", "cluster-admin", "ClusterRole", "clusterroles").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("rbac.authorization.k8s.io", "v1", "", "cluster-role1", "ClusterRole", "clusterroles").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("apiextensions.k8s.io", "v1", "", "crd.include.org", "CustomResourceDefinition", "customresourcedefinitions").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("apiextensions.k8s.io", "v1", "", "crd.exclude.org", "CustomResourceDefinition", "customresourcedefinitions").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("storage.k8s.io", "v1", "", "include-nfs-storage", "StorageClass", "storageclasses").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("storage.k8s.io", "v1", "", "exclude-nfs-storage", "StorageClass", "storageclasses").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, unstrctrdResource("", "v1", "", "controller-manager", "ComponentStatus", "componentstatuses").DeepCopyObject())
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newPV("pv1", "include-nfs-storage", "default", "pvc1")).DeepCopyObject())
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newPVC("default", "pvc1", "include-nfs-storage", "pv1")).DeepCopyObject())
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newPVC("default", "pvc2", "exclude-nfs-storage", "pv2")).DeepCopyObject())
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newPVC("default", "pvc3", "include-nfs-storage", "pv3")).DeepCopyObject())
	ukubeobjects = append(ukubeobjects, convertToUnstructured(t, newPVC("default", "pvc4", "include-nfs-storage", "")).DeepCopyObject())

	// Make dynamic client
	sch := runtime.NewScheme()
	dynamicClient := newDynamicClient(sch, 1, ukubeobjects...)
	var snapstart bool

	kubeClient.Fake.PrependReactor("create", "*", func(action core.Action) (bool, runtime.Object, error) {

		//t.Logf("Create %s called", action.GetResource().Resource)

		// Add/Delete/Modify contents during snapshot
		if snapstart {
			err := dynamicTracker.Add(convertToUnstructured(t, newConfiguredConfigMap("test1", "test1")))
			if err != nil {
				t.Errorf("Error in create configmap : %s", err.Error())
			}
			err = dynamicTracker.Update(
				schema.GroupVersionResource{Version: "v1", Resource: "configmaps"},
				convertToUnstructured(t, newConfiguredConfigMap("test1", "test1_edited")),
				"default",
			)
			if err != nil {
				t.Errorf("Error in update configmap : %s", err.Error())
			}
			err = dynamicTracker.Delete(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, "default", "test1")
			if err != nil {
				t.Errorf("Error in delete configmap : %s", err.Error())
			}
		}

		obj := action.(core.CreateAction).GetObject()
		uobj := convertToUnstructured(t, obj)
		newAction := core.NewCreateAction(action.GetResource(), action.GetNamespace(), uobj).DeepCopy()
		dynamicClient.Fake.Invokes(newAction, uobj)
		accessor, _ := meta.Accessor(obj)
		accessor.SetResourceVersion(strconv.FormatUint(dynamicTracker.GetResourceVersion(), 10))

		// toggle snapstart switch
		snapstart = !snapstart

		return false, obj, nil
	})

	kubeClient.Fake.PrependReactor("delete", "*", func(action core.Action) (bool, runtime.Object, error) {

		//t.Logf("Delete %s called", action.GetResource().Resource)

		dynamicClient.Fake.Invokes(action, nil)
		return false, nil, nil
	})

	snapstart = false
	snap := newConfiguredSnapshot("test1", "InProgress")

	// TEST1 : Get a snapshot
	err := SnapshotWithClient(snap, kubeClient, dynamicClient)
	if err != nil {
		t.Errorf("Error in snapshotWithClient : %s", err.Error())
	}
	if snap.Status.NumberOfContents != int32(len(ukubeobjects)) {
		t.Errorf(
			"Number of snapshot contents %d not equals to number of objects %d",
			snap.Status.NumberOfContents,
			len(ukubeobjects),
		)
	}

	// TEST2 : Uplaod the snapshot file
	bucket := &bucketMock{}
	objSize := int64(131072)
	objTime := time.Date(2001, 5, 20, 23, 59, 59, 0, time.UTC)
	objectInfo = &objectstore.ObjectInfo{Name: "test1.tgz", Size: objSize, Timestamp: objTime, BucketConfigName: "bucket"}
	err = UploadSnapshot(snap, bucket)
	if err != nil {
		t.Errorf("Error in UploadSnapshot : %s", err.Error())
	}
	if uploadFilename != "test1.tgz" {
		t.Error("Error upload filename not match")
	}
	if getObjectInfoFilename != "test1.tgz" {
		t.Error("Error GetObjectInfo filename not match")
	}
	if snap.Status.StoredFileSize != objSize {
		t.Error("Error file size not match")
	}
	metav1ObjTime := metav1.NewTime(objTime)
	if !snap.Status.StoredTimestamp.Equal(&metav1ObjTime) {
		t.Error("Error timestamp not match")
	}

	pref := newRestorePreference("pref1")
	restore := newConfiguredRestore("test1", "test2", "pref1", "InProgress")

	// TEST3 : Download snapshot tgz
	err = downloadSnapshot(restore, bucket)
	if err != nil {
		t.Errorf("Error in downloadSnapshot : %s", err.Error())
	}
	if downloadFilename != "test2.tgz" {
		t.Error("Error download filename not match")
	}

	// Delete PV/PVCs and Reactor for getting PVC to test restoring
	err = dynamicTracker.Delete(schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}, "", "pv1")
	if err != nil {
		t.Errorf("Error in delete pv : %s", err.Error())
	}
	err = dynamicTracker.Delete(schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}, "default", "pvc1")
	if err != nil {
		t.Errorf("Error in delete pvc : %s", err.Error())
	}
	waitcnt := 1
	dynamicClient.Fake.PrependReactor("get", "persistentvolumes", func(action core.Action) (bool, runtime.Object, error) {
		if waitcnt > 0 {
			waitcnt--
		} else {
			err = dynamicTracker.Update(
				schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"},
				convertToUnstructured(t, newPV("pv1", "include-nfs-storage", "default", "pvc1")),
				"",
			)
			if err != nil {
				t.Errorf("Error in update pv status : %s", err.Error())
			}
		}
		return false, nil, nil
	})

	// TEST4 : Restore resources
	restore = newConfiguredRestore("test1", "test1", "pref1", "InProgress")
	err = restoreResources(restore, pref, kubeClient, dynamicClient)
	if err != nil {
		t.Errorf("Error in restoreResources : %s", err.Error())
	}

	expectedNumPreferenceExcluded := 4
	if restore.Status.NumPreferenceExcluded != int32(expectedNumPreferenceExcluded) {
		t.Errorf("NumPreferenceExcluded not match : Result %d / Expected %d",
			restore.Status.NumPreferenceExcluded,
			expectedNumPreferenceExcluded,
		)
	}
	expectedExcluded := []string{
		"/api/v1/namespaces/default/persistentvolumeclaims/pvc2,(no-storageclass)",
		"/api/v1/namespaces/default/persistentvolumeclaims/pvc3,(pv-not-found)",
		"/api/v1/namespaces/default/persistentvolumeclaims/pvc4,(not-bounded)",
		"/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/cluster-admin-kube-system,(not-binded-to-ns)",
		"/apis/rbac.authorization.k8s.io/v1/clusterroles/cluster-role1,(not-binded-to-ns)",
		"/api/v1/namespaces/default/secrets/cloudCredentialSecret,(token-secret)",
		"/api/v1/namespaces/default/endpoints/svc1,(service-exists)",
		"/api/v1/namespaces/default/pods/pod1,(owner-ref)",
	}
	chkResourceList(t, restore.Status.Excluded, expectedExcluded)
	expectedCreated := []string{
		"/api/v1/persistentvolumes/pv1",
		"/api/v1/namespaces/default/persistentvolumeclaims/pvc1",
	}
	chkResourceList(t, restore.Status.Created, expectedCreated)
	expectedAlreadyExisted := []string{
		"/api/v1/namespaces/ns1",
		"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/crd.include.org",
		"/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/cluster-admin-default",
		"/apis/rbac.authorization.k8s.io/v1/clusterroles/cluster-admin",
		"/apis/storage.k8s.io/v1/storageclasses/include-nfs-storage",
		"/api/v1/namespaces/default/services/svc1",
	}
	chkResourceList(t, restore.Status.AlreadyExisted, expectedAlreadyExisted)
	expectedNumFailed := 0
	if restore.Status.NumFailed != int32(expectedNumFailed) {
		t.Errorf("NumFailed not match : Result %d / Expected %d",
			restore.Status.NumFailed,
			expectedNumFailed,
		)
	}
}

func chkResourceList(t *testing.T, res, ref []string) {
	notMatch := false
	if len(res) != len(ref) {
		notMatch = true
	}
	for _, x := range res {
		notFound := true
		for _, y := range ref {
			if x == y {
				notFound = false
				break
			}
		}
		if notFound {
			notMatch = true
			break
		}
	}
	if notMatch {
		t.Errorf("List not match\nResult : %v\nExpected : %v", res, ref)
	}
}

// Mock bucket

type bucketMock struct {
	objectstore.Objectstore
}

var uploadFilename string

func (b bucketMock) Upload(file *os.File, filename string) error {
	uploadFilename = filename
	return nil
}

var downloadFilename string

func (b bucketMock) Download(file *os.File, filename string) error {
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

var dynamicTracker ObjectTracker

func newDynamicClient(scheme *runtime.Scheme, rv uint64, objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	// In order to use List with this client, you have to have the v1.List registered in your scheme. Neat thing though
	// it does NOT have to be the *same* list
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "fake-dynamic-client-group", Version: "v1", Kind: "List"}, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"}, &unstructured.Unstructured{})

	codecs := serializer.NewCodecFactory(scheme)
	dynamicTracker = NewObjectTracker(scheme, codecs.UniversalDecoder(), rv)
	for _, obj := range objects {
		if err := dynamicTracker.Add(obj); err != nil {
			panic(err)
		}
	}

	cs := &dynamicfake.FakeDynamicClient{}
	cs.AddReactor("*", "*", core.ObjectReaction(dynamicTracker))
	cs.AddWatchReactor("*", func(action core.Action) (handled bool, ret watch.Interface, err error) {
		gvr := action.GetResource()
		ns := action.GetNamespace()
		watch, err := dynamicTracker.Watch(gvr, ns)
		if err != nil {
			return false, nil, err
		}
		return true, watch, nil
	})

	return cs
}

func convertToUnstructured(t *testing.T, obj runtime.Object) runtime.Object {
	maped, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Errorf("Error converting object to unstructured : %s", err.Error())
	}
	unstrctrd := &unstructured.Unstructured{Object: maped}
	return unstrctrd.DeepCopyObject()
}

func unstrctrdResource(group, version, ns, name, kind, resourcename string) *unstructured.Unstructured {
	item := &unstructured.Unstructured{}
	item.SetGroupVersionKind(schema.GroupVersionKind{Group: group, Version: version, Kind: kind})
	item.SetName(name)
	gvpath := "/apis/" + group + "/" + version
	if group == "" {
		gvpath = "/api/v1"
	}
	if ns == "" {
		item.SetSelfLink(gvpath + "/" + resourcename + "/" + name)
	} else {
		item.SetNamespace(ns)
		item.SetSelfLink(gvpath + "/namespaces/" + ns + "/" + resourcename + "/" + name)
	}
	return item
}

func setAPIResourceList(resources []*metav1.APIResourceList, group, version, name, kind string, namespaced bool) []*metav1.APIResourceList {
	gv := group + "/" + version
	if group == "" {
		gv = version
	}
	for _, r := range resources {
		if r.GroupVersion == gv {
			r.APIResources = append(r.APIResources, metav1.APIResource{
				Name: name, Group: group, Version: version, Kind: kind, Namespaced: namespaced,
				Verbs: []string{"list", "create", "get", "delete"},
			})
			return resources
		}
	}
	resources = append(resources, &metav1.APIResourceList{
		GroupVersion: gv,
		APIResources: []metav1.APIResource{
			metav1.APIResource{
				Name: name, Group: group, Version: version, Kind: kind, Namespaced: namespaced,
				Verbs: []string{"list", "create", "get", "delete"},
			},
		},
	})
	return resources
}

// Test resources

func newConfiguredSnapshot(name, phase string) *clustersnapshot.Snapshot {
	return &clustersnapshot.Snapshot{
		TypeMeta: metav1.TypeMeta{APIVersion: clustersnapshot.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: clustersnapshot.SnapshotSpec{
			ClusterName:       name,
			Kubeconfig:        "kubeconfig",
			ObjectstoreConfig: "objectstoreConfig",
		},
		Status: clustersnapshot.SnapshotStatus{
			Phase: phase,
		},
	}
}

func newConfiguredSecret(name string, stype corev1.SecretType) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			SelfLink:  "/api/v1/namespaces/default/secrets/cloudCredentialSecret",
		},
		Type: stype,
	}
}

func newConfiguredConfigMap(name, data string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
			SelfLink:  "/api/v1/namespaces/default/configmaps/" + name,
		},
		Data: map[string]string{
			"message": data,
		},
	}
}

func newRestorePreference(name string) *clustersnapshot.RestorePreference {
	return &clustersnapshot.RestorePreference{
		TypeMeta: metav1.TypeMeta{APIVersion: clustersnapshot.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: clustersnapshot.RestorePreferenceSpec{
			ExcludeNamespaces:        []string{"kube-system"},
			ExcludeCRDs:              []string{"crd.exclude.org"},
			ExcludeAPIPathes:         []string{"/api/v1,componentstatuses"},
			RestoreAppAPIPathes:      []string{"/api/v1,pods", "/api/v1,services", "/api/v1,endpoints"},
			RestoreNfsStorageClasses: []string{"include-nfs-storage"},
		},
	}
}

func newConfiguredRestore(name, snapshot, preference, phase string) *clustersnapshot.Restore {
	return &clustersnapshot.Restore{
		TypeMeta: metav1.TypeMeta{APIVersion: clustersnapshot.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: clustersnapshot.RestoreSpec{
			ClusterName:           name,
			Kubeconfig:            "kubeconfig",
			SnapshotName:          snapshot,
			RestorePreferenceName: preference,
		},
		Status: clustersnapshot.RestoreStatus{
			Phase: phase,
		},
	}
}

func newClusterRoleBinding(ns string) *rbac.ClusterRoleBinding {
	return &rbac.ClusterRoleBinding{
		TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{
			Name:     "cluster-admin-" + ns,
			SelfLink: "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/cluster-admin-" + ns,
		},
		Subjects: []rbac.Subject{
			rbac.Subject{Kind: "ServiceAccount", Name: "default", Namespace: ns},
		},
		RoleRef: rbac.RoleRef{Kind: "ClusterRole", Name: "cluster-admin"},
	}
}

func newPV(name, class, claimns, claimname string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{
			Name:     name,
			SelfLink: "/api/v1/persistentvolumes/" + name,
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef:         &corev1.ObjectReference{Namespace: claimns, Name: claimname},
			StorageClassName: class,
		},
		Status: corev1.PersistentVolumeStatus{
			Phase: "Bound",
		},
	}
}

func newPVC(ns, name, class, pvname string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			SelfLink:  "/api/v1/namespaces/" + ns + "/persistentvolumeclaims/" + name,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &class,
			VolumeName:       pvname,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			Phase: "Bound",
		},
	}
}
