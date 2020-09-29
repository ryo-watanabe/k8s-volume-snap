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

	cbv1alpha1 "github.com/ryo-watanabe/k8s-snap/pkg/apis/clustersnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-snap/pkg/objectstore"
	"github.com/ryo-watanabe/k8s-snap/pkg/utils"
)

func loadItem(item *unstructured.Unstructured, filepath string) error {
	bytes, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}
	err = item.UnmarshalJSON(bytes)
	if err != nil {
		return err
	}
	return nil
}

// Get resoutrce string from GetSelfLink
func resourceFromSelfLink(selflink string) string {
	s := strings.Split(selflink, "/")
	if len(s) >= 2 {
		return s[len(s)-2]
	}
	return ""
}

// Create resource
func createItem(item *unstructured.Unstructured, dyn dynamic.Interface) (*unstructured.Unstructured, error) {
	gv, err := schema.ParseGroupVersion(item.GetAPIVersion())
	if err != nil {
		return nil, err
	}
	gvr := gv.WithResource(resourceFromSelfLink(item.GetSelfLink()))
	ns := item.GetNamespace()
	if ns == "" {
		return dyn.Resource(gvr).Create(item, metav1.CreateOptions{})
	}
	return dyn.Resource(gvr).Namespace(ns).Create(item, metav1.CreateOptions{})
}

func excludeWithMsg(restore *cbv1alpha1.Restore, rlog *utils.NamedLog, selflink, msg string) {
	rlog.Infof("     [Excluded] %s", msg)
	restore.Status.NumExcluded++
	restore.Status.Excluded = append(restore.Status.Excluded, selflink+",("+msg+")")
}

func alreadyExist(restore *cbv1alpha1.Restore, rlog *utils.NamedLog, selflink string) {
	rlog.Info("     [Already exists]")
	restore.Status.NumAlreadyExisted++
	restore.Status.AlreadyExisted = append(restore.Status.AlreadyExisted, selflink)
}

func created(restore *cbv1alpha1.Restore, rlog *utils.NamedLog, selflink string) {
	rlog.Info("     [Created]")
	restore.Status.NumCreated++
	restore.Status.Created = append(restore.Status.Created, selflink)
}

func failedWithMsg(restore *cbv1alpha1.Restore, rlog *utils.NamedLog, selflink, msg string) {
	rlog.Warningf("     [Failed] %s", msg)
	restore.Status.NumFailed++
	if len(msg) > 300 {
		restore.Status.Failed = append(restore.Status.Failed, selflink+","+msg[0:300]+".....")
	} else {
		restore.Status.Failed = append(restore.Status.Failed, selflink+","+msg)
	}
}

// Restore resources according to preferences.
func restoreDir(dir, restorePref string, dyn dynamic.Interface, p *preference,
	restore *cbv1alpha1.Restore, rlog *utils.NamedLog) error {

	files, err := ioutil.ReadDir(filepath.Join(dir, restorePref))
	if err != nil {
		return err
	}
	for _, f := range files {

		// Load item
		var item unstructured.Unstructured
		err := loadItem(&item, filepath.Join(dir, restorePref, f.Name()))
		if err != nil {
			return err
		}

		rlog.Infof("---- %s", item.GetSelfLink())

		// Check owner
		owners := item.GetOwnerReferences()
		if len(owners) > 0 {
			excludeWithMsg(restore, rlog, item.GetSelfLink(), "owner-ref")
			for _, owner := range owners {
				rlog.Infof("     owner : %s %s", owner.Kind, owner.Name)
			}
			continue
		}

		// Operation for each resources
		switch item.GetKind() {
		case "Secret":
			if getUnstructuredString(item.Object, "type") == "kubernetes.io/service-account-token" {
				excludeWithMsg(restore, rlog, item.GetSelfLink(), "token-secret")
				continue
			}
		case "ClusterRole":
			if !isInList(item.GetName(), p.includedClusterRoles) {
				excludeWithMsg(restore, rlog, item.GetSelfLink(), "not-binded-to-ns")
				continue
			}
		case "ClusterRoleBinding":
			if !isInList(item.GetName(), p.includedClusterRoleBindings) {
				excludeWithMsg(restore, rlog, item.GetSelfLink(), "not-binded-to-ns")
				continue
			}
		case "PersistentVolume":
		case "PersistentVolumeClaim":
			klog.Warningf("     Warning : Excluded : PVs/PVCs must not be included here")
			continue
		case "Endpoints":
			if isInList(item.GetNamespace()+"/"+item.GetName(), p.serviceList) {
				excludeWithMsg(restore, rlog, item.GetSelfLink(), "service-exists")
				continue
			}
		}

		// Restore item
		item.SetResourceVersion("")
		item.SetUID("")
		_, err = createItem(&item, dyn)
		if err != nil {
			//p.cntUpCnnotRestore(err.Error())
			if strings.Contains(err.Error(), "already exists") {
				alreadyExist(restore, rlog, item.GetSelfLink())
			} else {
				failedWithMsg(restore, rlog, item.GetSelfLink(), err.Error())
			}
		} else {
			created(restore, rlog, item.GetSelfLink())
		}
	}
	return nil
}

// create a file
func writeFile(filepath string, tarReader *tar.Reader) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := io.Copy(file, tarReader); err != nil {
		return err
	}
	return nil
}

// Restore k8s resources
func Restore(restore *cbv1alpha1.Restore, pref *cbv1alpha1.RestorePreference, bucket objectstore.Objectstore) error {
	// download snapshot tgz
	err := downloadSnapshot(restore, bucket)
	if err != nil {
		return err
	}

	// kubeClient for external cluster.
	kubeClient, err := buildKubeClient(restore.Spec.Kubeconfig)
	if err != nil {
		return err
	}

	// DynamicClient for external cluster.
	dynamicClient, err := buildDynamicClient(restore.Spec.Kubeconfig)
	if err != nil {
		return err
	}

	return restoreResources(restore, pref, kubeClient, dynamicClient)
}

func downloadSnapshot(restore *cbv1alpha1.Restore, bucket objectstore.Objectstore) error {
	// Restore log
	rlog := utils.NewNamedLog("restore:" + restore.ObjectMeta.Name)

	// Download
	rlog.Infof("Downloading file %s", restore.Spec.SnapshotName+".tgz")
	snapshotFile, err := os.Create("/tmp/" + restore.Spec.SnapshotName + ".tgz")
	defer snapshotFile.Close()
	if err != nil {
		return err
	}
	return bucket.Download(snapshotFile, restore.Spec.SnapshotName+".tgz")
}

func restoreResources(
	restore *cbv1alpha1.Restore,
	pref *cbv1alpha1.RestorePreference,
	kubeClient kubernetes.Interface,
	dynamicClient dynamic.Interface) error {

	// Restore log
	rlog := utils.NewNamedLog("restore:" + restore.ObjectMeta.Name)

	p := newPreference(pref)

	// Initialize restore status
	restore.Status.NumPreferenceExcluded = 0
	restore.Status.NumExcluded = 0
	restore.Status.NumCreated = 0
	restore.Status.NumUpdated = 0
	restore.Status.NumAlreadyExisted = 0
	restore.Status.NumFailed = 0
	restore.Status.Excluded = nil
	restore.Status.Created = nil
	restore.Status.Updated = nil
	restore.Status.AlreadyExisted = nil
	restore.Status.Failed = nil

	// Read tar.gz
	snapshotFile, err := os.Open("/tmp/" + restore.Spec.SnapshotName + ".tgz")
	if err != nil {
		return err
	}
	tgz, err := gzip.NewReader(snapshotFile)
	if err != nil {
		return err
	}
	defer tgz.Close()

	tarReader := tar.NewReader(tgz)

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}

	rlog.Info("Extract files in snapshot tgz :")
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeReg {
			path := strings.Replace(header.Name, restore.Spec.SnapshotName, "", 1)

			if path == "/snapshot.json" {
				klog.Infof("-- [Snapshot resource file] %s", path)
				continue
			}

			restorePref := p.preferedToRestore(path)
			if restorePref == "Exclude" {
				rlog.Infof("-- [%s] %s", restorePref, path)
				//p.cntUpExcluded()
				restore.Status.NumPreferenceExcluded++
				continue
			}

			// create dir
			fullpath := filepath.Join(dir, restorePref, strings.Replace(path, "/", "|", -1))
			err := os.MkdirAll(filepath.Dir(fullpath), header.FileInfo().Mode())
			if err != nil {
				return err
			}

			// create file
			err = writeFile(fullpath, tarReader)
			if err != nil {
				return err
			}
		}
	}

	// Initialize preference
	err = p.initializeByDir(dir)
	if err != nil {
		return err
	}

	// Restore namespaces
	if p.isIn("Namespace") {
		rlog.Info("Restore Namespaces :")
		err = restoreDir(dir, "Namespace", dynamicClient, p, restore, rlog)
		if err != nil {
			return err
		}
	}
	// Restore CRDs
	if p.isIn("CRD") {
		rlog.Info("Restore CRDs :")
		err = restoreDir(dir, "CRD", dynamicClient, p, restore, rlog)
		if err != nil {
			return err
		}
	}
	// Restore PV/PVC
	if p.isIn("PV") && p.isIn("PVC") {
		rlog.Info("Restore PV/PVC :")
		err = restorePV(dir, dynamicClient, p, restore, rlog)
		if err != nil {
			return err
		}
	}
	// Other resources
	if p.isIn("Restore") {
		rlog.Info("Restore resources except Apps :")
		err = restoreDir(dir, "Restore", dynamicClient, p, restore, rlog)
		if err != nil {
			return err
		}
	}
	// Restore apps
	if p.isIn("App") {
		rlog.Info("Restore Apps :")
		err = restoreDir(dir, "App", dynamicClient, p, restore, rlog)
		if err != nil {
			return err
		}
	}
	// Remove tmp files
	err = os.RemoveAll(dir)
	if err != nil {
		return err
	}

	// Generate marker name
	markerName := "resource-version-marker-" + utils.RandString(10)

	// Get end resource version
	marker, err := ConfigMapMarker(kubeClient, markerName)
	if err != nil {
		return err
	}

	// Timestamp and resource version
	restore.Status.RestoreTimestamp = marker.ObjectMeta.CreationTimestamp
	// Set expiration
	if restore.Spec.AvailableUntil.IsZero() {
		restore.Status.AvailableUntil = metav1.NewTime(marker.ObjectMeta.CreationTimestamp.Add(restore.Spec.TTL.Duration))
		restore.Status.TTL = restore.Spec.TTL
	} else {
		restore.Status.AvailableUntil = restore.Spec.AvailableUntil
		restore.Status.TTL.Duration = restore.Status.AvailableUntil.Time.Sub(restore.Status.RestoreTimestamp.Time)
	}
	restore.Status.RestoreResourceVersion = marker.ObjectMeta.ResourceVersion

	// result
	rlog.Info("Restore completed")
	rlog.Infof("-- resource version    : %s", restore.Status.RestoreResourceVersion)
	rlog.Infof("-- timestamp           : %s", restore.Status.RestoreTimestamp)
	rlog.Infof("-- available until     : %s", restore.Status.AvailableUntil)
	rlog.Infof("-- preference excluded : %d", restore.Status.NumPreferenceExcluded)
	rlog.Infof("-- excluded            : %d", restore.Status.NumExcluded)
	rlog.Infof("-- created             : %d", restore.Status.NumCreated)
	rlog.Infof("-- updated             : %d", restore.Status.NumUpdated)
	rlog.Infof("-- already existed     : %d", restore.Status.NumAlreadyExisted)
	rlog.Infof("-- failed              : %d", restore.Status.NumFailed)

	return nil
}
