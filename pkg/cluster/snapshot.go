package cluster

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//corev1 "k8s.io/api/core/v1"
	"github.com/cenkalti/backoff"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/utils"
)

func matchVerbs(groupVersion string, r *metav1.APIResource) bool {
	return discovery.SupportsAllVerbs{Verbs: []string{"list", "create", "get", "delete"}}.Match(groupVersion, r)
}

func isOlderValidResourceVersion(rv, refrv string) bool {
	irv, err := strconv.ParseInt(rv, 10, 64)
	if err != nil {
		return false
	}
	irefrv, err := strconv.ParseInt(refrv, 10, 64)
	if err != nil {
		return false
	}
	return (irv < irefrv)
}

func isNewerValidResourceVersion(rv, refrv string) bool {
	irv, err := strconv.ParseInt(rv, 10, 64)
	if err != nil {
		return false
	}
	irefrv, err := strconv.ParseInt(refrv, 10, 64)
	if err != nil {
		return false
	}
	return (irv > irefrv)
}

func stopWatch(eventsWatch map[schema.GroupVersionResource]watch.Interface) {
	// Stop watch resources
	for gvr, w := range eventsWatch {
		if w != nil {
			w.Stop()
		} else {
			klog.V(4).Infof("+++ %s watch already exited", gvr)
		}
	}
}

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

// Snapshot k8s resources
func Snapshot(snapshot *cbv1alpha1.Snapshot) error {

	// kubeClient for external cluster.
	kubeClient, err := buildKubeClient(snapshot.Spec.Kubeconfig)
	if err != nil {
		return err
	}

	// DynamicClient for external cluster.
	dynamicClient, err := buildDynamicClient(snapshot.Spec.Kubeconfig)
	if err != nil {
		return err
	}

	return SnapshotWithClient(snapshot, kubeClient, dynamicClient)
}

// SnapshotWithClient takes a snapshot of k8s resources
func SnapshotWithClient(
	snapshot *cbv1alpha1.Snapshot,
	kubeClient kubernetes.Interface,
	dynamicClient dynamic.Interface) error {

	// Snapshot log
	blog := utils.NewNamedLog("snapshot:" + snapshot.ObjectMeta.Name)

	discoveryClient := kubeClient.Discovery()

	spr, err := discoveryClient.ServerResources()
	if err != nil {

		// This is the first time that k8s api of  target cluster accessed
		if apiPermError(err.Error()) {
			return backoff.Permanent(fmt.Errorf("Get server preferred resources failed : %s", err.Error()))
		}

		return fmt.Errorf("Get server preferred resources failed : %s", err.Error())
	}
	resources := discovery.FilteredBy(discovery.ResourcePredicateFunc(matchVerbs), spr)

	blog.Info("Backing up resources")

	eventsWatch := make(map[schema.GroupVersionResource]watch.Interface)
	watchgvr := make(map[schema.GroupVersionResource]string)
	snapshotList := make([]unstructured.Unstructured, 0)
	watchEventList := make([]watch.Event, 0)

	// goroutine gc
	defer stopWatch(eventsWatch)

	// Generate marker name
	markerName := "resource-version-marker-" + utils.RandString(10)

	// Get start resource version
	marker, err := ConfigMapMarker(kubeClient, markerName)
	if err != nil {
		return fmt.Errorf("Making start config map marker failed : %s", err.Error())
	}
	startRV := marker.ObjectMeta.ResourceVersion

	for _, resourceGroup := range resources {
		gv, err := schema.ParseGroupVersion(resourceGroup.GroupVersion)
		if err != nil {
			return fmt.Errorf("unable to parse GroupVersion %s : %s", resourceGroup.GroupVersion, err.Error())
		}
		blog.Infof("- GroupVersion : %s", resourceGroup.GroupVersion)

		for _, resource := range resourceGroup.APIResources {

			// exclude resource 'nodes' and 'events' on snapshot
			if resource.Name == "nodes" || resource.Name == "events" {
				continue
			}

			// Get list of a resource
			gvr := gv.WithResource(resource.Name)
			unstructuredList, err := dynamicClient.Resource(gvr).List(metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("Get resource %s list failed : %s", resource.Name, err.Error())
			}

			// Start watching the resource
			watchgvr[gvr] = resourceGroup.GroupVersion + "/" + resource.Name
			eventsWatch[gvr], err = dynamicClient.Resource(gvr).Watch(metav1.ListOptions{ResourceVersion: startRV})
			if err != nil {
				return fmt.Errorf("Watch resource %s list failed : %s", resource.Name, err.Error())
			}
			go func() {
				klog.V(4).Infof("+++ %s watch started", watchgvr[gvr])
				for e := range eventsWatch[gvr].ResultChan() {
					item, ok := e.Object.(*unstructured.Unstructured)
					if ok {
						switch e.Type {
						case watch.Added:
							klog.V(4).Infof("!!! Resource added : %s - rv:%s", item.GetSelfLink(), item.GetResourceVersion())
						case watch.Modified:
							klog.V(4).Infof("!!! Resource modified : %s - rv:%s", item.GetSelfLink(), item.GetResourceVersion())
						case watch.Deleted:
							klog.V(4).Infof("!!! Resource deleted : %s - rv:%s", item.GetSelfLink(), item.GetResourceVersion())
						}
					}
					watchEventList = append(watchEventList, e)
				}
				klog.V(4).Infof("+++ %s watch exiting", watchgvr[gvr])
			}()

			blog.Infof("-- %3d %s", len(unstructuredList.Items), resource.Name)

			// Join resource list
			snapshotList = append(snapshotList, unstructuredList.Items...)
		}
	}

	// Get end resource version
	marker, err = ConfigMapMarker(kubeClient, markerName)
	if err != nil {
		return fmt.Errorf("Making end config map marker failed : %s", err.Error())
	}
	endRV := marker.ObjectMeta.ResourceVersion
	blog.Infof("Start resource version : %s", startRV)
	blog.Infof("End resource version   : %s", endRV)

	// Stop watch resources and wait few seconds for running goroutines exiting
	stopWatch(eventsWatch)
	time.Sleep(3 * time.Second)

	// Sync resources
	blog.Infof("Syncing modified resources: %d events", len(watchEventList))
	for _, e := range watchEventList {
		item, ok := e.Object.(*unstructured.Unstructured)
		if ok {
			message := "unknown type"
			if !isOlderValidResourceVersion(item.GetResourceVersion(), endRV) {
				message = "ignored, not older than end resource version"
				blog.Infof("-- [%s] rv:%s %s - %s", e.Type, item.GetResourceVersion(), item.GetSelfLink(), message)
				continue
			}
			targetIndex := -1
			for i, u := range snapshotList {
				if u.GetSelfLink() == item.GetSelfLink() {
					targetIndex = i
					break
				}
			}
			if targetIndex >= 0 {
				if isNewerValidResourceVersion(item.GetResourceVersion(), snapshotList[targetIndex].GetResourceVersion()) {
					switch e.Type {
					case watch.Added, watch.Modified:
						snapshotList[targetIndex] = *item
						message = "applied"
					case watch.Deleted:
						snapshotList = append(snapshotList[:targetIndex], snapshotList[targetIndex+1:]...)
						message = "deleted"
					}
				} else {
					message = "ignored, resource version is older than stored"
				}
			} else {
				switch e.Type {
				case watch.Added, watch.Modified:
					snapshotList = append(snapshotList, *item)
					message = "added"
				case watch.Deleted:
					message = "already deleted"
				}
			}
			blog.Infof("-- [%s] rv:%s %s - %s", e.Type, item.GetResourceVersion(), item.GetSelfLink(), message)
		} else {
			blog.Infof("-- [%s] %#v", e.Type, e.Object)
		}
	}

	// snapshot file
	snapshotFile, err := os.Create("/tmp/" + snapshot.ObjectMeta.Name + ".tgz")
	if err != nil {
		return fmt.Errorf("Creating tgz file failed : %s", err.Error())
	}
	tgz := gzip.NewWriter(snapshotFile)
	defer tgz.Close()

	tarWriter := tar.NewWriter(tgz)
	defer tarWriter.Close()

	// Write resources into json
	snapshot.Status.Contents = nil
	snapshot.Status.NumberOfContents = 0
	for _, item := range snapshotList {

		// Resources stored according to api path.
		itempath := item.GetSelfLink()
		// Namespaces and CRDs stored on top level.
		if item.GetKind() == "Namespace" {
			itempath = filepath.Join("/namespaces", item.GetName())
		}
		if item.GetKind() == "CustomResourceDefinition" {
			itempath = filepath.Join("/crds", item.GetName())
		}

		// snapshot item
		content, err := item.MarshalJSON()
		if err != nil {
			return fmt.Errorf("Marshalling json failed : %s", err.Error())
		}
		hdr := &tar.Header{
			Name:     filepath.Join(snapshot.ObjectMeta.Name, itempath+".json"),
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
			Mode:     0755,
			ModTime:  time.Now(),
		}
		if err := tarWriter.WriteHeader(hdr); err != nil {
			return fmt.Errorf("Tar writer writing header failed : %s", err.Error())
		}
		if _, err := tarWriter.Write(content); err != nil {
			return fmt.Errorf("Tar writer writing content failed : %s", err.Error())
		}

		// Contents
		snapshot.Status.Contents = append(snapshot.Status.Contents, itempath)
		snapshot.Status.NumberOfContents++
	}

	blog.Info("Making snapshot.json")
	snapshot.Status.SnapshotTimestamp = marker.ObjectMeta.CreationTimestamp
	// Set expiration
	if snapshot.Spec.AvailableUntil.IsZero() {
		snapshot.Status.AvailableUntil = metav1.NewTime(marker.ObjectMeta.CreationTimestamp.Add(snapshot.Spec.TTL.Duration))
		snapshot.Status.TTL = snapshot.Spec.TTL
	} else {
		snapshot.Status.AvailableUntil = snapshot.Spec.AvailableUntil
		snapshot.Status.TTL.Duration = snapshot.Status.AvailableUntil.Time.Sub(snapshot.Status.SnapshotTimestamp.Time)
	}
	snapshot.Status.SnapshotResourceVersion = endRV

	// Sort Contents
	sort.Strings(snapshot.Status.Contents)
	snapshotCopy := snapshot.DeepCopy()
	snapshotCopy.Status.Phase = ""
	snapshotCopy.TypeMeta.SetGroupVersionKind(cbv1alpha1.SchemeGroupVersion.WithKind("Snapshot"))
	snapshotCopy.ObjectMeta.SetResourceVersion("")
	snapshotCopy.ObjectMeta.SetUID("")

	// Store snapshot resource as snapshot.json
	snapshotResource, err := json.Marshal(snapshotCopy)
	if err != nil {
		return fmt.Errorf("Marshalling snapshot.json failed : %s", err.Error())
	}
	hdr := &tar.Header{
		Name:     filepath.Join(snapshot.ObjectMeta.Name, "snapshot.json"),
		Size:     int64(len(snapshotResource)),
		Typeflag: tar.TypeReg,
		Mode:     0755,
		ModTime:  time.Now(),
	}
	if err := tarWriter.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar writer snapshot.json header failed : %s", err.Error())
	}
	if _, err := tarWriter.Write(snapshotResource); err != nil {
		return fmt.Errorf("tar writer snapshot.json content failed : %s", err.Error())
	}

	tarWriter.Close()
	tgz.Close()
	snapshotFile.Close()

	return nil
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

// UploadSnapshot uploads a snapshot tgz file to the bucket
func UploadSnapshot(snapshot *cbv1alpha1.Snapshot, bucket objectstore.Objectstore) error {

	// Snapshot log
	blog := utils.NewNamedLog("snapshot:" + snapshot.ObjectMeta.Name)

	snapshotFile, err := os.Open("/tmp/" + snapshot.ObjectMeta.Name + ".tgz")
	defer snapshotFile.Close()
	if err != nil {
		return backoff.Permanent(fmt.Errorf("Re-opening tgz file failed : %s", err.Error()))
	}
	blog.Infof("Uploading file %s", snapshot.ObjectMeta.Name+".tgz")
	err = bucket.Upload(snapshotFile, snapshot.ObjectMeta.Name+".tgz")
	if err != nil {
		if objectstorePermError(err.Error()) {
			return backoff.Permanent(fmt.Errorf("Uploading tgz file failed : %s", err.Error()))
		}
		return fmt.Errorf("Uploading tgz file failed : %s", err.Error())
	}

	objInfo, err := bucket.GetObjectInfo(snapshot.ObjectMeta.Name + ".tgz")
	if err != nil {
		return fmt.Errorf("Getting objectstore file info failed : %s", err.Error())
	}

	// Timestamps and size
	snapshot.Status.StoredTimestamp = metav1.NewTime(objInfo.Timestamp)
	snapshot.Status.StoredFileSize = objInfo.Size
	blog.Info("Upload completed")
	blog.Infof("-- resource version : %s", snapshot.Status.SnapshotResourceVersion)
	blog.Infof("-- snapshot timestamp : %s", snapshot.Status.SnapshotTimestamp)
	blog.Infof("-- available until  : %s", snapshot.Status.AvailableUntil)
	blog.Infof("-- num resources    : %d", snapshot.Status.NumberOfContents)
	blog.Infof("-- stored file size : %d", snapshot.Status.StoredFileSize)
	blog.Infof("-- stored timestamp : %s", snapshot.Status.StoredTimestamp)

	return nil
}
