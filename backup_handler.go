package main

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	cbv1alpha1 "github.com/ryo-watanabe/k8s-snap/pkg/apis/clustersnapshot/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	//"github.com/ryo-watanabe/k8s-snap/pkg/cluster"
	//"github.com/ryo-watanabe/k8s-snap/pkg/objectstore"
)

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runSnapshotWorker() {
	for c.processNextSnapshotItem(false) {
	}
}

func (c *Controller) runSnapshotQueuer() {
	for c.processNextSnapshotItem(true) {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextSnapshotItem(queueonly bool) bool {
	// Process snapshot queue
	obj, shutdown := c.snapshotQueue.Get()
	if shutdown {
		return false
	}
	err := func(obj interface{}) error {
		defer c.snapshotQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.snapshotQueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.snapshotSyncHandler(key, queueonly); err != nil {
			c.snapshotQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.snapshotQueue.Forget(obj)
		klog.V(4).Infof("Successfully synced '%s'", key)
		return nil
	}(obj)
	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

func retryNotify(err error, wait time.Duration) {
	klog.Infof("Retrying after %.2f seconds with error : %s", wait.Seconds(), err.Error())
}

// snapshotSyncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Snapshot resource
// with the current status of the resource.
func (c *Controller) snapshotSyncHandler(key string, queueonly bool) error {

	//getOptions := metav1.GetOptions{IncludeUninitialized: false}

	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Snapshot resource with this namespace/name.
	snapshot, err := c.snapshotLister.Snapshots(namespace).Get(name)

	if err != nil {
		if errors.IsNotFound(err) {
			// When deleting a snapshot, exit sync handler here.
			return nil
		}
		return err
	}

	// controller stopped wwhile taking the snapshot
	if snapshot.Status.Phase == "InProgress" {

		// check timestamp just in case
		retryend := metav1.NewTime(snapshot.ObjectMeta.CreationTimestamp.Add(time.Duration(c.maxretryelapsedsec+1) * time.Second))
		nowTime := metav1.NewTime(time.Now())
		if retryend.Before(&nowTime) {
			snapshot, err = c.updateSnapshotStatus(snapshot, "Failed", "Controller stopped while taking the snapshot")
			if err != nil {
				return err
			}
		}
	}

	// do snapshot
	if !queueonly && snapshot.Status.Phase == "InQueue" {
		snapshot, err = c.updateSnapshotStatus(snapshot, "InProgress", "")
		if err != nil {
			return err
		}

		// bucket
		bucket, err := c.getBucket(c.namespace, snapshot.Spec.ObjectstoreConfig, c.kubeclientset, c.cbclientset, c.insecure)
		if err != nil {
			snapshot, err = c.updateSnapshotStatus(snapshot, "Failed", err.Error())
			if err != nil {
				return err
			}
			return nil
		}
		klog.Infof("- Objectstore Config name:%s endpoint:%s bucket:%s", bucket.GetName(), bucket.GetEndpoint(), bucket.GetBucketName())

		// do snapshot with backoff retry
		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = time.Duration(c.maxretryelapsedsec) * time.Second
		b.RandomizationFactor = 0.2
		b.Multiplier = 2.0
		b.InitialInterval = 2 * time.Second
		operationSnapshot := func() error {
			return c.clusterCmd.Snapshot(snapshot)
		}
		err = backoff.RetryNotify(operationSnapshot, b, retryNotify)
		if err != nil {
			snapshot, err = c.updateSnapshotStatus(snapshot, "Failed", err.Error())
			if err != nil {
				return err
			}
			return nil
		}

		// upload snapshot with backoff retry
		b.Reset()
		operationUpload := func() error {
			return c.clusterCmd.UploadSnapshot(snapshot, bucket)
		}
		err = backoff.RetryNotify(operationUpload, b, retryNotify)
		if err != nil {
			snapshot, err = c.updateSnapshotStatus(snapshot, "Failed", err.Error())
			if err != nil {
				return err
			}
			return nil
		}

		snapshot, err = c.updateSnapshotStatus(snapshot, "Completed", "")
		if err != nil {
			return err
		}
	}

	nowTime := metav1.NewTime(time.Now())

	// initialize
	if snapshot.Status.Phase == "" {
		// Check AvailableUntil
		if snapshot.Spec.AvailableUntil.IsZero() {
			// Check TTL string
			if snapshot.Spec.TTL.Duration == 0 {
				snapshot.Spec.TTL.Duration = 24 * 30 * time.Hour
			}
		} else if snapshot.Spec.AvailableUntil.Before(&nowTime) {
			snapshot, err = c.updateSnapshotStatus(snapshot, "Failed", "AvailableUntil is set as past.")
			if err != nil {
				return err
			}
			// When the snapshot failed, exit sync handler here.
			return nil
		}
		snapshot, err = c.updateSnapshotStatus(snapshot, "InQueue", "")
		if err != nil {
			return err
		}
	}

	// expiration for failed snapshot
	if snapshot.Status.Phase == "Failed" && snapshot.Status.AvailableUntil.IsZero() {
		if !snapshot.Spec.AvailableUntil.IsZero() {
			snapshot.Status.AvailableUntil = snapshot.Spec.AvailableUntil
			snapshot.Status.TTL.Duration = snapshot.Status.AvailableUntil.Time.Sub(snapshot.ObjectMeta.CreationTimestamp.Time)
		} else {
			snapshot.Status.AvailableUntil = metav1.NewTime(snapshot.ObjectMeta.CreationTimestamp.Add(snapshot.Spec.TTL.Duration))
			snapshot.Status.TTL = snapshot.Spec.TTL
		}
		snapshot, err = c.updateSnapshotStatus(snapshot, snapshot.Status.Phase, snapshot.Status.Reason)
		if err != nil {
			return err
		}
	}

	// expiration edited
	if snapshot.Status.Phase == "Completed" || snapshot.Status.Phase == "Failed" {
		if !snapshot.Spec.AvailableUntil.IsZero() && !snapshot.Spec.AvailableUntil.Equal(&snapshot.Status.AvailableUntil) {
			snapshot.Status.AvailableUntil = snapshot.Spec.AvailableUntil
			snapshot, err = c.updateSnapshotStatus(snapshot, snapshot.Status.Phase, snapshot.Status.Reason)
			if err != nil {
				return err
			}
		}
	}

	// delete expired
	if !snapshot.Status.AvailableUntil.IsZero() && snapshot.Status.AvailableUntil.Before(&nowTime) {
		err := c.cbclientset.ClustersnapshotV1alpha1().Snapshots(c.namespace).Delete(name, &metav1.DeleteOptions{})
		if err != nil {
			snapshot, err = c.updateSnapshotStatus(snapshot, "Failed", err.Error())
			if err != nil {
				return err
			}
		}
		klog.Infof("snapshot:%s expired - deleted", name)
		// When the snapshot deleted, exit sync handler here.
		return nil
	}

	c.recorder.Event(snapshot, corev1.EventTypeNormal, "Synced", "Snapshot synced successfully")
	return nil
}

func (c *Controller) updateSnapshotStatus(snapshot *cbv1alpha1.Snapshot, phase, reason string) (*cbv1alpha1.Snapshot, error) {
	snapshotCopy := snapshot.DeepCopy()
	snapshotCopy.Status.Phase = phase
	snapshotCopy.Status.Reason = reason
	klog.Infof("snapshot:%s status %s => %s : %s", snapshot.ObjectMeta.Name, snapshot.Status.Phase, phase, reason)
	snapshot, err := c.cbclientset.ClustersnapshotV1alpha1().Snapshots(snapshot.Namespace).Update(snapshotCopy)
	if err != nil {
		return snapshot, fmt.Errorf("Failed to update snapshot status for %s : %s", snapshot.ObjectMeta.Name, err.Error())
	}
	return snapshot, err
}

// enqueueSnapshot takes a Snapshot resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Snapshot.
func (c *Controller) enqueueSnapshot(obj interface{}) {
	var key string
	var err error
	//klog.Info("snapshot enqueued : %#v", obj)

	// queue only snapshots in our namespace
	meta, err := meta.Accessor(obj)
	if err != nil {
		runtime.HandleError(fmt.Errorf("object has no meta: %v", err))
		return
	}
	if meta.GetNamespace() != c.namespace {
		return
	}

	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.snapshotQueue.AddRateLimited(key)
}

// Delete snapshot files on objectstore when Snapshot resource deleted
func (c *Controller) deleteSnapshot(obj interface{}) {

	// convert object into Snapshot and get info for deleting
	snapshot, ok := obj.(*cbv1alpha1.Snapshot)
	if !ok {
		klog.Warningf("Delete snapshot: Invalid object passed: %#v", obj)
		return
	}

	// delete only snapshots in our namespace
	if snapshot.ObjectMeta.GetNamespace() != c.namespace {
		return
	}

	bucket, err := c.getBucket(c.namespace, snapshot.Spec.ObjectstoreConfig, c.kubeclientset, c.cbclientset, c.insecure)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	// Delete snapshot data.
	klog.Infof("Deleting snapshot %s data from objectstore %s", snapshot.ObjectMeta.Name, snapshot.Spec.ObjectstoreConfig)
	err = bucket.Delete(snapshot.ObjectMeta.Name + ".tgz")
	if err != nil {
		runtime.HandleError(err)
	}
}
