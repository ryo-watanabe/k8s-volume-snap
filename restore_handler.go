package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
)

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runRestoreWorker() {
	for c.processNextRestoreItem(false) {
	}
}
//func (c *Controller) runRestoreQueuer() {
//	for c.processNextRestoreItem(true) {
//	}
//}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextRestoreItem(queueonly bool) bool {
	// Process restore queue
	obj, shutdown := c.restoreQueue.Get()
	if shutdown {
		return false
	}
	err := func(obj interface{}) error {
		defer c.restoreQueue.Done(obj)
		var key string
		var ok bool
		if key, ok = obj.(string); !ok {
			c.restoreQueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		if err := c.restoreSyncHandler(key, queueonly); err != nil {
			c.restoreQueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.restoreQueue.Forget(obj)
		klog.V(4).Infof("Successfully synced '%s'", key)
		return nil
	}(obj)
	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// snapshotSyncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Snapshot resource
// with the current status of the resource.
func (c *Controller) restoreSyncHandler(key string, queueonly bool) error {

	ctx := context.TODO()

	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Restore resource with this namespace/name.
	restore, err := c.restoreLister.VolumeRestores(namespace).Get(name)

	// if deleted.
	if err != nil {
		if errors.IsNotFound(err) {
			// if deleted ok, exit sync handler here.
			return nil
		}
		return err
	}

	if !queueonly && restore.Status.Phase == "InQueue" {
		restore, err = c.updateRestoreStatus(ctx, restore, "InProgress", "")
		if err != nil {
			return err
		}

		// snapshot
		snapshot, err := c.vsclientset.VolumesnapshotV1alpha1().VolumeSnapshots(c.namespace).Get(
			ctx, restore.Spec.VolumeSnapshotName, metav1.GetOptions{})
		if err != nil {
			restore, err = c.updateRestoreStatus(ctx, restore, "Failed", err.Error())
			if err != nil {
				return err
			}
			return nil
		}
		if snapshot.Status.Phase != "Completed" {
			restore, err = c.updateRestoreStatus(ctx, restore, "Failed", "Snapshot data is not status 'Completed'")
			if err != nil {
				return err
			}
			return nil
		}

		// bucket
		bucket, err := c.getBucket(c.namespace, snapshot.Spec.ObjectstoreConfig, c.kubeclientset, c.vsclientset, c.insecure)
		if err != nil {
			restore, err = c.updateRestoreStatus(ctx, restore, "Failed", err.Error())
			if err != nil {
				return err
			}
			return nil
		}
		klog.Infof("- Objectstore Config name:%s endpoint:%s bucket:%s", bucket.GetName(), bucket.GetEndpoint(), bucket.GetBucketName())

		// do restore
		err = c.clusterCmd.Restore(restore, snapshot, bucket, c.kubeclientset)
		if err != nil {
			restore, err = c.updateRestoreStatus(ctx, restore, "Failed", err.Error())
			if err != nil {
				return err
			}
			return nil
		}

		restore, err = c.updateRestoreStatus(ctx, restore, "Completed", "")
		if err != nil {
			return err
		}
	}

	// initialize
	if restore.Status.Phase == "" {
		restore, err = c.updateRestoreStatus(ctx, restore, "InQueue", "")
		if err != nil {
			return err
		}
	}

	c.recorder.Event(restore, corev1.EventTypeNormal, "Synced", "Restore synced successfully")
	return nil
}

func (c *Controller) updateRestoreStatus(ctx context.Context, restore *vsv1alpha1.VolumeRestore, phase, reason string) (*vsv1alpha1.VolumeRestore, error) {
	restoreCopy := restore.DeepCopy()
	restoreCopy.Status.Phase = phase
	restoreCopy.Status.Reason = reason
	klog.Infof("restore:%s status %s => %s : %s", restore.ObjectMeta.Name, restore.Status.Phase, phase, reason)
	restore, err := c.vsclientset.VolumesnapshotV1alpha1().VolumeRestores(restore.Namespace).Update(ctx, restoreCopy, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("Failed to update restore status for " + restore.ObjectMeta.Name + " : " + err.Error())
	}
	return restore, err
}

// enqueueRestore takes a Restore resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Restore.
func (c *Controller) enqueueRestore(obj interface{}) {
	// queue only restores in our namespace
	meta, err := meta.Accessor(obj)
	if err != nil {
		runtime.HandleError(fmt.Errorf("object has no meta: %v", err))
		return
	}
	if meta.GetNamespace() != c.namespace {
		return
	}

	key, _ := cache.MetaNamespaceKeyFunc(obj)
	c.restoreQueue.AddRateLimited(key)
}
