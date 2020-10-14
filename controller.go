/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	clientset "github.com/ryo-watanabe/k8s-volume-snap/pkg/client/clientset/versioned"
	ccscheme "github.com/ryo-watanabe/k8s-volume-snap/pkg/client/clientset/versioned/scheme"
	informers "github.com/ryo-watanabe/k8s-volume-snap/pkg/client/informers/externalversions/volumesnapshot/v1alpha1"
	listers "github.com/ryo-watanabe/k8s-volume-snap/pkg/client/listers/volumesnapshot/v1alpha1"

	"github.com/ryo-watanabe/k8s-volume-snap/pkg/cluster"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
)

const controllerAgentName = "k8s-volume-snapshot"

// Controller is the controller implementation for Snapshot and Restore resources
type Controller struct {
	kubeclientset kubernetes.Interface
	vsclientset   clientset.Interface

	snapshotLister  listers.VolumeSnapshotLister
	snapshotsSynced cache.InformerSynced
	restoreLister   listers.VolumeRestoreLister
	restoresSynced  cache.InformerSynced

	snapshotQueue workqueue.RateLimitingInterface
	restoreQueue  workqueue.RateLimitingInterface
	recorder      record.EventRecorder

	insecure         bool
	createbucket     bool

	maxretryelapsedsec int

	namespace string
	labels    map[string]string

	clusterCmd cluster.Cluster
	getBucket  func(namespace, objectstoreConfig string, kubeclient kubernetes.Interface, client clientset.Interface, insecure bool) (objectstore.Objectstore, error)
}

// NewController returns a new controller
func NewController(
	kubeclientset kubernetes.Interface,
	vsclientset clientset.Interface,
	snapshotInformer informers.VolumeSnapshotInformer,
	restoreInformer informers.VolumeRestoreInformer,
	namespace string,
	insecure, createbucket bool,
	maxretryelapsedsec int,
	clusterCmd cluster.Cluster) *Controller {
	//bucket *objectstore.Bucket) *Controller {

	// Create event broadcaster
	// Add k8s-snapshot-controller types to the default Kubernetes Scheme so Events can be
	// logged for k8s-snapshot-controller types.
	utilruntime.Must(ccscheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.V(4).Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeclientset:      kubeclientset,
		vsclientset:        vsclientset,
		snapshotLister:     snapshotInformer.Lister(),
		snapshotsSynced:    snapshotInformer.Informer().HasSynced,
		restoreLister:      restoreInformer.Lister(),
		restoresSynced:     restoreInformer.Informer().HasSynced,
		snapshotQueue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Snapshots"),
		restoreQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Restores"),
		recorder:           recorder,
		insecure:           insecure,
		createbucket:       createbucket,
		maxretryelapsedsec: maxretryelapsedsec,
		namespace:          namespace,
		labels: map[string]string{
			"app":        "k8s-snap",
			"controller": "k8s-snap-controller",
		},
		clusterCmd: clusterCmd,
		getBucket:  getBucketFunc,
	}

	klog.Info("Setting up event handlers")

	// Set up an event handler for when Snapshot resources change
	snapshotInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueSnapshot,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueSnapshot(new)
		},
		DeleteFunc: controller.deleteSnapshot,
	})

	// Set up an event handler for when Restore resources change
	restoreInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueRestore,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueRestore(new)
		},
		//DeleteFunc: controller.enqueueRestore,
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(snapshotthreads, restorethreads int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.snapshotQueue.ShutDown()
	defer c.restoreQueue.ShutDown()

	klog.Info("Checking namespace")
	_, err := c.kubeclientset.CoreV1().Namespaces().Get(c.namespace, metav1.GetOptions{})
	if err != nil {
		klog.Fatalf("Namespace %s not exist", c.namespace)
	}

	// TO DO : Check CRDs are existing here.
	//klog.Info("Checking CRDs")

	klog.Info("Checking objectstore buckets")
	osConfigs, err := c.vsclientset.VolumesnapshotV1alpha1().ObjectstoreConfigs(c.namespace).List(metav1.ListOptions{})
	if err != nil {
		klog.Fatalf("List Objectstore Config error : %s", err.Error())
	}
	for _, os := range osConfigs.Items {

		bucket, err := c.getBucket(c.namespace, os.ObjectMeta.Name, c.kubeclientset, c.vsclientset, c.insecure)
		if err != nil {
			klog.Fatalf("Get bucket error for ObjectstoreConfig %s * %s", os.ObjectMeta.Name, err.Error())
		}
		klog.Infof("- Objectstore Config name:%s endpoint:%s bucket:%s", bucket.GetName(), bucket.GetEndpoint(), bucket.GetBucketName())

		found, err := bucket.ChkBucket()
		if err != nil {
			klog.Fatalf("Check bucket error : %s", err.Error())
		}
		if !found {
			if c.createbucket {
				klog.Infof("Creating bucket %s", bucket.GetBucketName())
				err = bucket.CreateBucket()
				if err != nil {
					klog.Fatalf("Create bucket error : %s", err.Error())
				}
			} else {
				klog.Fatalf("Bucket %s not found", bucket.GetBucketName())
			}
		}

		objList, err := bucket.ListObjectInfo()
		if err != nil {
			klog.Fatalf("List objects error : %s", err.Error())
		}
		klog.Infof("-- Objects in bucket %s:", bucket.GetBucketName())
		for _, obj := range objList {
			klog.Infof("--- filename:%s size:%d timestamp:%s", obj.Name, obj.Size, obj.Timestamp)
		}
	}

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting volume snapshot controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.snapshotsSynced, c.restoresSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")

	// Launch two workers to process Proxy resources
	for i := 0; i < snapshotthreads; i++ {
		go wait.Until(c.runSnapshotWorker, time.Second, stopCh)
	}
	go wait.Until(c.runSnapshotQueuer, time.Second, stopCh)
	for i := 0; i < restorethreads; i++ {
		go wait.Until(c.runRestoreWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

func getBucketFunc(namespace, objectstoreConfig string, kubeclient kubernetes.Interface, client clientset.Interface, insecure bool) (objectstore.Objectstore, error) {
	// bucket
	osConfig, err := client.VolumesnapshotV1alpha1().ObjectstoreConfigs(namespace).Get(
		objectstoreConfig, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// cloud credentials secret
	cred, err := kubeclient.CoreV1().Secrets(namespace).Get(
		osConfig.Spec.CloudCredentialSecret, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	bucket := objectstore.NewBucket(osConfig.ObjectMeta.Name, string(cred.Data["accesskey"]),
		string(cred.Data["secretkey"]), osConfig.Spec.RoleArn,
		osConfig.Spec.Endpoint, osConfig.Spec.Region, osConfig.Spec.Bucket, insecure)

	return bucket, nil
}
