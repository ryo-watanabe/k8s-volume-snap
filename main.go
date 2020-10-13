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
	"flag"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"

	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	clientset "github.com/ryo-watanabe/k8s-volume-snap/pkg/client/clientset/versioned"
	informers "github.com/ryo-watanabe/k8s-volume-snap/pkg/client/informers/externalversions"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/cluster"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/signals"
)

var (
	masterURL          string
	kubeconfig         string
	namespace          string
	snapshotthreads    int
	restorethreads     int
	insecure           bool
	createbucket       bool
	maxretryelapsedsec int
	version            string
	revision           string
)

func main() {

	flag.Parse()
	klog.Infof("k8s-volume-snap version:%s revision:%s", version, revision)

	// set up signals so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	vsClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building volumesnapshot clientset: %s", err.Error())
	}

	vsInformerFactory := informers.NewSharedInformerFactory(vsClient, time.Second*30)

	controller := NewController(kubeClient, vsClient,
		vsInformerFactory.Volumesnapshot().V1alpha1().VolumeSnapshots(),
		vsInformerFactory.Volumesnapshot().V1alpha1().VolumeRestores(),
		namespace,
		insecure, createbucket,
		maxretryelapsedsec,
		cluster.NewClusterCmd(),
	)

	// notice that there is no need to run Start methods in a separate goroutine. (i.e. go kubeInformerFactory.Start(stopCh)
	// Start method is non-blocking and runs all registered informers in a dedicated goroutine.
	vsInformerFactory.Start(stopCh)

	if err = controller.Run(snapshotthreads, restorethreads, stopCh); err != nil {
		klog.Fatalf("Error running controller: %s", err.Error())
	}
}

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&namespace, "namespace", "k8s-snap", "Namespace for k8s-snap")
	flag.IntVar(&snapshotthreads, "snapshotthreads", 5, "Number of snapshot threads")
	flag.IntVar(&restorethreads, "restorethreads", 2, "Number of restore threads")
	flag.BoolVar(&insecure, "insecure", false, "Skip ssl certificate verification on connecting object store")
	flag.BoolVar(&createbucket, "createbucket", false, "Create bucket if not exists")
	flag.IntVar(&maxretryelapsedsec, "maxretryelapsedsec", 300, "Max elaspsed seconds to retry snapshot")
}
