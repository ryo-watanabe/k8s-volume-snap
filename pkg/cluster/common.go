package cluster

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
)

// Interface for taking and restoring snapshots of k8s clusters
type Interface interface {
	Snapshot(snapshot *vsv1alpha1.VolumeSnapshot,
		bucket objectstore.Objectstore,
		localKubeClient kubernetes.Interface) error
	Restore(restore *vsv1alpha1.VolumeRestore,
		snapshot *vsv1alpha1.VolumeSnapshot,
		bucket objectstore.Objectstore,
		localKubeClient kubernetes.Interface) error
	DeleteSnapshot(snapshot *vsv1alpha1.VolumeSnapshot,
		bucket objectstore.Objectstore,
		localKubeClient kubernetes.Interface) error
}

// Cluster for execute cluster commands
type Cluster struct {
	Interface
}

// NewCluster returns new Cmd
func NewCluster() *Cluster {
	return &Cluster{}
}

// Setup Kubernetes client for target cluster.
func buildKubeClient(kubeconfig string) (*kubernetes.Clientset, error) {
	// Check if Kubeconfig available.
	if kubeconfig == "" {
		return nil, fmt.Errorf("Cannot create Kubeconfig : Kubeconfig not given")
	}

	// Setup Rancher Kubeconfig to access customer cluster.
	cfg, err := clientcmd.RESTConfigFromKubeConfig([]byte(kubeconfig))
	if err != nil {
		return nil, fmt.Errorf("Error building kubeconfig: %s", err.Error())
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("Error building kubernetes clientset: %s", err.Error())
	}
	return kubeClient, err
}

// Get namespace UID
func getNamespaceUID(ctx context.Context, name string, kubeClient kubernetes.Interface) (string, error) {
	ns, err := kubeClient.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("Error getting namespace UID : %s", err.Error())
	}
	return string(ns.ObjectMeta.GetUID()), nil
}

// k8s api errors not to retry
var apiPermErrors = []string{
	"Unauthorized",
	"the server has asked for the client to provide credentials",
}

func apiPermError(error string) bool {
	for _, e := range apiPermErrors {
		if strings.Contains(error, e) {
			return true
		}
	}
	return false
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
