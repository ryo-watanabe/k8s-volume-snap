package cluster

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	vsv1alpha1 "github.com/ryo-watanabe/k8s-volume-snap/pkg/apis/volumesnapshot/v1alpha1"
	"github.com/ryo-watanabe/k8s-volume-snap/pkg/objectstore"
)

// Cluster interfaces for taking and restoring snapshot of k8s clusters
type Cluster interface {
	Snapshot(snapshot *vsv1alpha1.VolumeSnapshot,
		bucket objectstore.Objectstore,
		localKubeClient kubernetes.Interface) error
	Restore(restore *vsv1alpha1.VolumeRestore,
		snapshot *vsv1alpha1.VolumeSnapshot,
		bucket objectstore.Objectstore,
		localKubeClient kubernetes.Interface) error
}

// Cmd for execute cluster commands
type Cmd struct {
}

// NewClusterCmd returns new Cmd
func NewClusterCmd() *Cmd {
	return &Cmd{}
}

// Snapshot takes a volume snapshot
func (c *Cmd) Snapshot(
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {

	return Snapshot(snapshot, bucket, localKubeClient)
}

// Restore restores volumes form a snapshot
func (c *Cmd) Restore(
	restore *vsv1alpha1.VolumeRestore,
	snapshot *vsv1alpha1.VolumeSnapshot,
	bucket objectstore.Objectstore,
	localKubeClient kubernetes.Interface) error {

	return Restore(restore, snapshot, bucket, localKubeClient)
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
func getNamespaceUID(name string, kubeClient *kubernetes.Clientset) (string, error) {
	ns, err := kubeClient.CoreV1().Namespaces().Get(name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("Error getting namespace UID : %s", err.Error())
	}
	return string(ns.ObjectMeta.GetUID()), nil
}

/*
// ConfigMapMarker creates and deletes a config map to get a marker for Resource Version
func ConfigMapMarker(kubeClient kubernetes.Interface, name string) (*corev1.ConfigMap, error) {
	configMap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
	}
	configMap, err := kubeClient.CoreV1().ConfigMaps("default").Create(configMap)
	if err != nil {
		return nil, err
	}
	err = kubeClient.CoreV1().ConfigMaps("default").Delete(name, &metav1.DeleteOptions{})
	if err != nil {
		return nil, err
	}
	return configMap, nil
}
*/