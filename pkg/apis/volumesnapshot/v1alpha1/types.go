package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshot is a specification for a VolumeSnapshot resource
type VolumeSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VolumeSnapshotSpec   `json:"spec"`
	Status VolumeSnapshotStatus `json:"status"`
}

// VolumeSnapshotSpec is the spec for a VolumeSnapshot resource
type VolumeSnapshotSpec struct {
	ClusterName       string        `json:"clusterName"`
	Kubeconfig        string        `json:"kubeconfig"`
	ObjectstoreConfig string        `json:"objectstoreConfig"`
	IncludeNamespaces []string      `json:"includeNamespaces"` // If not set, snapshot all namespaces
	VolumeClaims      []VolumeClaim `json:"volumeClaims"`
	ClusterId         string        `json:"clusterId"` //nolint:golint // = kube-system namespace UID
}

// VolumeClaim keeps the spec of each volume snapshot
type VolumeClaim struct {
	Name          string                           `json:"name"`       // Name of source PVC
	Namespace     string                           `json:"namespace"`  // Namespace of source PVC
	ClaimSpec     corev1.PersistentVolumeClaimSpec `json:"claimSpec"`  // Spec of source PVC
	SnapshotId    string                           `json:"snapshotId"` //nolint:golint
	SnapshotTime  metav1.Time                      `json:"snapshotTime"`
	SnapshotSize  int64                            `json:"snapshotSize"`
	SnapshotFiles int64                            `json:"snapshotFiles"`
	SnapshotReady bool                             `json:"snapshotReady"`
}

// VolumeSnapshotStatus is the status for a Snapshot resource
type VolumeSnapshotStatus struct {
	Phase             string      `json:"phase"`
	Reason            string      `json:"reason"`
	SnapshotStartTime metav1.Time `json:"snapshotStartTime"`
	SnapshotEndTime   metav1.Time `json:"snapshotEndTime"`
	NumVolumeClaims   int32       `json:"numVolumeClaims"`
	SkippedClaims     int32       `json:"skippedClaims"`
	SkippedMessages   []string    `json:"skippedMessages"`
	ReadyVolumeClaims int32       `json:"readyVolumeClaims"`
	TotalBytes        int64       `json:"totalBytes"`
	TotalFiles        int64       `json:"totalFiles"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeRestore is a specification for a Restore resource
type VolumeRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VolumeRestoreSpec   `json:"spec"`
	Status VolumeRestoreStatus `json:"status"`
}

// VolumeRestoreSpec is the spec for a Restore resource
type VolumeRestoreSpec struct {
	ClusterName        string   `json:"clusterName"`
	VolumeSnapshotName string   `json:"volumeSnapshotName"`
	Kubeconfig         string   `json:"kubeconfig"`
	RestoreNamespaces  []string `json:"restoreNamespaces"` // If not set, restore all namespaces in the snapshot
	StrictVolumeClass  bool     `json:"strictVolumeClass"` // Do not restore if original volume class is not available
}

// VolumeRestoreStatus is the status for a Restore resource
type VolumeRestoreStatus struct {
	Phase                 string      `json:"phase"`
	Reason                string      `json:"reason"`
	RestoreTimestamp      metav1.Time `json:"restoreTimestamp"`
	NumVolumeClaims       int32       `json:"numVolumeClaims"`
	FailedVolumeClaims    []string    `json:"failed"`
	NumFailedVolumeClaims int32       `json:"numFailed"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ObjectstoreConfig is a specification for a ObjectstoreConfig resource
type ObjectstoreConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ObjectstoreConfigSpec `json:"spec"`
}

// ObjectstoreConfigSpec is the spec for a ObjectstoreConfig resource
type ObjectstoreConfigSpec struct {
	Region                string `json:"region"`
	Endpoint              string `json:"endpoint"`
	CloudCredentialSecret string `json:"cloudCredentialSecret"`
	Bucket                string `json:"bucket"`
	RoleArn               string `json:"roleArn"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeSnapshotList is a list of Snapshot resources
type VolumeSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []VolumeSnapshot `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeRestoreList is a list of Restore resources
type VolumeRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []VolumeRestore `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ObjectstoreConfigList is a list of ObjectstoreConfig resources
type ObjectstoreConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ObjectstoreConfig `json:"items"`
}
