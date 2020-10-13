package cluster

import (
	"bufio"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	//"github.com/golang/glog"
	//"github.com/golang/protobuf/ptypes"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog"
)

type ResticSnapshot struct {
	ShortId  string   `json:"short_id"`
	Id       string   `json:"id"`
	Time     string   `json:"time"`
	Paths    []string `json:"paths"`
	Hostname string   `json:"hostname"`
	Username string   `json:"username"`
	Tree     string   `json:"tree"`
	Parent   string   `json:"parent"`
}

func (snap *ResticSnapshot) GetSourceVolumeId() string {
	_, file := filepath.Split(snap.Paths[0])
	return file
}

type ResticBackupSummary struct {
	FilesNew            int64   `json:"files_new"`
	FilesChanges        int64   `json:"files_changed"`
	FilesUnmodified     int64   `json:"files_unmodified"`
	DirsNew             int64   `json:"dirs_new"`
	DirsChanges         int64   `json:"dirs_changed"`
	DirsModified        int64   `json:"dirs_unmodified"`
	DataBlobs           int64   `json:"data_blobs"`
	TreeBlobs           int64   `json:"tree_blobs"`
	DataAdded           int64   `json:"data_added"`
	TotalFilesProcessed int64   `json:"total_files_processed"`
	TotalBytesProcessed int64   `json:"total_bytes_processed"`
	TotalDuration       float64 `json:"total_duration"`
	SnapshotId          string  `json:"snapshot_id"`
}

type Restic struct {
	password     string
	accesskey    string
	secretkey    string
	sessiontoken string
	endpoint     string
	bucket       string
	clusterid    string
	image        string
	namespace    string
	id           string
}

// NewRestic returns new Restic struct
func NewRestic(endpoint, bucket, clusterid, password,
	accesskey, secretkey, sessiontoken, namespace, id string) *Restic {

	return &Restic{
		endpoint:     endpoint,
		bucket:       bucket,
		clusterid:    clusterid,
		password:     password,
		accesskey:    accesskey,
		secretkey:    secretkey,
		sessiontoken: sessiontoken,
		image:        "restic/restic:latest",
		namespace:    namespace,
		id:           id,
	}
}

func (r *Restic)repository() string {
	return "s3:" + r.endpoint + "/" + r.bucket + "/" + r.clusterid
}

func retryNotifyRestic(err error, wait time.Duration) {
	klog.V(4).Infof("%s : will be checked again in %.2f seconds", err.Error(), wait.Seconds())
}

// DoResticJob executes restic Job with backing off
func DoResticJob(job *batchv1.Job, kubeClient kubernetes.Interface, initInterval int) (string, error) {

	name := job.GetName()
	namespace := job.GetNamespace()
	var errBuf error
	errBuf = nil

	// Create job
	var dp metav1.DeletionPropagation = metav1.DeletePropagationForeground
	_, err := kubeClient.BatchV1().Jobs(namespace).Create(job)
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			return "", fmt.Errorf("Creating restic job error - %s", err.Error())
		}
		kubeClient.BatchV1().Jobs(namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &dp})
		_, err = kubeClient.BatchV1().Jobs(namespace).Create(job)
		if err != nil {
			return "", fmt.Errorf("Re-creating restic job error - %s", err.Error())
		}
	}
	defer kubeClient.BatchV1().Jobs(namespace).Delete(name, &metav1.DeleteOptions{PropagationPolicy: &dp})

	// wait for job completed with backoff retry
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = time.Duration(30) * time.Minute
	b.RandomizationFactor = 0.2
	b.Multiplier = 2.0
	b.InitialInterval = time.Duration(initInterval) * time.Second
	chkJobCompleted := func() error {
		chkJob, err := kubeClient.BatchV1().Jobs(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return backoff.Permanent(err)
		}
		if len(chkJob.Status.Conditions) > 0 {
			if chkJob.Status.Conditions[0].Type == "Failed" {
				return backoff.Permanent(fmt.Errorf("Job %s failed", name))
			} else {
				return nil
			}
		}
		return fmt.Errorf("Job %s is running", name)
	}
	err = backoff.RetryNotify(chkJobCompleted, b, retryNotifyRestic)
	if err != nil {
		errBuf = fmt.Errorf("Error doing restic job - %s", err.Error())
	}

	// Get logs
	podList, err := kubeClient.CoreV1().Pods(namespace).List(metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("Listing job pods error - %s", err.Error())
	}
	for _, pod := range podList.Items {
		refs := pod.ObjectMeta.GetOwnerReferences()
		if len(refs) > 0 && refs[0].Name == name {
			req := kubeClient.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{})
			podLogs, err := req.Stream()
			if err != nil {
				return "", fmt.Errorf("Logs request error - %s", err.Error())
			}
			reader := bufio.NewReader(podLogs)
			defer podLogs.Close()
			// return a line which contains 'summary' or a last one
			out := ""
			for {
				line, err := reader.ReadString('\n')
				if line != "" {
					out = line
				}
				if err != nil || strings.Contains(out, "summary") || strings.Contains(out, "Fatal:") {
					break
				}
			}
			if errBuf != nil {
				return "", fmt.Errorf("%s : %s", out, errBuf.Error())
			}
			return out, nil
		}
	}
	return "", fmt.Errorf("Cannot find pod for job %s", name)
}

// restic snapshots
func (r *Restic) resticJobListSnapshots() *batchv1.Job {
	job := r.resticJob("restic-job-list-snapshots-" + r.id, r.namespace)
	job.Spec.Template.Spec.Containers[0].Args = append(
		job.Spec.Template.Spec.Containers[0].Args,
		[]string{"snapshots", "--json"}...,
	)
	return job
}

// restic backup
func (r *Restic) resticJobBackup(volumeId, hostPath, nodeName string) *batchv1.Job {
	job := r.resticJob("restic-job-backup-" + volumeId, "default")
	job.Spec.Template.Spec.Containers[0].Args = append(
		job.Spec.Template.Spec.Containers[0].Args,
		[]string{"backup", "--json", "--tag", r.id, "/" + volumeId}...,
	)
	// add node selector
	job.Spec.Template.Spec.NodeSelector = map[string]string{
		"kubernetes.io/hostname": nodeName,
	}
	// volumes
	hpType := corev1.HostPathDirectory
	volume := corev1.Volume{
		Name: "pv-path",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: hostPath,
				Type: &hpType,
			},
		},
	}
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, volume)
	// volume mounts
	volumeMount := corev1.VolumeMount{
		Name:      "pv-path",
		MountPath: "/" + volumeId,
		ReadOnly:  true,
	}
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		job.Spec.Template.Spec.Containers[0].VolumeMounts,
		volumeMount,
	)
	job.Spec.Template.Spec.Hostname = "restic-backup-job"
	job.Spec.Template.Spec.NodeSelector = map[string]string{
		"kubernetes.io/hostname": nodeName,
	}

	return job
}

// restic forget (delete)
func (r *Restic) resticJobDelete(snapshotId string) *batchv1.Job {
	job := r.resticJob("restic-job-delete-" + snapshotId, r.namespace)
	job.Spec.Template.Spec.Containers[0].Args = append(
		job.Spec.Template.Spec.Containers[0].Args,
		[]string{"forget", "--prune", snapshotId}...,
	)
	return job
}

// restic init
func (r *Restic) resticJobInit() *batchv1.Job {
	job := r.resticJob("restic-job-init-repo-" + r.clusterid, r.namespace)
	job.Spec.Template.Spec.Containers[0].Args = append(
		job.Spec.Template.Spec.Containers[0].Args,
		[]string{"init"}...,
	)
	return job
}

// restic restore
func (r *Restic) resticJobRestore(snapId, volumeId, pvcName, pvcNamespace string) *batchv1.Job {
	// Job
	job := r.resticJob("restic-job-restore-"+snapId, pvcNamespace)
	job.Spec.Template.Spec.Containers[0].Args = append(
		job.Spec.Template.Spec.Containers[0].Args,
		[]string{"restore", "-t", "/", snapId}...,
	)
	// volumes
	volume := corev1.Volume{
		Name: "pvc",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	}
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, volume)
	// volume mounts
	volumeMount := corev1.VolumeMount{
		Name:      "pvc",
		MountPath: "/" + volumeId,
	}
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		job.Spec.Template.Spec.Containers[0].VolumeMounts,
		volumeMount,
	)

	return job
}

// restic job pod
func (r *Restic) resticJob(name, namespace string) *batchv1.Job {

	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  name,
							Image: r.image,
							Env: []corev1.EnvVar{
								{
									Name:  "AWS_ACCESS_KEY_ID",
									Value: r.accesskey,
								},
								{
									Name:  "AWS_SECRET_ACCESS_KEY",
									Value: r.secretkey,
								},
								{
									Name:  "RESTIC_PASSWORD",
									Value: r.password,
								},
								{
									Name:  "RESTIC_REPOSITORY",
									Value: r.repository(),
								},
							},
							ImagePullPolicy: "IfNotPresent",
						},
					},
					RestartPolicy: "Never",
				},
			},
			BackoffLimit: &backoffLimit,
		},
	}

	// Add session token env
	if r.sessiontoken != "" {
		job.Spec.Template.Spec.Containers[0].Env = append(
			job.Spec.Template.Spec.Containers[0].Env,
			corev1.EnvVar{
				Name:  "AWS_SESSION_TOKEN",
				Value: r.sessiontoken,
			},
		)
	}

	return job
}

// pv path globber
func (r *Restic) globberJob(volumeId, nodeName string) *batchv1.Job {

	backoffLimit := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "glob-" + volumeId + "-" + nodeName,
			Namespace: "default",
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "globber",
							Image: "alpine",
							Command: []string{
								"/bin/sh",
								"-c",
								"ls -d /var/lib/kubelet/pods/*/volumes/*/" + volumeId,
							},
							ImagePullPolicy: "IfNotPresent",
						},
					},
					RestartPolicy: "Never",
				},
			},
			BackoffLimit: &backoffLimit,
		},
	}
	// add node selector
	job.Spec.Template.Spec.NodeSelector = map[string]string{
		"kubernetes.io/hostname": nodeName,
	}
	// add mount hostpath /var/lib/kubelet/pods
	hpType := corev1.HostPathDirectory
	volume := corev1.Volume{
		Name: "pod-path",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/var/lib/kubelet/pods",
				Type: &hpType,
			},
		},
	}
	job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, volume)
	volumeMount := corev1.VolumeMount{
		Name:      "pod-path",
		MountPath: "/var/lib/kubelet/pods",
	}
	job.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		job.Spec.Template.Spec.Containers[0].VolumeMounts,
		volumeMount,
	)

	return job
}
