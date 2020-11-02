# k8s-volume-snap
Snapshot k8s persistent volumes from outside of the cluster.

## Features
- Snapshot contents of k8s PVs from outside of cluster
- Backup files stored in restic repositories on s3 compatible objectstore
- Restore PV/PVCs and their contents on new/same clusters
- Create restic repositories for each cluster and files are incrementally backuped par PV
- Manipulate AssumeRole tokens for each cluster's backup/restore jobs

### ToDo
- Delete snapshots with custom resource VolumeSnapshot deletion (done 2020/11/2)
- Take snapshots from PVs mounted on nodes with some taints (done 2020/11/2)
- Delete repository when all the snapshots of a cluster deleted
- Update client-go to v1.19 (currently v1.15)
- Take snapshots from PVs not mounted on any node

## Options
````
$ /k8s-volume-snap \
--namespace=k8s-volume-snap
````
|param|default| | |
|----|----|----|----|
|kubeconfig| |Path to a kubeconfig. Only required if out-of-cluster|Optional|
|master| |The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.|Optional|
|namespace|volume-snapshot|Namespace for snapshot|Optional|
|snapshotthreads|5|Number of snapshot threads|Optional|
|restorethreads|2|Number of restore threads|Optional|
|insecure|false|Skip ssl certificate verification on connecting object store|Optional|
|createbucket|false|Create bucket on controller startup if not exist|Optional|
|maxretryelaspsedminutes|60|Max elaspsed minutes to retry snapshot|Optional|

## Deploy controller
````
$ kubectl apply -f artifacts/crd.yaml
$ kubectl apply -f artifacts/namespace-rbac.yaml
````
Set objectstore's access/secret key in artifacts/cloud-credential.yaml and create a secret.
````
apiVersion: v1
kind: Secret
metadata:
  namespace: volume-snapshot
  name: k8s-volume-snap-ap-northeast-1
data:
  accesskey: [base64 access_key]
  secretkey: [base64 secret_key]

$ kubectl apply -f artifacts/cloud-credential.yaml
````
Make a bucket for volume snapshots.  
Set credential secret and bucket name in artifacts/objectstore-config.yaml and create a config.
````
apiVersion: volumesnapshot.rywt.io/v1alpha1
kind: ObjectstoreConfig
metadata:
  name: k8s-snap-ap-northeast-1
  namespace: volume-snapshot
spec:
  region: ap-northeast-1
  bucket: k8s-volume-snap
  cloudCredentialSecret: k8s-volume-snap-ap-northeast-1
  roleArn: arn:aws:iam::123456789012:role/example-role # any string

$ kubectl apply -f artifacts/objectstore-config.yaml
````
Set image in artifacts/deploy.yaml and deploy.
````
$ kubectl apply -f artifacts/deploy.yaml
````
## Taking a volume snapshot
### Create a VolumeSnapshot resource
````
apiVersion: volumesnapshot.rywt.io/v1alpha1
kind: VolumeSnapshot
metadata:
  name: cluster01-001
  namespace: k8s-volume-snap
spec:
  clusterName: cluster01
  objectstoreConfig: k8s-snap-ap-northeast-1
  kubeconfig: |
    apiVersion: v1
    clusters:
    - cluster:
        certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUN3akND...
        server: https://cluster01.kubernetes.111.111.111.111.nip.io:6443
      name: cluster
    contexts:
    - context:
        cluster: cluster
        user: remote-user
      name: context
    current-context: context
    kind: Config
    preferences: {}
    users:
    - name: remote-user
      user:
        token: eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVz....
````

### VolumeSnapshot status
````
$ kubectl get volumesnapshots.volumesnapshot.rywt.io -n volume-snapshot
NAME            CLUSTER     TIMESTAMP              VOLUMES   READY   SKIP   FILES   SIZE   OBJECTSTORE               STATUS      AGE
cluster00-001   cluster00   2020-10-25T01:10:13Z   3         3       2      3       315    k8s-snap-ap-northeast-1   Completed   2d
cluster00-002   cluster00   2020-10-28T01:15:24Z   3         3       2      12      11025  k8s-snap-ap-northeast-1   Completed   3d

````
|phase|status|
|----|----|
|""(empty string)|Just created|
|InQueue|Waiting for proccess|
|InProgress|Taking volume snapshot|
|Failed|Error ocuered in taking volume snapshot|
|Completed|Volume snapshot done and available for restore|
|DeleteFailed|Failed in deleting volume snapshot and associating data|

#### Completed VolumeSnapshot example
````
apiVersion: volumesnapshot.rywt.io/v1alpha1
kind: VolumeSnapshot
metadata:
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
:
  creationTimestamp: "2020-10-13T12:33:09Z"
  generation: 4
  name: cluster00-001
  namespace: k8s-volume-snap
  resourceVersion: "85196160"
  selfLink: /apis/volumesnapshot.rywt.io/v1alpha1/namespaces/volume-snapshot/volumesnapshots/cluster00-001
  uid: 541d179e-50ce-4d16-b3b6-fabb9d4b2ab0
spec:
  clusterId: 88047a23-0255-4432-a40b-94310e4fc7e8
  clusterName: cluster00
  includeNamespaces: null
  kubeconfig: |
:
  objectstoreConfig: k8s-snap-ap-northeast-1
  volumeClaims:
  - claimSpec:
      accessModes:
      - ReadWriteMany
      resources:
        requests:
          storage: 5Gi
      storageClassName: csi-cloud-nas-shrd
      volumeMode: Filesystem
      volumeName: pvc-e4055f99-bec9-45ac-bb7b-0442978bc4f9
    name: deploy-nginx-contents
    namespace: deploy-nginx
    snapshotId: cd929221
    snapshotReady: true
    snapshotSize: 42
    snapshotStartTime: "2020-10-13T12:34:27Z"
  - claimSpec:
      accessModes:
      - ReadWriteMany
      resources:
        requests:
          storage: 5Gi
      storageClassName: csi-cloud-nas-shrd
      volumeMode: Filesystem
      volumeName: pvc-c7c81a7b-e397-4fc9-9cae-3d599fee04a2
    name: contents-stateful-nginx-0
    namespace: stateful-nginx
    snapshotId: 9e3d17a9
    snapshotReady: true
    snapshotSize: 39
    snapshotStartTime: "2020-10-13T12:35:21Z"
  - claimSpec:
      accessModes:
      - ReadWriteMany
      resources:
        requests:
          storage: 5Gi
      storageClassName: csi-cloud-nas-shrd
      volumeMode: Filesystem
      volumeName: pvc-e25971f7-efd6-4ac3-ac67-09e8825a138e
    name: contents-stateful-nginx-1
    namespace: stateful-nginx
    snapshotId: d55b4d96
    snapshotReady: true
    snapshotSize: 39
    snapshotStartTime: "2020-10-13T12:36:00Z"
status:
  numVolumeClaims: 3
  phase: Completed
  readyVolumeClaims: 3
  reason: ""
  skippedClaims: 2
  snapshotEndTime: "2020-10-28T01:15:24Z"
  snapshotStartTime: "2020-10-28T01:13:45Z"
  totalBytes: 315
  totalFiles: 3
````
#### Failed VolumeSnapshot example
````
TODO
````

## Restoring a volume snapshot
### Create a VolumeRestore resource
````
apiVersion: volumesnapshot.rywt.io/v1alpha1
kind: VolumeRestore
metadata:
  name: cluster02-cluster01-001-001
  namespace: k8s-volume-snap
spec:
  clusterName: cluster02
  volumeSnapshotName: cluster01-001
  kubeconfig: |
    apiVersion: v1
    clusters:
    - cluster:
        certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUN3akND...
        server: https://cluster01.kubernetes.111.111.111.111.nip.io:6443
      name: cluster
    contexts:
    - context:
        cluster: cluster
        user: remote-user
      name: context
    current-context: context
    kind: Config
    preferences: {}
    users:
    - name: remote-user
      user:
        token: eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9.eyJpc3MiOiJrdWJlcm5ldGVz....
````
### Restore status
````
$ kubectl get volumerestores.volumesnapshot.rywt.io -n volume-snapshot
NAME                          CLUSTER     SNAPSHOT        TIMESTAMP              VOLUMECLAIMS   FAILED   STATUS
cluster01-cluster00-002-001   cluster01   cluster00-002   2020-10-14T02:49:35Z   3                       Completed
````
|phase|status|
|----|----|
|""(empty string)|Just created|
|InQueue|Waiting for proccess|
|InProgress|Doing volume restore|
|Failed|Error ocuered in volume restore|
|Completed|Volume restore done|

#### Completed VolumeRestore example
````
apiVersion: volumesnapshot.rywt.io/v1alpha1
kind: VolumeRestore
metadata:
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: |
:
  creationTimestamp: "2020-10-14T02:47:17Z"
  generation: 4
  name: cluster01-cluster00-002-001
  namespace: k8s-volume-snap
  resourceVersion: "85367044"
  selfLink: /apis/volumesnapshot.rywt.io/v1alpha1/namespaces/volume-snapshot/volumerestores/cluster01-cluster00-002-001
  uid: 68d88490-c277-4b6b-9191-8d058357bc65
spec:
  clusterName: cluster01
  kubeconfig: |
:
  restoreNamespaces: null
  strictVolumeClass: false
  volumeSnapshotName: cluster00-002
status:
  failed: null
  numFailed: 0
  numVolumeClaims: 3
  phase: Completed
  reason: ""
  restoreTimestamp: "2020-10-14T02:49:35Z"

````
#### Failed VolumeRestore example
````
TODO
````
## Deleting snapshot

You can delete a snapshot manually with:
````
$ kubectl delete volumesnapshots.volumesnapshot.rywt.io -n volume-snapshot cluster01-001
````
and also the corresponding snapshots on the repository automatically deleted.