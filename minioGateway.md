# Deploy minio gateway rooming k8s apiserver's etcd

### Copy etcd tls certs from master server

Copy server.crt, server.key and ca.crt from master server's /etc/kubernetes/pki/etcd/
```
# ls -l /etc/kubernetes/pki/etcd/
total 32
-rw-r--r-- 1 root root 1058 Nov 12 07:07 ca.crt  <--
-rw-r--r-- 1 root root 1679 Nov 12 07:07 ca.key
-rw-r--r-- 1 root root 1139 Nov 12 07:07 healthcheck-client.crt
-rw-r--r-- 1 root root 1675 Nov 12 07:07 healthcheck-client.key
-rw-r--r-- 1 root root 1172 Nov 12 07:07 peer.crt
-rw-r--r-- 1 root root 1679 Nov 12 07:07 peer.key
-rw-r--r-- 1 root root 1172 Nov 12 07:07 server.crt  <--
-rw-r--r-- 1 root root 1675 Nov 12 07:07 server.key  <--
```

### Create secrets
```
# kubectl create secret tls etcd-certs --cert server.crt --key server.key -n minio
# kubectl create secret generic etcd-ca -n minio --from-file=ca.crt
```

### Minio deployment

Deploy minio with etcd certs and endpoint of the master server
```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - args:
        - gateway
        - s3
        - https://jp-east-2.storage.api.nifcloud.com
        env:
        - name: MINIO_ACCESS_KEY
          value: minio
        - name: MINIO_SECRET_KEY
          value: minio123
        - name: MINIO_DOMAIN
          value: minio.[LB's IP].nip.io
        - name: MINIO_PUBLIC_IPS
          value: 10.96.0.100
        - name: AWS_ACCESS_KEY_ID
          value: [accesskey]
        - name: AWS_SECRET_ACCESS_KEY
          value: [secretkey]
        - name: MINIO_ETCD_ENDPOINTS
          value: https://[master server's IP]:2379
        - name: MINIO_ETCD_CLIENT_CERT
          value: /certs/tls.crt
        - name: MINIO_ETCD_CLIENT_CERT_KEY
          value: /certs/tls.key
        image: minio/minio
        imagePullPolicy: Always
        name: minio
        ports:
        - containerPort: 9000
          protocol: TCP
        volumeMounts:
        - name: certs
          mountPath: /certs
        - name: ca
          mountPath: /etc/ssl/certs/ca.crt
          subPath: ca.crt
      volumes:
      - name: certs
        secret:
          secretName: etcd-certs
      - name: ca
        secret:
          secretName: etcd-ca
```
### Appendix: etcdctl pod
```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: etcdctl
  namespace: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: etcdctl
  template:
    metadata:
      labels:
        app: etcdctl
    spec:
      containers:
      - image: alpine
        imagePullPolicy: IfNotPresent
        name: etcdctl
        command:
        - "sh"
        - "-c"
        args:
        - "apk add etcd-ctl --repository=http://dl-cdn.alpinelinux.org/alpine/edge/testing && tail -f /dev/null"
        env:
        - name: ETCDCTL_API
          value: "3"
        - name: ETCDCTL_ENDPOINTS
          value: https://[master server's IP]:2379
        - name: ETCDCTL_CERT
          value: /certs/tls.crt
        - name: ETCDCTL_KEY
          value: /certs/tls.key
        volumeMounts:
        - name: certs
          mountPath: /certs
        - name: ca
          mountPath: /etc/ssl/certs
      volumes:
      - name: certs
        secret:
          secretName: etcd-certs
      - name: ca
        secret:
          secretName: etcd-ca
```

# Create user for volume-snapshot

### Create user with mc command
````
$ mc admin user add local [newuser accesskey] [newuser secretkey]
Added user `[newuser accesskey]` successfully.

$ mc admin policy add local allowall allow-all-policy.json
Added policy `allowall` successfully.

$ mc admin policy set local allowall user=[newuser accesskey]
Policy allowall is set on user `[newuser accesskey]`
````
### allow-all-policy.json
````
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": ["s3:*"],
      "Effect": "Allow",
      "Resource": ["arn:aws:s3:::*"],
      "Sid": "AllowStatement1"
    }
  ]
}

````
### mc command configuration ~/.mc/config.json
````
{
        "version": "10",
        "aliases": {
                "local": {
                        "url": "http://minio.[LB's IP].nip.io",
                        "accessKey": "[minio admin accesskey]",
                        "secretKey": "[minio admin secretkey]",
                        "api": "S3v2",
                        "path": "auto"
                }
        }
}
````
### Appendix: mc command pod
```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mc
  namespace: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mc
  template:
    metadata:
      labels:
        app: mc
    spec:
      containers:
      - command: ["tail","-f","/dev/null"]
        image: minio/mc
        imagePullPolicy: IfNotPresent
        name: mc
        volumeMounts:
        - name: config
          mountPath: /root/.mc/config.json
          subPath: config.json
        - name: policy
          mountPath: /allow-all-policy.json
          subPath: allow-all-policy.json
      volumes:
      - name: config
        configMap:
          name: mc-config
      - name: policy
        configMap:
          name: allow-all-policy
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: mc-config
  namespace: minio
data:
  config.json: |
    {
      "version": "10",
      "aliases": {
         "local": {
            "url": "http://minio.[LB's IP].nip.io",
            "accessKey": "[minio admin accesskey]",
            "secretKey": "[minio admin secretkey]",
            "api": "S3v2",
            "path": "auto"
        }
      }
    }
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: allow-all-policy
  namespace: minio
data:
  allow-all-policy.json: |
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Action": ["s3:*"],
          "Effect": "Allow",
          "Resource": ["arn:aws:s3:::*"],
          "Sid": "AllowStatement1"
        }
      ]
    }
```
### Appendix: keys on etcd
- keys for k8s : prefix /registry/.....
- keys for minio : other (/skydns/...., backend-encrypted, etc)
```
# etcdctl get --keys-only --from-key /registry/st
/registry/statefulsets/elasticsearch/elasticsearch
/registry/statefulsets/kafka/confluent-cp-kafka
/registry/statefulsets/kafka/confluent-cp-zookeeper
/registry/storageclasses/managed-nfs-storage
/skydns/io/nip/3/95/64/111/minio/k8s-backup-2nd/111.64.95.3
backend-encrypted
compact_rev_key
config/iam/format.json
config/iam/policies/allowall/policy.json
config/iam/policydb/users/newuseresuwen.json
config/iam/users/newuseresuwen/identity.json
```