package objectstore

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/sts/stsiface"

	"k8s.io/klog"
)

const crPath = "crs/"

// Objectstore interfaces
type Objectstore interface {
	ChkBucket() (bool, error)
	CreateBucket() error
	CreateAssumeRole(clusterId string, durationSeconds int64) (*sts.Credentials, error)

	Upload(file *os.File, filename string) error
	Download(file *os.File, filename string) error
	Delete(filename string) error
	GetObjectInfo(filename string) (*ObjectInfo, error)
	ListObjectInfo() ([]ObjectInfo, error)

	GetName() string
	GetEndpoint() string
	GetBucketName() string
}

// Bucket for connection to a bucket in object store
type Bucket struct {
	Name              string
	AccessKey         string
	SecretKey         string
	RoleArn           string
	Endpoint          string
	Region            string
	BucketName        string
	insecure          bool
	newS3func         func(*session.Session) s3iface.S3API
	newSTSfunc        func(*session.Session) stsiface.STSAPI
	newUploaderfunc   func(*session.Session) s3manageriface.UploaderAPI
	newDownloaderfunc func(*session.Session) s3manageriface.DownloaderAPI
}

// GetName returns bucket's Name
func (b *Bucket) GetName() string {
	return b.Name
}

// GetEndpoint returns bucket's Endpoint
func (b *Bucket) GetEndpoint() string {
	return b.Endpoint
}

// GetBucketName returns bucket's BacketName
func (b *Bucket) GetBucketName() string {
	return b.BucketName
}

// NewBucket returns new Bucket
func NewBucket(name, accessKey, secretKey, roleArn, endpoint, region, bucketName string, insecure bool) *Bucket {
	return &Bucket{
		Name:              name,
		AccessKey:         accessKey,
		SecretKey:         secretKey,
		RoleArn:           roleArn,
		Endpoint:          endpoint,
		Region:            region,
		BucketName:        bucketName,
		insecure:          insecure,
		newS3func:         newS3,
		newSTSfunc:        newSTS,
		newUploaderfunc:   newUploader,
		newDownloaderfunc: newDownloader,
	}
}

func newS3(sess *session.Session) s3iface.S3API {
	return s3.New(sess)
}

func newSTS(sess *session.Session) stsiface.STSAPI {
	return sts.New(sess)
}

func newUploader(sess *session.Session) s3manageriface.UploaderAPI {
	return s3manager.NewUploader(sess)
}

func newDownloader(sess *session.Session) s3manageriface.DownloaderAPI {
	return s3manager.NewDownloader(sess)
}

func (b *Bucket) setSession() (*session.Session, error) {
	creds := credentials.NewStaticCredentials(b.AccessKey, b.SecretKey, "")
	var client *http.Client
	if b.insecure {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client = &http.Client{Transport: tr}
	} else {
		client = http.DefaultClient
	}
	sess, err := session.NewSession(&aws.Config{
		HTTPClient:  client,
		Credentials: creds,
		Region:      aws.String(b.Region),
		Endpoint:    &b.Endpoint,
	})
	if err != nil {
		return nil, err
	}
	return sess, nil
}

// ChkBucket checks the bucket exists
func (b *Bucket) ChkBucket() (bool, error) {
	// set session
	sess, err := b.setSession()
	if err != nil {
		return false, err
	}

	// list buckets
	svc := b.newS3func(sess)
	result, err := svc.ListBuckets(nil)
	if err != nil {
		return false, err
	}
	klog.Info("Buckets:")
	found := false
	for _, bu := range result.Buckets {
		klog.Infof("-- %s created on %s\n", aws.StringValue(bu.Name), aws.TimeValue(bu.CreationDate))
		if aws.StringValue(bu.Name) == b.BucketName {
			found = true
		}
	}

	return found, nil
}

// CreateBucket creates a bucket
func (b *Bucket) CreateBucket() error {
	// set session
	sess, err := b.setSession()
	if err != nil {
		return err
	}
	// Create bucket
	svc := b.newS3func(sess)
	_, err = svc.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(b.BucketName)})
	return err
}

const policyString = `{
	"Version":"2012-10-17",
	"Statement":[
		{
			"Action":[
				"s3:GetBucketLocation",
				"s3:ListBucket"
			],
			"Effect":"Allow",
			"Resource":[
				"arn:aws:s3:::PATH_FOR_CLUSTER_ID"
			],
			"Sid":"AllowStatement1"
		},
		{
			"Action":[
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject"
			],
			"Effect":"Allow",
			"Resource":[
				"arn:aws:s3:::PATH_FOR_CLUSTER_ID/*"
			],
			"Sid":"AllowStatement2"
		}
	]
}`

// CreateAssumeRole creates temporaly credentials for a path(=clusterId) in the bucket
func (b *Bucket) CreateAssumeRole(clusterId string, durationSeconds int64) (*sts.Credentials, error) {
	// set session
	sess, err := b.setSession()
	if err != nil {
		return nil, err
	}
	// create assume role
	svc := b.newSTS(sess)
	policy := strings.Trim(strings.ReplaceAll(policyString, "PATH_FOR_CLUSTER_ID", clusterId), "\n\t")
	klog.Infof("Creating AssumeRole policy:%s", policy)
	input := &sts.AssumeRoleInput{
		DurationSeconds: &durationSeconds,
		Policy:          aws.String(policy),
		RoleArn:         aws.String(b.roleArn),              // required
		RoleSessionName: aws.String("K8sVolumeSnapSession"), // required
	}
	output, err := svc.AssumeRole(input)
	if err != nil {
		return nil, err
	}
	return output.Credentials, nil
}

// Upload a file to crPath of the bucket
func (b *Bucket) Upload(file *os.File, filename string) error {
	// set session
	sess, err := b.setSession()
	if err != nil {
		return err
	}

	uploader := b.newUploaderfunc(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(b.BucketName),
		Key:    aws.String(crPath + filename),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("Error uploading %s to bucket %s : %s", filename, b.BucketName, err.Error())
	}

	return nil
}

// Download a file from crPath of the bucket
func (b *Bucket) Download(file *os.File, filename string) error {
	// set session
	sess, err := b.setSession()
	if err != nil {
		return err
	}

	downloader := b.newDownloaderfunc(sess)
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(b.BucketName),
			Key:    aws.String(crPath + filename),
		})
	if err != nil {
		return fmt.Errorf("Error downloading %s from bucket %s : %s", filename, b.BucketName, err.Error())
	}

	return nil
}

// Delete a file in crPath of the bucket
func (b *Bucket) Delete(filename string) error {
	// set session
	sess, err := b.setSession()
	if err != nil {
		return err
	}

	svc := b.newS3func(sess)
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(b.BucketName),
		Key:    aws.String(crPath + filename),
	})
	if err != nil {
		return fmt.Errorf("Error deleting %s from bucket %s : %s", filename, b.BucketName, err.Error())
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(b.BucketName),
		Key:    aws.String(crPath + filename),
	})
	return err
}

// GetObjectInfo gets info of a file in the bucket
func (b *Bucket) GetObjectInfo(filename string) (*ObjectInfo, error) {
	// set session
	sess, err := b.setSession()
	if err != nil {
		return nil, err
	}

	// list objects
	svc := b.newS3func(sess)
	result, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(b.BucketName),
		Prefix: aws.String(crPath + filename),
	})
	if err != nil {
		return nil, err
	}

	// find in list
	for _, obj := range result.Contents {
		if aws.StringValue(obj.Key) == filename {
			objInfo := ObjectInfo{
				Name:             filename,
				Size:             aws.Int64Value(obj.Size),
				Timestamp:        aws.TimeValue(obj.LastModified),
				BucketConfigName: b.Name,
			}
			return &objInfo, nil
		}
	}

	return nil, fmt.Errorf("Object %s not found in bucket %s", filename, b.BucketName)
}

// ListObjectInfo lists object info in crPath
func (b *Bucket) ListObjectInfo() ([]ObjectInfo, error) {
	// set session
	sess, err := b.setSession()
	if err != nil {
		return nil, err
	}

	// list objects
	svc := b.newS3func(sess)
	result, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(b.BucketName),
		Prefix: aws.String(crPath),
	})
	if err != nil {
		return nil, err
	}

	// make ObjectInfo list
	objInfoList := make([]ObjectInfo, 0)
	for _, obj := range result.Contents {
		objInfo := ObjectInfo{
			Name:             strings.TrimPrefix(aws.StringValue(obj.Key), crPath),
			Size:             aws.Int64Value(obj.Size),
			Timestamp:        aws.TimeValue(obj.LastModified),
			BucketConfigName: b.Name,
		}
		objInfoList = append(objInfoList, objInfo)
	}

	return objInfoList, nil
}
