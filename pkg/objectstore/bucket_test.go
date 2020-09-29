package objectstore

import (
	"flag"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"k8s.io/klog"
)

// NewMockBucket returns new mock Bucket
func NewMockBucket(name, accessKey, secretKey, endpoint, region, bucketName string, insecure bool) *Bucket {
	return &Bucket{
		Name:              name,
		AccessKey:         accessKey,
		SecretKey:         secretKey,
		Endpoint:          endpoint,
		Region:            region,
		BucketName:        bucketName,
		insecure:          insecure,
		newS3func:         newMockS3,
		newUploaderfunc:   newMockUploader,
		newDownloaderfunc: newMockDownloader,
	}
}

// Mock interfaces

type mockS3Client struct {
	s3iface.S3API
}

var listBucketsOutput s3.ListBucketsOutput

func (m mockS3Client) ListBuckets(input *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return &listBucketsOutput, nil
}

var createdBucketName string

func (m mockS3Client) CreateBucket(input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	createdBucketName = *input.Bucket
	return &s3.CreateBucketOutput{}, nil
}

var deleteObjectBucketName string
var deleteObjectKey string

func (m mockS3Client) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	deleteObjectBucketName = *input.Bucket
	deleteObjectKey = *input.Key
	return &s3.DeleteObjectOutput{}, nil
}

var headObjectBucketName string
var headObjectKey string

func (m mockS3Client) WaitUntilObjectNotExists(input *s3.HeadObjectInput) error {
	headObjectBucketName = *input.Bucket
	headObjectKey = *input.Key
	return nil
}

var listObjectsOutput s3.ListObjectsOutput
var listObjectsBucketName string
var listObjectsPrefix string

func (m mockS3Client) ListObjects(input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	listObjectsBucketName = *input.Bucket
	if input.Prefix != nil {
		listObjectsPrefix = *input.Prefix
	}
	return &listObjectsOutput, nil
}

func newMockS3(sess *session.Session) s3iface.S3API {
	return mockS3Client{}
}

// Mock Uploader

type mockUploader struct {
	s3manageriface.UploaderAPI
}

var uploadBucketName string
var uploadKey string

func (m mockUploader) Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	uploadBucketName = *input.Bucket
	uploadKey = *input.Key
	return &s3manager.UploadOutput{}, nil
}

func newMockUploader(sess *session.Session) s3manageriface.UploaderAPI {
	return mockUploader{}
}

// Mock Downloader

type mockDownloader struct {
	s3manageriface.DownloaderAPI
}

var downloadBucketName string
var downloadKey string

func (m mockDownloader) Download(w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (int64, error) {
	downloadBucketName = *input.Bucket
	downloadKey = *input.Key
	return 0, nil
}

func newMockDownloader(sess *session.Session) s3manageriface.DownloaderAPI {
	return mockDownloader{}
}

func TestBucket(t *testing.T) {

	// Init klog
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Parse()
	klog.Infof("k8s-snap pkg objectstore test")
	klog.Flush()

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "https://endpoint.net", "region", "k8s-snap", true)

	// ChkBucket with no bucket
	found, _ := b.ChkBucket()
	if found {
		t.Error("Bucket k8s-snap must not be found")
	}

	// ChkBucket with bucket named k8s-snap
	bucketName := "k8s-snap"
	bu := s3.Bucket{Name: &bucketName}
	listBucketsOutput.SetBuckets([]*s3.Bucket{&bu})
	found, _ = b.ChkBucket()
	if !found {
		t.Error("Bucket k8s-snap must be found")
	}

	// ChkBucket with bucket named k8s-foo
	bucketName = "k8s-foo"
	found, _ = b.ChkBucket()
	if found {
		t.Error("Bucket k8s-snap must not be found")
	}

	// Create Bucket
	b.CreateBucket()
	if createdBucketName != "k8s-snap" {
		t.Errorf("Error in Created Bucket name")
	}

	// Upload a file
	b.Upload(nil, "UPLOAD_FILENAME")
	if uploadBucketName != "k8s-snap" {
		t.Errorf("Error in Upload Bucket name")
	}
	if uploadKey != "UPLOAD_FILENAME" {
		t.Errorf("Error in Upload Key")
	}

	// Upload a file
	b.Download(nil, "DOWNLOAD_FILENAME")
	if downloadBucketName != "k8s-snap" {
		t.Errorf("Error in Download Bucket name")
	}
	if downloadKey != "DOWNLOAD_FILENAME" {
		t.Errorf("Error in Download Key")
	}

	// Delete a file
	b.Delete("DELETE_FILENAME")
	if deleteObjectBucketName != "k8s-snap" {
		t.Errorf("Error in Delete Object Bucket name")
	}
	if deleteObjectKey != "DELETE_FILENAME" {
		t.Errorf("Error in Delete Object Key")
	}
	if headObjectBucketName != "k8s-snap" {
		t.Errorf("Error in head Object Bucket name")
	}
	if headObjectKey != "DELETE_FILENAME" {
		t.Errorf("Error in head Object Key")
	}

	// Get file info without list
	_, err := b.GetObjectInfo("GETINFO_FILENAME")
	if listObjectsBucketName != "k8s-snap" {
		t.Errorf("Error in list Object Bucket name")
	}
	if listObjectsPrefix != "GETINFO_FILENAME" {
		t.Errorf("Error in list Object Prefix")
	}
	if err == nil {
		t.Errorf("Error must be occurred GetObjectInfo without list")
	}

	// Get file info
	objKey := "GETINFO_FILENAME"
	objTime := time.Date(2001, 5, 20, 23, 59, 59, 0, time.UTC)
	objSize := int64(131072)
	obj := s3.Object{Key: &objKey, LastModified: &objTime, Size: &objSize}
	listObjectsOutput.SetContents([]*s3.Object{&obj})
	objectInfo, err := b.GetObjectInfo("GETINFO_FILENAME")
	if err != nil {
		t.Errorf("Error object not found")
	}
	if objectInfo.Name != "GETINFO_FILENAME" {
		t.Errorf("Error in GetObjectInfo Name")
	}
	if objectInfo.Size != objSize {
		t.Errorf("Error in GetObjectInfo Size")
	}
	if !objectInfo.Timestamp.Equal(objTime) {
		t.Errorf("Error in GetObjectInfo Time")
	}
	if objectInfo.BucketConfigName != "test" {
		t.Errorf("Error in GetObjectInfo BucketConfig")
	}

	// List file info
	objectInfoList, _ := b.ListObjectInfo()
	if objectInfoList[0].Name != "GETINFO_FILENAME" {
		t.Errorf("Error in ListObjectInfo Name")
	}
	if objectInfoList[0].Size != objSize {
		t.Errorf("Error in ListObjectInfo Size")
	}
	if !objectInfoList[0].Timestamp.Equal(objTime) {
		t.Errorf("Error in ListObjectInfo Time")
	}
	if objectInfoList[0].BucketConfigName != "test" {
		t.Errorf("Error in ListObjectInfo BucketConfig")
	}
}
