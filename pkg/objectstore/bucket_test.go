package objectstore

import (
	"flag"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/sts/stsiface"
	"k8s.io/klog"
)

func TestGetBucket(t *testing.T) {

	// Init klog
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Parse()
	klog.Infof("k8s-volume-snap/pkg/objectstore test")
	klog.Flush()

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "ROLEARN",
		"https://endpoint.net", "region", "k8s-volume-snap", true)

	// get methods
	if b.GetName() != "test" {
		t.Error("Error in GetName()")
	}
	if b.GetEndpoint() != "https://endpoint.net" {
		t.Error("Error in GetEndpoint()")
	}
	if b.GetBucketName() != "k8s-volume-snap" {
		t.Error("Error in GetBucketName()")
	}
	if b.GetAccessKey() != "ACCESSKEY" {
		t.Error("Error in GetAccessKey()")
	}
	if b.GetSecretKey() != "SECRETKEY" {
		t.Error("Error in GetSecretKey()")
	}
}

func TestChkBucket(t *testing.T) {

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "ROLEARN",
		"https://endpoint.net", "region", "k8s-volume-snap", true)

	// ChkBucket with no bucket
	found, _ := b.ChkBucket()
	if found {
		t.Error("Bucket k8s-snap must not be found")
	}

	// ChkBucket with bucket named k8s-snap
	bucketName := "k8s-volume-snap"
	bu := s3.Bucket{Name: &bucketName}
	listBucketsOutput.SetBuckets([]*s3.Bucket{&bu})
	found, _ = b.ChkBucket()
	if !found {
		t.Error("Bucket k8s-volume-snap must be found")
	}

	// ChkBucket with bucket named k8s-foo
	bucketName = "k8s-foo"
	found, _ = b.ChkBucket()
	if found {
		t.Error("Bucket k8s-foo must not be found")
	}

	// ChkBucket error
	mockErr = fmt.Errorf("ChkBucket error")
	_, err := b.ChkBucket()
	chkErr(t, mockErr, err)
	mockErr = nil
}

func TestCreateBucket(t *testing.T) {

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "ROLEARN",
		"https://endpoint.net", "region", "k8s-volume-snap", true)

	// Create Bucket
	b.CreateBucket()
	if createdBucketName != "k8s-volume-snap" {
		t.Errorf("Error in Created Bucket name")
	}

	// CreateBucket error
	mockErr = fmt.Errorf("CreateBucket error")
	_, err := b.ChkBucket()
	chkErr(t, mockErr, err)
	mockErr = nil
}

func TestAssumeRole(t *testing.T) {

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "ROLEARN",
		"https://endpoint.net", "region", "k8s-volume-snap", true)

	// AssumeRole
	b.CreateAssumeRole("clusterid", 999)
	mockPolicyString := `{
		"Version":"2012-10-17",
		"Statement":[
			{
				"Action":["s3:GetBucketLocation"],
				"Effect":"Allow",
				"Resource":["arn:aws:s3:::k8s-volume-snap"],
				"Sid":"AllowStatement1"
			},
			{
				"Action":["s3:ListBucket"],
				"Effect":"Allow",
				"Resource":["arn:aws:s3:::k8s-volume-snap"],
				"Condition":{"StringLike":{"s3:prefix": "clusterid/*"}},
				"Sid":"AllowStatement1B"
			},
			{
				"Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],
				"Effect":"Allow",
				"Resource":["arn:aws:s3:::k8s-volume-snap/clusterid/*"],
				"Sid":"AllowStatement2"
			}
		]
	}`
	mockPolicyString = strings.Replace(mockPolicyString, "\n", "", -1)
	mockPolicyString = strings.Replace(mockPolicyString, "\t", "", -1)
	if policy != mockPolicyString {
		t.Errorf("Error making policy string")
	}
	if roleArn != "ROLEARN" {
		t.Errorf("Error setting RoleArn")
	}
	if durationSeconds != 999 {
		t.Errorf("Error setting duration seconds")
	}

	// AssumeRole error
	mockErr = fmt.Errorf("AssumeRole error")
	_, err := b.CreateAssumeRole("", 0)
	chkErr(t, mockErr, err)
	mockErr = nil
}

func TestUpload(t *testing.T) {

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "ROLEARN",
		"https://endpoint.net", "region", "k8s-volume-snap", true)

	// Upload a file
	b.Upload(nil, "UPLOAD_FILENAME")
	if uploadBucketName != "k8s-volume-snap" {
		t.Errorf("Error in Upload Bucket name")
	}
	if uploadKey != crPath + "UPLOAD_FILENAME" {
		t.Errorf("Error in Upload Key")
	}

	// Upload error
	mockErr = fmt.Errorf("Uplaod error")
	err := b.Upload(nil, "FILE")
	chkMsg(t, "Error uploading FILE to bucket k8s-volume-snap", err.Error())
	mockErr = nil
}

func TestDownload(t *testing.T) {

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "ROLEARN",
		"https://endpoint.net", "region", "k8s-volume-snap", true)

	// Download a file
	b.Download(nil, "DOWNLOAD_FILENAME")
	if downloadBucketName != "k8s-volume-snap" {
		t.Errorf("Error in Download Bucket name")
	}
	if downloadKey != crPath + "DOWNLOAD_FILENAME" {
		t.Errorf("Error in Download Key")
	}

	// Download error
	mockErr = fmt.Errorf("Download error")
	err := b.Download(nil, "FILE")
	chkMsg(t, "Error downloading FILE from bucket k8s-volume-snap", err.Error())
	mockErr = nil
}

func TestDelete(t *testing.T) {

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "ROLEARN",
		"https://endpoint.net", "region", "k8s-volume-snap", true)


	// Delete a file
	b.Delete("DELETE_FILENAME")
	if deleteObjectBucketName != "k8s-volume-snap" {
		t.Errorf("Error in Delete Object Bucket name")
	}
	if deleteObjectKey != crPath + "DELETE_FILENAME" {
		t.Errorf("Error in Delete Object Key")
	}
	if headObjectBucketName != "k8s-volume-snap" {
		t.Errorf("Error in head Object Bucket name")
	}
	if headObjectKey != crPath + "DELETE_FILENAME" {
		t.Errorf("Error in head Object Key")
	}

	// Delete error
	mockErr = fmt.Errorf("Delete error")
	err := b.Delete("FILE")
	chkMsg(t, "Error deleting FILE from bucket k8s-volume-snap", err.Error())
	mockErr = nil

	// Delete wait error
	mockDeleteWaitErr = fmt.Errorf("Delete wait error")
	err = b.Delete("FILE")
	chkErr(t, mockDeleteWaitErr, err)
	mockDeleteWaitErr = nil
}

func TestGetInfo(t *testing.T) {

	b := NewMockBucket("test", "ACCESSKEY", "SECRETKEY", "ROLEARN",
		"https://endpoint.net", "region", "k8s-volume-snap", true)

	// Get file info without list
	_, err := b.GetObjectInfo("GETINFO_FILENAME")
	if listObjectsBucketName != "k8s-volume-snap" {
		t.Errorf("Error in list Object Bucket name")
	}
	if listObjectsPrefix != crPath + "GETINFO_FILENAME" {
		t.Errorf("Error in list Object Prefix")
	}
	if err == nil {
		t.Errorf("Error must be occurred GetObjectInfo without list")
	} else {
		chkMsg(t, "Object GETINFO_FILENAME not found in bucket k8s-volume-snap", err.Error())
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

	// GetObjectInfo error
	mockErr = fmt.Errorf("GetObjectInfo error")
	_, err = b.GetObjectInfo("FILE")
	chkErr(t, mockErr, err)
	mockErr = nil

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

	// ListObjectInfo error
	mockErr = fmt.Errorf("ListObjectInfo error")
	_, err = b.ListObjectInfo()
	chkErr(t, mockErr, err)
	mockErr = nil
}

// NewMockBucket returns new mock Bucket
func NewMockBucket(name, accessKey, secretKey, roleArn, endpoint, region, bucketName string, insecure bool) *Bucket {
	b := NewBucket(name, accessKey, secretKey, roleArn, endpoint, region, bucketName, insecure)
	b.newS3func = newMockS3
	b.newSTSfunc = newMockSTS
	b.newUploaderfunc = newMockUploader
	b.newDownloaderfunc = newMockDownloader
	return b
}

var mockErr error

func chkMsg(t *testing.T, exp, got string) {
	if !strings.Contains(got, exp) {
		t.Errorf("message not matched \nexpected : %s\nbut got  : %s", exp, got)
	}
}

func chkErr(t *testing.T, exp, got error) {
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("Error not matched\nexpected : %v\nbut got  : %v", exp, got)
	}
}
// Mock interfaces

type mockS3Client struct {
	s3iface.S3API
}

var listBucketsOutput s3.ListBucketsOutput

func (m mockS3Client) ListBuckets(input *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	if mockErr != nil {
		return nil, mockErr
	}
	return &listBucketsOutput, nil
}

var createdBucketName string

func (m mockS3Client) CreateBucket(input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	if mockErr != nil {
		return nil, mockErr
	}
	createdBucketName = *input.Bucket
	return &s3.CreateBucketOutput{}, nil
}

var deleteObjectBucketName string
var deleteObjectKey string

func (m mockS3Client) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	deleteObjectBucketName = *input.Bucket
	deleteObjectKey = *input.Key
	if mockErr != nil {
		return nil, mockErr
	}
	return &s3.DeleteObjectOutput{}, nil
}

var headObjectBucketName string
var headObjectKey string
var mockDeleteWaitErr error

func (m mockS3Client) WaitUntilObjectNotExists(input *s3.HeadObjectInput) error {
	headObjectBucketName = *input.Bucket
	headObjectKey = *input.Key
	if mockDeleteWaitErr != nil {
		return mockDeleteWaitErr
	}
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
	if mockErr != nil {
		return nil, mockErr
	}
	return &listObjectsOutput, nil
}

func newMockS3(sess *session.Session) s3iface.S3API {
	return mockS3Client{}
}

// Mock STS

type mockSTSClient struct {
	stsiface.STSAPI
}

var durationSeconds int64
var policy string
var roleArn string
var roleSessionName string

func (m mockSTSClient) AssumeRole(input *sts.AssumeRoleInput) (*sts.AssumeRoleOutput, error) {
	durationSeconds = *input.DurationSeconds
	policy = *input.Policy
	roleArn = *input.RoleArn
	roleSessionName = *input.RoleSessionName
	if mockErr != nil {
		return nil, mockErr
	}
	return &sts.AssumeRoleOutput{}, nil
}

func newMockSTS(sess *session.Session) stsiface.STSAPI {
	return mockSTSClient{}
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
	if mockErr != nil {
		return nil, mockErr
	}
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
	if mockErr != nil {
		return 0, mockErr
	}
	return 0, nil
}

func newMockDownloader(sess *session.Session) s3manageriface.DownloaderAPI {
	return mockDownloader{}
}
