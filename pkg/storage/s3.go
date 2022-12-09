package storage

import (
	"io"
	"net/url"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3Storage struct {
	s3Session *session.Session
	bucket    string
	proxy     string
}

func NewS3Storage(endpoint string, region string, bucket string, accessId string, secretId string, proxy string) StorageService {
	newSession := session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(accessId, secretId, ""),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String(region),
	}))

	return &s3Storage{
		s3Session: newSession,
		bucket:    bucket,
		proxy:     proxy,
	}
}

func (s *s3Storage) Save(path string, mimeType string, file io.Reader) error {
	upload := s3manager.UploadInput{
		ACL:    aws.String("public-read"),
		Body:   file,
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}

	if mimeType != "" {
		upload.ContentType = aws.String(mimeType)
	}

	uploader := s3manager.NewUploader(s.s3Session)

	_, err := uploader.Upload(&upload)

	return err
}

func (s *s3Storage) Get(path string) (io.ReadCloser, error) {
	get := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	output, err := s3.New(s.s3Session).GetObject(get)
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (s *s3Storage) GetUrl(file string) (string, error) {
	endpoint := s.proxy
	if endpoint == "" {
		endpoint = s3.New(s.s3Session).Endpoint
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	u.Path += path.Join(s.bucket, file)
	return u.String(), nil
}

func (s *s3Storage) Delete(path string) error {
	deleteInput := &s3.DeleteObjectInput{
		Key:    aws.String(path),
		Bucket: aws.String(s.bucket),
	}
	_, err := s3.New(s.s3Session).DeleteObject(deleteInput)

	return err
}

func (s *s3Storage) List() ([]string, error) {
	input := &s3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
	}

	objects, err := s3.New(s.s3Session).ListObjects(input)
	if err != nil {
		return nil, err
	}
	var fileNames []string
	for _, obj := range objects.Contents {
		fileNames = append(fileNames, aws.StringValue(obj.Key))
	}
	return fileNames, nil
}
