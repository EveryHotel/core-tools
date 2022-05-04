package storage

import (
	"io"
	"net/url"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Storage struct {
	client *s3.S3
	bucket string
}

func NewS3Storage(endpoint string, region string, bucket string, accessId string, secretId string) StorageService {
	newSession := session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(accessId, secretId, ""),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String(region),
	}))

	return &s3Storage{
		client: s3.New(newSession),
		bucket: bucket,
	}
}

func (s *s3Storage) Save(path string, mimeType string, file io.ReadSeeker) error {
	put := &s3.PutObjectInput{
		ACL:    aws.String("public-read"),
		Body:   file,
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	if mimeType != "" {
		put.ContentType = aws.String(mimeType)
	}

	_, err := s.client.PutObject(put)

	return err
}

func (s *s3Storage) Get(path string) (io.ReadCloser, error) {
	get := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	output, err := s.client.GetObject(get)
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (s *s3Storage) GetUrl(file string) (string, error) {
	u, err := url.Parse(s.client.Endpoint)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(s.bucket, file)
	return u.String(), nil
}

func (s *s3Storage) Delete(path string) error {
	deleteInput := &s3.DeleteObjectInput{
		Key:    aws.String(path),
		Bucket: aws.String(s.bucket),
	}
	_, err := s.client.DeleteObject(deleteInput)

	return err
}

func (s *s3Storage) List() ([]string, error) {
	input := &s3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
	}

	objects, err := s.client.ListObjects(input)
	if err != nil {
		return nil, err
	}
	var fileNames []string
	for _, obj := range objects.Contents {
		fileNames = append(fileNames, aws.StringValue(obj.Key))
	}
	return fileNames, nil
}
