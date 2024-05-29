package storage

import (
	"fmt"
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

func (s *s3Storage) Save(path string, mimeType string, file io.Reader) (int64, error) {
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
	if err != nil {
		return 0, err
	}

	headObj := s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	result, err := s3.New(s.s3Session).HeadObject(&headObj)
	if err != nil {
		return 0, err
	}
	return aws.Int64Value(result.ContentLength), nil
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
	if s.proxy == "" {
		u, err := url.Parse(s3.New(s.s3Session).Endpoint)
		if err != nil {
			return "", err
		}
		u.Path = path.Join(s.bucket, file)
		return u.String(), nil
	}
	u, err := url.Parse(s.proxy)
	if err != nil {
		return "", err
	}

	u.Path = path.Join(u.Path, file)
	return u.String(), nil
}

func (s *s3Storage) Delete(path string, recursive bool) (err error) {
	client := s3.New(s.s3Session)
	if recursive {
		if path[len(path)-1] != '/' {
			path += "/"
		}
		input := &s3.ListObjectsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(path),
		}
		objects, err := client.ListObjects(input)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		if len(objects.Contents) == 0 {
			return nil
		}

		var objectIds []*s3.ObjectIdentifier
		for _, obj := range objects.Contents {
			objectIds = append(objectIds, &s3.ObjectIdentifier{Key: obj.Key})
		}
		deleteInput := s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &s3.Delete{
				Objects: objectIds,
			},
		}

		_, err = client.DeleteObjects(&deleteInput)
		if err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
	} else {
		deleteInput := &s3.DeleteObjectInput{
			Key:    aws.String(path),
			Bucket: aws.String(s.bucket),
		}
		_, err = client.DeleteObject(deleteInput)
		if err != nil {
			return fmt.Errorf("delete object: %w", err)
		}
	}

	return nil
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
