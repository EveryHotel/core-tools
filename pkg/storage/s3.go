package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3Storage struct {
	endpoint string
	region   string
	accessId string
	secretId string
	bucket   string
	proxy    string
}

func NewS3Storage(endpoint string, region string, bucket string, accessId string, secretId string, proxy string) StorageService {
	return &s3Storage{
		endpoint: endpoint,
		region:   region,
		accessId: accessId,
		secretId: secretId,
		bucket:   bucket,
		proxy:    proxy,
	}
}

func (s *s3Storage) getClient(ctx context.Context) (*s3.Client, error) {
	creds := credentials.NewStaticCredentialsProvider(s.accessId, s.secretId, "")

	cfg, err := config.LoadDefaultConfig(ctx, config.WithCredentialsProvider(creds), config.WithRegion(s.region), config.WithBaseEndpoint(s.endpoint))
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	}), nil
}

func (s *s3Storage) Save(ctx context.Context, path string, mimeType string, file io.Reader) (int64, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return 0, err
	}

	upload := s3.PutObjectInput{
		ACL:    types.ObjectCannedACLPublicRead,
		Body:   file,
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}

	if mimeType != "" {
		upload.ContentType = aws.String(mimeType)
	}

	uploader := manager.NewUploader(client)

	_, err = uploader.Upload(ctx, &upload)

	if err != nil {
		return 0, err
	}

	headObj := s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	result, err := client.HeadObject(ctx, &headObj)
	if err != nil {
		return 0, err
	}
	return aws.ToInt64(result.ContentLength), nil
}

func (s *s3Storage) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}

	get := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}

	output, err := client.GetObject(ctx, get)
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (s *s3Storage) Exists(ctx context.Context, path string) (bool, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return false, err
	}

	get := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}
	_, err = client.HeadObject(ctx, get)
	if err != nil {
		var responseError *awshttp.ResponseError
		if errors.As(err, &responseError) && responseError.ResponseError.HTTPStatusCode() == http.StatusNotFound {
			return false, nil
		}

		return false, err
	}
	return true, nil
}

func (s *s3Storage) GetUrl(ctx context.Context, file string) (string, error) {
	if s.proxy == "" {
		u, err := url.Parse(s.endpoint)
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

func (s *s3Storage) Delete(ctx context.Context, path string, recursive bool) (err error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return err
	}

	if recursive {
		if path[len(path)-1] != '/' {
			path += "/"
		}
		input := &s3.ListObjectsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(path),
		}
		objects, err := client.ListObjects(ctx, input)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		if len(objects.Contents) == 0 {
			return nil
		}

		var objectIds []types.ObjectIdentifier
		for _, obj := range objects.Contents {
			objectIds = append(objectIds, types.ObjectIdentifier{Key: obj.Key})
		}
		deleteInput := s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{
				Objects: objectIds,
			},
		}

		_, err = client.DeleteObjects(ctx, &deleteInput)
		if err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
	} else {
		deleteInput := &s3.DeleteObjectInput{
			Key:    aws.String(path),
			Bucket: aws.String(s.bucket),
		}
		_, err = client.DeleteObject(ctx, deleteInput)
		if err != nil {
			return fmt.Errorf("delete object: %w", err)
		}
	}

	return nil
}

func (s *s3Storage) List(ctx context.Context) ([]string, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}

	input := &s3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
	}

	objects, err := client.ListObjects(ctx, input)
	if err != nil {
		return nil, err
	}
	var fileNames []string
	for _, obj := range objects.Contents {
		if obj.Key == nil {
			continue
		}

		fileNames = append(fileNames, aws.ToString(obj.Key))
	}
	return fileNames, nil
}

func (s *s3Storage) FileInfo(ctx context.Context, path string) (os.FileInfo, error) {
	client, err := s.getClient(ctx)
	if err != nil {
		return nil, err
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	}

	output, err := client.HeadObject(ctx, input)
	if err != nil {
		return nil, err
	}

	size := int64(0)
	if output.ContentLength != nil {
		size = *output.ContentLength
	}
	return &s3FileInfo{
		key:  path,
		size: size,
	}, nil
}

type s3FileInfo struct {
	key  string
	size int64
}

func (s *s3FileInfo) Name() string {
	return path.Base(s.key)
}

func (s *s3FileInfo) Size() int64 {
	return s.size
}

func (s *s3FileInfo) Mode() os.FileMode {
	return os.ModePerm
}

func (s *s3FileInfo) ModTime() time.Time {
	return time.Now()
}

func (s *s3FileInfo) IsDir() bool {
	return false
}

func (s *s3FileInfo) Sys() interface{} {
	return nil
}
