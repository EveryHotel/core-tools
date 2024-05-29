package storage

import (
	"context"
	"fmt"
	"io"
	"mime"
	"path"

	"github.com/google/uuid"
)

type StorageService interface {
	Save(path string, mimeType string, file io.Reader) (int64, error)
	Get(path string) (io.ReadCloser, error)
	Delete(path string, recursive bool) error
	List() ([]string, error)
	GetUrl(path string) (string, error)
}

type StorageManagerService interface {
	Upload(ctx context.Context, storageName string, uploadPrefix string, realName string, file io.Reader) (FileInfo, error)
	UploadWithFileName(ctx context.Context, storageName, uploadPrefix, fileName, mimeType string, file io.Reader) (string, int64, error)
	GetUrl(storageName string, path string) (string, error)
	Get(storageName string, path string) (io.ReadCloser, error)
	Delete(storageName string, path string, recursive bool) error
	ListFiles(storageName string) ([]string, error)
}

type FileInfo struct {
	Uuid     string
	Path     string
	OrigName string
	MimeType string
	Size     int64
}

func NewStorageManager(storages map[string]StorageService) StorageManagerService {
	return &fileService{
		storages: storages,
	}
}

type fileService struct {
	storages map[string]StorageService
}

// Upload - загружает файл в хранилище, создает новый уникальный файл с переданным суффиксом в имени, в частности расширении
func (s *fileService) Upload(ctx context.Context, storageName string, uploadPrefix string, realName string, file io.Reader) (FileInfo, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return FileInfo{}, err
	}

	filepath := u.String() + path.Ext(realName)

	var mimeType string
	ext := path.Ext(realName)
	if ext != "" {
		mimeType = mime.TypeByExtension(ext)
		if mimeType == "" {
			switch ext {
			case ".docx":
				mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
			case ".xlsx":
				mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
			case ".pptx":
				mimeType = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
			case ".doc":
				mimeType = "application/msword"
			case ".xls":
				mimeType = "application/vnd.ms-excel"
			case ".ppt":
				mimeType = "application/vnd.ms-powerpoint"
			}
		}
	}

	uploadedPath, size, err := s.UploadWithFileName(ctx, storageName, uploadPrefix, filepath, mimeType, file)
	if err != nil {
		return FileInfo{}, err
	}

	return FileInfo{
		Uuid:     u.String(),
		Path:     uploadedPath,
		OrigName: realName,
		MimeType: mimeType,
		Size:     size,
	}, nil
}

// UploadWithFileName загружает файл в хранилище с определенным именем, перезаписывает файл при необходимости
func (s *fileService) UploadWithFileName(ctx context.Context, storageName string, uploadPrefix string, fileName string, mimeType string, file io.Reader) (string, int64, error) {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return "", 0, fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}
	location := path.Join(uploadPrefix, fileName)

	size, err := storageService.Save(location, mimeType, file)
	if err != nil {
		return "", size, err
	}

	return location, size, nil
}

// GetUrl - получает ссылку на файл в хранилище
func (s *fileService) GetUrl(storageName string, path string) (string, error) {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return "", fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}

	return storageService.GetUrl(path)
}

// Get - получает содержимое файла из хранилища
func (s *fileService) Get(storageName string, path string) (io.ReadCloser, error) {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return nil, fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}

	return storageService.Get(path)
}

// Delete - удаляет файл в хранилище
func (s *fileService) Delete(storageName string, path string, recursive bool) error {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}

	return storageService.Delete(path, recursive)
}

// ListFiles - выводит список файлов в хранилище
func (s *fileService) ListFiles(storageName string) ([]string, error) {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return nil, fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}

	return storageService.List()
}
