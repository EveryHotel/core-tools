package storage

import (
	"fmt"
	"io"
	"mime"
	"path"

	"github.com/google/uuid"
)

type StorageService interface {
	Save(path string, mimeType string, file io.ReadSeeker) error
	Get(path string) (io.ReadCloser, error)
	Delete(path string) error
	List() ([]string, error)
	GetUrl(path string) (string, error)
}

type StorageManagerService interface {
	Upload(storageName string, uploadPrefix string, fileName string, file io.ReadSeeker) (string, error)
	GetUrl(storageName string, path string) (string, error)
	Get(storageName string, path string) (io.ReadCloser, error)
	Delete(storageName string, path string) error
	ListFiles(storageName string) ([]string, error)
}

func NewStorageManager(storages map[string]StorageService) StorageManagerService {
	return &fileService{
		storages: storages,
	}
}

type fileService struct {
	storages map[string]StorageService
}

//Upload - загружает файл в хранилище
func (s *fileService) Upload(storageName string, uploadPrefix string, fileName string, file io.ReadSeeker) (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	storageService, ok := s.storages[storageName]
	if ok != true {
		return "", fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}
	location := path.Join(uploadPrefix, u.String())

	var mimeType string
	ext := path.Ext(fileName)
	if ext != "" {
		location += ext
		mimeType = mime.TypeByExtension(ext)
	}

	err = storageService.Save(location, mimeType, file)
	if err != nil {
		return "", err
	}

	return storageService.GetUrl(location)
}

//GetUrl - получает ссылку на файл в хранилище
func (s *fileService) GetUrl(storageName string, path string) (string, error) {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return "", fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}

	return storageService.GetUrl(path)
}

//Get - получает содержимое файла из хранилища
func (s *fileService) Get(storageName string, path string) (io.ReadCloser, error) {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return nil, fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}

	return storageService.Get(path)
}

//Delete - удаляет файл в хранилище
func (s *fileService) Delete(storageName string, path string) error {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}

	return storageService.Delete(path)
}

//ListFiles - выводит список файлов в хранилище
func (s *fileService) ListFiles(storageName string) ([]string, error) {
	storageService, ok := s.storages[storageName]
	if ok != true {
		return nil, fmt.Errorf("file: storage \"%s\" doesn't register", storageName)
	}

	return storageService.List()
}
