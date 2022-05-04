package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var ErrOutOfStorageDir = errors.New("path is out of storage dir")

type localStorage struct {
	directory string
	urlPrefix string
}

func NewLocalStorage(directory string, urlPrefix string) StorageService {
	return &localStorage{
		directory: directory,
		urlPrefix: urlPrefix,
	}
}

func (s *localStorage) Save(path string, mimeType string, file io.ReadSeeker) error {
	apath, err := s.getAbsolute(path)
	if err != nil {
		return err
	}

	err = os.MkdirAll(filepath.Dir(apath), os.ModePerm)
	if err != nil {
		return err
	}

	f, err := os.Create(apath)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, file)

	return err
}

func (s *localStorage) Get(path string) (io.ReadCloser, error) {
	apath, err := s.getAbsolute(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(apath)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (s *localStorage) Delete(path string) error {
	apath, err := s.getAbsolute(path)
	if err != nil {
		return err
	}

	return os.Remove(apath)
}

func (s *localStorage) List() ([]string, error) {
	var fileNames []string

	err := filepath.Walk(s.directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		fileNames = append(fileNames, path)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return fileNames, nil
}

func (s *localStorage) GetUrl(p string) (string, error) {
	_, err := s.getAbsolute(p)
	if err != nil {
		return "", nil
	}

	return fmt.Sprintf("%s%s", s.urlPrefix, p), nil
}

func (s *localStorage) getAbsolute(path string) (string, error) {
	dirAbs, err := filepath.Abs(s.directory)
	if err != nil {
		return "", err
	}

	p := filepath.Join(dirAbs, path)

	// не разрешаем получать доступ к файлам вне директории хранилища или работать непосредственно с самой директорией хранилища
	if !strings.HasPrefix(p, dirAbs) || p == dirAbs {
		return "", ErrOutOfStorageDir
	}

	return p, nil
}
