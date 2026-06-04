package storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpStorage struct {
	host       string
	port       int
	username   string
	password   string
	privateKey string
	directory  string
	urlPrefix  string
}

func NewSFTPStorage(host string, port int, username, password, privateKey, directory, urlPrefix string) StorageService {
	if port == 0 {
		port = 22
	}
	return &sftpStorage{
		host:       host,
		port:       port,
		username:   username,
		password:   password,
		privateKey: privateKey,
		directory:  directory,
		urlPrefix:  urlPrefix,
	}
}

func (s *sftpStorage) getClient() (*ssh.Client, *sftp.Client, error) {
	var auth []ssh.AuthMethod

	if s.privateKey != "" {
		signer, err := ssh.ParsePrivateKey([]byte(s.privateKey))
		if err != nil {
			return nil, nil, fmt.Errorf("unable to parse private key: %w", err)
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}

	if s.password != "" {
		auth = append(auth, ssh.Password(s.password))
	}

	config := &ssh.ClientConfig{
		User:            s.username,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	sshClient, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to %s: %w", addr, err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return nil, nil, fmt.Errorf("unable to start sftp subsystem: %w", err)
	}

	return sshClient, sftpClient, nil
}

func (s *sftpStorage) getAbsolutePath(p string) string {
	return path.Join(s.directory, p)
}

func (s *sftpStorage) Save(ctx context.Context, p string, mimeType string, file io.Reader) (int64, error) {
	sshConn, sftpClient, err := s.getClient()
	if err != nil {
		return 0, err
	}
	defer sshConn.Close()
	defer sftpClient.Close()

	absPath := s.getAbsolutePath(p)
	dir := path.Dir(absPath)

	err = sftpClient.MkdirAll(dir)
	if err != nil {
		return 0, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	f, err := sftpClient.Create(absPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file %s: %w", absPath, err)
	}
	defer f.Close()

	n, err := io.Copy(f, file)
	if err != nil {
		return n, fmt.Errorf("failed to copy data to %s: %w", absPath, err)
	}

	return n, nil
}

func (s *sftpStorage) Get(ctx context.Context, p string, opts ...GetOption) (io.ReadCloser, error) {
	sshConn, sftpClient, err := s.getClient()
	if err != nil {
		return nil, err
	}

	absPath := s.getAbsolutePath(p)
	f, err := sftpClient.Open(absPath)
	if err != nil {
		sftpClient.Close()
		sshConn.Close()
		return nil, fmt.Errorf("failed to open file %s: %w", absPath, err)
	}

	return &sftpReadCloser{
		File:       f,
		sftpClient: sftpClient,
		sshConn:    sshConn,
	}, nil
}

type sftpReadCloser struct {
	*sftp.File
	sftpClient *sftp.Client
	sshConn    *ssh.Client
}

func (r *sftpReadCloser) Close() error {
	err1 := r.File.Close()
	err2 := r.sftpClient.Close()
	err3 := r.sshConn.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

func (s *sftpStorage) Exists(ctx context.Context, p string) (bool, error) {
	sshConn, sftpClient, err := s.getClient()
	if err != nil {
		return false, err
	}
	defer sshConn.Close()
	defer sftpClient.Close()

	absPath := s.getAbsolutePath(p)
	_, err = sftpClient.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *sftpStorage) Delete(ctx context.Context, p string, recursive bool) error {
	sshConn, sftpClient, err := s.getClient()
	if err != nil {
		return err
	}
	defer sshConn.Close()
	defer sftpClient.Close()

	absPath := s.getAbsolutePath(p)

	if recursive {
		walker := sftpClient.Walk(absPath)
		var items []string
		for walker.Step() {
			if walker.Err() != nil {
				continue
			}
			items = append(items, walker.Path())
		}
		// Delete items in reverse order
		for i := len(items) - 1; i >= 0; i-- {
			info, err := sftpClient.Stat(items[i])
			if err != nil {
				continue
			}
			if info.IsDir() {
				_ = sftpClient.RemoveDirectory(items[i])
			} else {
				_ = sftpClient.Remove(items[i])
			}
		}
		return nil
	}

	return sftpClient.Remove(absPath)
}

func (s *sftpStorage) List(ctx context.Context) ([]string, error) {
	sshConn, sftpClient, err := s.getClient()
	if err != nil {
		return nil, err
	}
	defer sshConn.Close()
	defer sftpClient.Close()

	var fileNames []string
	walker := sftpClient.Walk(s.directory)
	for walker.Step() {
		if walker.Err() != nil {
			return nil, walker.Err()
		}
		if !walker.Stat().IsDir() {
			relPath, err := filepath.Rel(s.directory, walker.Path())
			if err != nil {
				fileNames = append(fileNames, walker.Path())
			} else {
				fileNames = append(fileNames, relPath)
			}
		}
	}

	return fileNames, nil
}

func (s *sftpStorage) GetUrl(ctx context.Context, p string) (string, error) {
	return fmt.Sprintf("%s%s", s.urlPrefix, p), nil
}

func (s *sftpStorage) FileInfo(ctx context.Context, p string) (fs.FileInfo, error) {
	sshConn, sftpClient, err := s.getClient()
	if err != nil {
		return nil, err
	}
	defer sshConn.Close()
	defer sftpClient.Close()

	absPath := s.getAbsolutePath(p)
	return sftpClient.Stat(absPath)
}
