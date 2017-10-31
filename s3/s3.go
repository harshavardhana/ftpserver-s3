/*
 * ftp-s3 (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package s3 is a ftp server driver
package s3

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/fclairamb/ftpserver/server"
	"github.com/go-ini/ini"
	minio "github.com/minio/minio-go"
)

// MainDriver defines a very basic serverftp driver
type MainDriver struct {
	SettingsFile string // Settings file
	BaseDir      string // Base directory from which to serve file
	client       *minio.Client
	tlsConfig    *tls.Config // TLS config (if applies)
}

// WelcomeUser is called to send the very first welcome message
func (driver *MainDriver) WelcomeUser(cc server.ClientContext) (string, error) {
	// This will remain the official name for now
	return fmt.Sprintf("Welcome on ftpserver, you're on dir %s", driver.BaseDir), nil
}

// AuthUser authenticates the user and selects an handling driver
func (driver *MainDriver) AuthUser(cc server.ClientContext, user, pass string) (server.ClientHandlingDriver, error) {
	return driver, nil
}

// GetTLSConfig returns a TLS Certificate to use
func (driver *MainDriver) GetTLSConfig() (*tls.Config, error) {
	if driver.tlsConfig == nil {
		cert, err := tls.LoadX509KeyPair("mycert.crt", "mycert.key")
		if err != nil {
			return nil, err
		}
		driver.tlsConfig = &tls.Config{
			NextProtos:   []string{"ftp"},
			Certificates: []tls.Certificate{cert},
		}
	}
	return driver.tlsConfig, nil
}

// ChangeDirectory changes the current working directory
func (driver *MainDriver) ChangeDirectory(cc server.ClientContext, directory string) error {
	_, err := driver.client.StatObject(driver.BaseDir, directory, minio.StatObjectOptions{})
	return err
}

// MakeDirectory creates a directory
func (driver *MainDriver) MakeDirectory(cc server.ClientContext, directory string) error {
	_, err := driver.client.PutObject(driver.BaseDir, directory, bytes.NewReader([]byte("")), 0, minio.PutObjectOptions{})
	return err
}

type mObjectInfo struct {
	objInfo minio.ObjectInfo
	isDir   bool
}

func (o mObjectInfo) Name() string {
	return o.objInfo.Key
}

func (o mObjectInfo) Size() int64 {
	return o.objInfo.Size
}

func (o mObjectInfo) Mode() os.FileMode {
	if o.isDir {
		return os.ModeDir
	}
	return os.FileMode(0644)
}

func (o mObjectInfo) ModTime() time.Time {
	return o.objInfo.LastModified
}

func (o mObjectInfo) IsDir() bool {
	return o.isDir
}

func (o mObjectInfo) Sys() interface{} {
	return &syscall.Stat_t{}
}

// ListFiles lists the files of a directory
func (driver *MainDriver) ListFiles(cc server.ClientContext) ([]os.FileInfo, error) {
	var fileInfos []os.FileInfo
	var prefix = cc.Path()
	if prefix == "/" {
		prefix = ""
	}
	for obj := range driver.client.ListObjects(driver.BaseDir, prefix, false, nil) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.HasSuffix(obj.Key, "/") {
			fileInfos = append(fileInfos, mObjectInfo{
				isDir: true,
				objInfo: minio.ObjectInfo{
					Key:          obj.Key,
					LastModified: time.Now().UTC(),
				},
			})
			continue
		}
		fileInfos = append(fileInfos, mObjectInfo{objInfo: obj})
	}
	return fileInfos, nil
}

// UserLeft is called when the user disconnects, even if he never authenticated
func (driver *MainDriver) UserLeft(cc server.ClientContext) {}

type nopWriter struct {
	*minio.Object
}

func (n nopWriter) Write(p []byte) (int, error) {
	return 0, nil
}

type nopReadSeeker struct {
	*io.PipeWriter
}

func (n nopReadSeeker) Read(p []byte) (int, error) {
	return 0, nil
}

func (n nopReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

// OpenFile opens a file in 3 possible modes: read, write, appending write (use appropriate flags)
func (driver *MainDriver) OpenFile(cc server.ClientContext, path string, flag int) (server.FileStream, error) {
	if (flag & os.O_RDONLY) != 0 {
		obj, err := driver.client.GetObject(driver.BaseDir, path, minio.GetObjectOptions{})
		return nopWriter{obj}, err
	}
	reader, writer := io.Pipe()
	go func() {
		if _, err := driver.client.PutObject(driver.BaseDir, path, reader, -1, minio.PutObjectOptions{}); err != nil {
			reader.CloseWithError(err)
			return
		}
		reader.Close()
	}()
	return nopReadSeeker{writer}, nil
}

// GetFileInfo gets some info around a file or a directory
func (driver *MainDriver) GetFileInfo(cc server.ClientContext, path string) (os.FileInfo, error) {
	objInfo, err := driver.client.StatObject(driver.BaseDir, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return mObjectInfo{objInfo: objInfo}, nil
}

// CanAllocate gives the approval to allocate some data
func (driver *MainDriver) CanAllocate(cc server.ClientContext, size int) (bool, error) {
	return true, nil
}

// ChmodFile changes the attributes of the file
func (driver *MainDriver) ChmodFile(cc server.ClientContext, path string, mode os.FileMode) error {
	return nil
}

// DeleteFile deletes a file or a directory
func (driver *MainDriver) DeleteFile(cc server.ClientContext, path string) error {
	return driver.client.RemoveObject(driver.BaseDir, path)
}

// RenameFile renames a file or a directory
func (driver *MainDriver) RenameFile(cc server.ClientContext, from, to string) error {
	// Source object
	src := minio.NewSourceInfo(driver.BaseDir, from, nil)

	// Destination object
	dst, err := minio.NewDestinationInfo(driver.BaseDir, to, nil, nil)
	if err != nil {
		return err
	}

	if err = driver.client.CopyObject(dst, src); err != nil {
		return err
	}
	return driver.client.RemoveObject(driver.BaseDir, from)
}

// GetSettings returns some general settings around the server setup
func (driver *MainDriver) GetSettings() *server.Settings {
	config, err := ini.Load(driver.SettingsFile)
	if err != nil {
		panic(err)
	}

	sec, err := config.GetSection("default")
	if err != nil {
		panic(err)
	}

	var (
		listenHost = sec.Key("listen_host")
		listenPort = sec.Key("listen_port")
		publicHost = sec.Key("public_host")
		maxConn    = sec.Key("max_connections")
	)

	portRange, err := config.GetSection("port-range")
	if err != nil {
		panic(err)
	}

	settings := &server.Settings{
		ListenHost:     listenHost.String(),
		ListenPort:     listenPort.MustInt(),
		PublicHost:     publicHost.String(),
		MaxConnections: maxConn.MustInt(),
		DataPortRange: &server.PortRange{
			Start: portRange.Key("start").MustInt(),
			End:   portRange.Key("end").MustInt(),
		},
	}

	// This is the new IP loading change coming from Ray
	if settings.PublicHost == "" {
		settings.PublicHost, _ = externalIP()
	}

	// Settings..
	return settings
}

// NewDriver creates a new s3 driver
func NewDriver(configFile, dataDir, endpoint, accessKey, secretKey string) (*MainDriver, error) {
	if dataDir == "" {
		return nil, errors.New("empty data dir not supported")
	}

	clnt, err := minio.New(endpoint, accessKey, secretKey, false)
	if err != nil {
		return nil, err
	}
	clnt.TraceOn(os.Stderr)

	driver := &MainDriver{
		SettingsFile: configFile,
		BaseDir:      dataDir,
		client:       clnt,
	}

	clnt.MakeBucket(driver.BaseDir, "")
	return driver, nil
}

func externalIP() (string, error) {
	// If you need to take a bet, amazon is about as reliable & sustainable a service as you can get
	rsp, err := http.Get("http://checkip.amazonaws.com")
	if err != nil {
		return "", err
	}
	defer rsp.Body.Close()

	buf, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return "", err
	}

	return string(bytes.TrimSpace(buf)), nil
}
