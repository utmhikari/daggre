package util

import (
	"encoding/json"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

// ExistsPath is path exist
func ExistsPath(p string) bool {
	if _, err := os.Stat(p); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// IsDirectory is path a directory
func IsDirectory(p string) bool {
	if stat, err := os.Stat(p); err == nil && stat.IsDir() {
		return true
	}
	return false
}

// IsFile is path a file
func IsFile(p string) bool {
	if stat, err := os.Stat(p); err == nil && stat.Mode().IsRegular() {
		return true
	}
	return false
}

// GetFileStat get stat file info
func GetFileStat(p string) (os.FileInfo, error) {
	stat, err := os.Stat(p)
	return stat, err
}

// ListFilesOfDirectory list files of a directory
func ListFilesOfDirectory(p string) (*[]os.FileInfo, error) {
	fileInfoList, err := ioutil.ReadDir(p)
	if err != nil {
		return nil, err
	}
	return &fileInfoList, nil
}

// ReadFile reads content of a file
func ReadFile(p string) (string, error) {
	bytes, err := ioutil.ReadFile(p)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ReadJsonFile reads json content of file, unmarshal to an interface{}
func ReadJsonFile(p string, v interface{}) error {
	bytes, err := ioutil.ReadFile(p)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, v)
}

func ReadYamlFile(p string, v interface{}) error {
	bytes, err := ioutil.ReadFile(p)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(bytes, v)
}
