package cmd

import (
	"regexp"
	"strconv"
	"time"
)

//go:generate msgp
type Request struct {
	Id       int64  `msg:"id"`
	Type     byte   `msg:"type"`
	FilePath string `msg:"path"`
	Volume   string `msg:"volume,omitempty"`
	Object   string `msg:"object,omitempty"`
	FileSize int    `msg:"filesize,omitempty"`
	Len      int    `msg:"len,omitempty"`
}

type RawIOResponse struct {
	Extents  []ExtentInfo `msg:"es"`
	FileSize int          `msg:"filesize,omitempty"`
	Err      string       `msg:"error,omitempty"`
	MTime    time.Time    `msg:"time,omitempty"`
}

type FileSlabExtentInfo struct {
	Extents  []ExtentInfo
	FileSize int
}

type FileSlabResponse struct {
	Fsei []FileSlabExtentInfo `msg:"fsei"`
}

const (
	MsgReadFileStream = 0
	MsgCreateFile     = 1
	MsgReadXL         = 2
	MsgFileSlab       = 3
	MsgMkdir          = 4

	MaxMsgSize = 1024

	BasePort = 10000
)

var writeRestAPI = false
var readRestAPI = false
var readXLRestAPI = false

var XLCaching = true // false

func extractNumberWithRegex(path string) int {
	re := regexp.MustCompile(`minio(\d+)`) // /mnt/minio1/data
	match := re.FindStringSubmatch(path)
	if len(match) != 2 {
		return -1
	}
	number, err := strconv.Atoi(match[1])
	if err != nil {
		return -1
	}
	return number
}

func isRegularBucket(bucket string) bool {
	if bucket != ".minio.sys" && bucket != ".minio.sys/buckets" {
		return true
	} else {
		return false
	}
}

func isRegularObject(object string) bool {
	if object != "pool.bin" && object != "config/config.json" &&
		object != "config/iam/format.json" {
		return true
	} else {
		return false
	}
}
