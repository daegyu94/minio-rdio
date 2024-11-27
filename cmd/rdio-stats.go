package cmd

import (
	"fmt"
	"os"
	"time"
)

const (
	ETE_PUT_OBJECT = iota

	BR_PUT_OBJECT_BEGIN
	BR_PUT_OBJECT_ENCODE
	BR_PUT_OBJECT_WRITE
	BR_PUT_OBJECT_ENCODE_AND_WRITE
	BR_PUT_OBJECT_COMMIT_PREPARE
	BR_PUT_OBJECT_COMMIT

	BR_REMOTE_WRITE
	BR_REMOTE_WRITE_RPC
	BR_REMOTE_WRITE_IO
	BR_REMOTE_WRITE_CACHE
	BR_REMOTE_WRITE_GET_SLAB

	BR_CREATE_FILE_LOCAL
	BR_CREATE_FILE_REMOTE

	ETE_GET_OBJECT
	BR_GET_OBJECT_GET_METADATA
	BR_GET_OBJECT_READ_DATA
	BR_GET_OBJECT_DECODE_DATA

	BR_REMOTE_READ
	BR_REMOTE_READ_RPC
	BR_REMOTE_READ_IO
	BR_REMOTE_READ_CACHE

	BR_REMOTE_METADATA_READ
	BR_REMOTE_METADATA_READ_RPC
	BR_REMOTE_METADATA_READ_IO
	BR_REMOTE_METADATA_READ_CACHE
	BR_REMOTE_METADATA_READ_SERVER

	BR_RENAMEDATA_LOCAL
	BR_RENAMEDATA_REMOTE
	BR_RENAMEDATA_CACHE
	BR_RENAMEDATA_CLIENT_SLAB
	BR_RENAMEDATA_SERVER
	BR_RENAMEDATA_SERVER_FORM
	BR_RENAMEDATA_SERVER_SLAB
	BR_RENAMEDATA_SERVER_CACHE
	BR_RENAMEDATA_SERVER_RENAMEDATA

	BR_MAX
)

var timeBreakdownNames = []string{
	"ETE_PUT_OBJECT",

	"BR_PUT_OBJECT_BEGIN",
	"BR_PUT_OBJECT_ENCODE",
	"BR_PUT_OBJECT_WRITE",
	"BR_PUT_OBJECT_ENCODE_AND_WRITE",
	"BR_PUT_OBJECT_COMMIT_PREPARE",
	"BR_PUT_OBJECT_COMMIT",

	"BR_REMOTE_WRITE",
	"BR_REMOTE_WRITE_RPC",
	"BR_REMOTE_WRITE_IO",
	"BR_REMOTE_WRITE_CACHE",
	"BR_REMOTE_WRITE_GET_SLAB",

	"BR_CREATE_FILE_LOCAL",
	"BR_CREATE_FILE_REMOTE",

	"ETE_GET_OBJECT",
	"BR_GET_OBJECT_GET_METADATA",
	"BR_GET_OBJECT_READ_DATA",
	"BR_GET_OBJECT_DECODE_DATA",

	"BR_REMOTE_READ",
	"BR_REMOTE_READ_RPC",
	"BR_REMOTE_READ_IO",
	"BR_REMOTE_READ_CACHE",

	"BR_REMOTE_METADATA_READ",
	"BR_REMOTE_METADATA_READ_RPC",
	"BR_REMOTE_METADATA_READ_IO",
	"BR_REMOTE_METADATA_READ_CACHE",
	"BR_REMOTE_METADATA_READ_SERVER",

	"BR_RENAMEDATA_LOCAL",
	"BR_RENAMEDATA_REMOTE",
	"BR_RENAMEDATA_CACHE",
	"BR_RENAMEDATA_CLIENT_SLAB",
	"BR_RENAMEDATA_SERVER",
	"BR_RENAMEDATA_SERVER_FORM",
	"BR_RENAMEDATA_SERVER_SLAB",
	"BR_RENAMEDATA_SERVER_CACHE",
	"BR_RENAMEDATA_SERVER_RENAMEDATA",
}

type xlRawStorageBreakdown struct {
	Elapseds [BR_MAX]uint64
	Counters [BR_MAX]uint64
}

var brInstance xlRawStorageBreakdown

func StartTS(startTS *time.Time) {
	if globalBreakdownEnable {
		*startTS = time.Now()
	}
}

func EndTS(startTS *time.Time, name int) {
	if globalBreakdownEnable {
		elapsed := uint64(time.Since(*startTS))
		brInstance.Elapseds[name] += elapsed
		brInstance.Counters[name] += 1

		//if name == BR_GET_META {
		//	fmt.Println("elapsed=", elapsed)
		//}
	}
}

func GetAvgTS(i int) uint64 {
	if globalBreakdownEnable {
		if brInstance.Counters[i] > 0 {
			return brInstance.Elapseds[i] / 1000 / brInstance.Counters[i]
		} else {
			return 0
		}
	} else {
		return 0
	}
}

func writeToLog() {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("[ERROR] Failed to get hostname:", err)
		return
	}

	logDir := "/home/daegyu/minio-research/minio-rdio/log"
	logFile := logDir + "/" + hostname + "_breakdown.log"

	fmt.Println("[INFO] start breakdown..., logFile=", logFile)
	for {
		time.Sleep(2 * time.Second)

		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("[ERROR] Failed to open file:", err)
			continue
		}

		fmt.Fprintf(file, "[ Latency breakdown ]\n")
		for i := 0; i < BR_MAX; i++ {
			avg := uint64(0)
			if i == BR_PUT_OBJECT_WRITE {
				avg = GetAvgTS(BR_PUT_OBJECT_ENCODE_AND_WRITE) - GetAvgTS(BR_PUT_OBJECT_ENCODE)
			} else {
				avg = GetAvgTS(i)
			}
			_, err := fmt.Fprintf(file, "%6d(us)=%s\n", avg, timeBreakdownNames[i])
			//_, err := fmt.Fprintf(file, "[%d] %s: %d(us)\n", i, timeBreakdownNames[i], avg)
			if err != nil {
				fmt.Println("[ERROR] Failed to write to file:", err)
				continue
			}
		}

		/*
			fmt.Fprintf(file, "[ Mapping cache ]\n")
			fmt.Fprintf(file, "len=%d\n", )
			fmt.Fprintf(file, "Put: Hit=%d, Miss=%d\n")
			fmt.Fprintf(file, "Get: Hit=%d, Miss=%d\n")
		*/
		file.Close()
	}
}

func clearReadStats() {
	for i := ETE_GET_OBJECT; i < BR_REMOTE_METADATA_READ_SERVER+1; i++ {
		brInstance.Elapseds[i] = 0
		brInstance.Counters[i] = 0

	}
}

func init() {
	go writeToLog()
}
