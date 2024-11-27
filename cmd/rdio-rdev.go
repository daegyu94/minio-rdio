package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	pathutil "path"
	"sync"
	"syscall"
	"time"

	//lru "github.com/hashicorp/golang-lru/v2"
	"github.com/cespare/xxhash/v2"
	"github.com/elastic/go-freelru"
	"github.com/minio/minio/internal/disk"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/ncw/directio"
)

const (
	//MinExtentChunkSize = 8 << 10
	MinExtentChunkSize = 16 << 10
	MaxExtentChunkSize = 8 << 20

	readSyncPoolEnable  = false
	writeSyncPoolEnable = true

	//enableLocalLazySync = true
	enableLocalLazySync = false

	//enableCacheDataByWrite = true
	enableCacheDataByWrite = false

	//enableCacheMetaByWrite = true
	enableCacheMetaByWrite = false

	//enableAsyncFdatasync = true
	enableAsyncFdatasync = false
)

var (
	//numMappingCacheShards = runtime.NumCPU()
	numMappingCacheShards = 1

	//mappingCacheSize      = 10 * 1000 * (5.0 / 100)

	//mappingCacheSize = 100 * 1000 * 1.0
	//mappingCacheSize = 1 * 1000 * 1000 * 1.0
	mappingCacheSize = 2 * 1000 * 1000 * 1.0
	//mappingCacheSize = 4 * 1000 * 1000 * 1.0

	//mappingCacheSize = 10240 * (1.0 / 100)

	mappingCacheShardSize = mappingCacheSize / float64(numMappingCacheShards)

	extentSizeClasses = []int{
		8 << 10,
		16 << 10,
		32 << 10,
		64 << 10,
		128 << 10,
		256 << 10,
		512 << 10,
		1 << 20,
		2 << 20,
		4 << 20,
		8 << 20,
	}
	extentChunkPools = [...]sync.Pool{
		{New: func() interface{} { return make([]byte, 8<<10) }},
		{New: func() interface{} { return make([]byte, 16<<10) }},
		{New: func() interface{} { return make([]byte, 32<<10) }},
		{New: func() interface{} { return make([]byte, 64<<10) }},
		{New: func() interface{} { return make([]byte, 128<<10) }},
		{New: func() interface{} { return make([]byte, 256<<10) }},
		{New: func() interface{} { return make([]byte, 512<<10) }},
		{New: func() interface{} { return make([]byte, 1<<20) }},
		{New: func() interface{} { return make([]byte, 2<<20) }},
		{New: func() interface{} { return make([]byte, 4<<20) }},
		{New: func() interface{} { return make([]byte, 8<<20) }},
	}

	msgChunkPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, MaxMsgSize)
		},
	}
)

type RdioWriteOpt struct {
	object   string
	isParity bool
}

type RdioReadOpt struct {
	bypassRPC bool
}

func GetExtentBufferChunk(size int64) []byte {
	i := 0
	for ; i < len(extentSizeClasses)-1; i++ {
		if size <= int64(extentSizeClasses[i]) {
			break
		}
	}
	return extentChunkPools[i].Get().([]byte)
}

func PutExtentBufferChunk(p []byte) {
	for i, n := range extentSizeClasses {
		if len(p) == n {
			extentChunkPools[i].Put(p)
			return
		}
	}
	panic(fmt.Sprintf("unexpected buffer len=%v", len(p)))
}

type ExtCacheValue struct {
	fileSize int64
	extents  []ExtentInfo
	mtime    time.Time
}

type MappingCache struct {
	shards []*freelru.ShardedLRU[string, ExtCacheValue]
}

func lruHashFunc(key string) uint32 {
	length := len(key)
	start := length - 32
	if start < 0 {
		start = 0
	}
	hashValue := uint32(xxhash.Sum64String(key[start:]))
	//fmt.Printf("[INFO] lruHashFunc, length=%d, start=%d, hashValue=%d,\n", length, start, hashValue)
	return hashValue
}

func newMappingCache() *MappingCache {
	cache := &MappingCache{
		shards: make([]*freelru.ShardedLRU[string, ExtCacheValue], numMappingCacheShards),
	}

	for i := 0; i < numMappingCacheShards; i++ {
		lruCache, err := freelru.NewSharded[string, ExtCacheValue](uint32(mappingCacheShardSize), lruHashFunc)
		if err != nil {
			fmt.Println("[ERROR] Failed to initialize shard cache:",
				mappingCacheSize, numMappingCacheShards, mappingCacheShardSize)
			return nil
		}
		cache.shards[i] = lruCache
	}

	return cache
}

func (cache *MappingCache) GetShardId(key string) int32 {
	if numMappingCacheShards == 1 {
		return 0
	}

	start := len(key) - 32
	if start < 0 {
		start = 0
	}
	h := fnv.New32a()
	h.Write([]byte(key[start:]))
	hash := h.Sum32()
	return int32(hash) % int32(numMappingCacheShards)
}

func (cache *MappingCache) Add(key string, value ExtCacheValue) {
	shardId := cache.GetShardId(key)
	contained := cache.shards[shardId].Contains(key)
	if !contained {
		cache.shards[shardId].Add(key, value)
	}
	//fmt.Println("[INFO] MappingCache:Add(), key=", key, "value=", value)
}

func (cache *MappingCache) Get(key string) (ExtCacheValue, bool) {
	return cache.shards[cache.GetShardId(key)].Get(key)
}

func (cache *MappingCache) Remove(key string) bool {
	if false {
		shardId := cache.GetShardId(key)
		fmt.Println("[INFO] MappingCache:Remove: shardId=", shardId, "key=", key)
		cache.shards[shardId].Remove(key)
	} else {
		cache.shards[cache.GetShardId(key)].Remove(key)
	}
	return true
}

func (cache *MappingCache) Purge() {
	if false {
		for i := 0; i < numMappingCacheShards; i++ {
			cache.shards[i] = nil

			lruCache, err := freelru.NewSharded[string, ExtCacheValue](uint32(mappingCacheShardSize), lruHashFunc)
			if err != nil {
				fmt.Println("[ERROR] Failed to initialize shard cache:",
					mappingCacheSize, numMappingCacheShards, mappingCacheShardSize)
				return
			}
			cache.shards[i] = lruCache
		}
		//fmt.Println("[INFO] mapping cache len=", cache.Len())
	} else {
		for i := 0; i < numMappingCacheShards; i++ {
			cache.shards[i].Purge()
		}
	}
}

func (cache *MappingCache) Len() int {
	totalLen := 0
	for i := 0; i < numMappingCacheShards; i++ {
		totalLen += cache.shards[i].Len()
	}

	return totalLen
}

type xlRawStorage struct {
	devFile *os.File
	devPath string
	mntpnt  string

	cache        *MappingCache
	metaCache    *MappingCache
	sumWriteSize int64
}

func (s *xlRawStorage) Close() error {
	s.devFile.Close()
	s.cache = nil
	s.metaCache = nil

	return nil
}

func newXLRawStorage(endpoint Endpoint) *xlRawStorage {
	devFile, devPath, mntpnt := MapEndPoint2Device(endpoint)
	if devPath == "" {
		return nil
	}

	mappingCache := newMappingCache()
	metaMappingCache := newMappingCache()

	rawStorage := &xlRawStorage{
		devFile:   devFile,
		devPath:   devPath,
		mntpnt:    mntpnt,
		cache:     mappingCache,
		metaCache: metaMappingCache,
	}

	return rawStorage
}

type NvmeofConfig struct {
	EndPoint string `json:"endPoint"`
	DevPath  string `json:"devPath"`
	Mntpnt   string `json:"mntpnt"`
}

func MapEndPoint2Device(endpoint Endpoint) (devFile *os.File, devPath, mntpnt string) {
	configFile := "/home/daegyu/minio-research/minio-evaluation/nvmeof.config"
	configBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		return devFile, devPath, mntpnt
	}

	// Unmarshal JSON data
	var configs map[string][]NvmeofConfig
	err = json.Unmarshal(configBytes, &configs)
	if err != nil {
		fmt.Printf("[ERROR] Failed to unmarshaling config data: %v\n", err)
		return devFile, devPath, mntpnt
	}

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("[ERROR] Failed to get hostname:", err)
		return devFile, devPath, mntpnt
	}

	// Print the unmarshaled JSON data
	for host, values := range configs {
		for _, config := range values {
			//fmt.Println(hostname, host)
			//fmt.Println(config.EndPoint, endpoint.String())
			if hostname == host && config.EndPoint == endpoint.String() {
				devPath = config.DevPath
				mntpnt = config.Mntpnt
				break
			}
		}
	}

	/* TODO: don't need for NFS */
	flags := os.O_RDWR
	devFile, err = directio.OpenFile(devPath, flags, 0666)
	if err != nil {
		fmt.Println("[ERROR] Failed to open devFile=", devFile, err)
	}

	return devFile, devPath, mntpnt
}

var fallocateEnabled = true

func AllocateFileAndGetExtents(filePath string, fileSize int64, ei *[]ExtentInfo) error {
	if fallocateEnabled {
		//var startTS time.Time
		//startTS = time.Now()
		flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
		file, err := os.OpenFile(filePath, flags, 0666)
		//file, err := directio.OpenFile(filePath, flags, 0666)
		if err != nil {
			fmt.Println("[ERROR] Failed to open or create file:", err)
			return err
		}
		defer file.Close()

		err = Fallocate(file, fileSize)
		if err != nil {
			return err
		}
		//elapsed := time.Since(startTS).Microseconds()
		//fmt.Println(time.Now(), "[INFO] Fallocate fileSize=", fileSize, "elapsed(us)=", elapsed)

		//var startTS time.Time
		//startTS = time.Now()
		err = Fiemap(file, ei)
		//elapsed := time.Since(startTS).Microseconds()
		//fmt.Println(time.Now(), "[INFO] Fiemap fileSize=", fileSize, "elapsed(us)=", elapsed)
		//fmt.Println(time.Now(), "[INFO] AllocateFileAndGetExtents fileSize=", fileSize, "elapsed(us)=", elapsed)

		return err
	} else {
		// XXX: slow -> double write problem...
		w, err := OpenFileDirectIO(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o666)
		if err != nil {
			return osErrToFileErr(err)
		}

		defer func() {
			disk.Fdatasync(w) // Only interested in flushing the size_t not mtime/atime
			w.Close()
		}()

		var bufp *[]byte
		if fileSize > 0 && fileSize >= largestFileThreshold {
			// use a larger 4MiB buffer for a really large streams.
			bufp = xioutil.ODirectPoolXLarge.Get().(*[]byte)
			defer xioutil.ODirectPoolXLarge.Put(bufp)
		} else {
			bufp = xioutil.ODirectPoolLarge.Get().(*[]byte)
			defer xioutil.ODirectPoolLarge.Put(bufp)
		}

		zeroBytes := make([]byte, fileSize)
		r := bytes.NewReader(zeroBytes)
		written, err := xioutil.CopyAligned(diskHealthWriter(context.TODO(), w), r, *bufp, fileSize, w)
		if err != nil {
			return err
		}

		if written < fileSize && fileSize >= 0 {
			return errLessData
		} else if written > fileSize && fileSize >= 0 {
			return errMoreData
		}

		err = Fiemap(w, ei)
		return err
	}

}

func PwriteMix(devPath string, fd int, fileSize int64, ei []ExtentInfo, reader io.Reader) error {
	offset := int64(0)
	remaining := int64(fileSize)

	alignedSize := func(size int) int {
		const blockSize = 4096
		if size%blockSize == 0 {
			return size
		}
		return ((size / blockSize) + 1) * blockSize
	}(int(fileSize))

	totalBuf := make([]byte, alignedSize)

	n, err := io.ReadFull(reader, totalBuf[:fileSize])
	if err != nil {
		fmt.Println("[ERROR] PwriteMix: Failed to do ReadFull, err=", err, "n=", n, "size=", fileSize)
	}

	for _, extent := range ei {
		phyOffset := int64(extent.Physical)
		length := int64(extent.Length)

		/* last extent which is unaligned */
		if remaining-length < 0 {
			length = int64(math.Ceil(float64(remaining)/float64(4096))) * 4096

			devFile, err := os.OpenFile(devPath, os.O_WRONLY|syscall.O_DSYNC, 0666)
			if err != nil {
				fmt.Println("[ERROR] Failed to open dev file:", err)
				return err
			}
			defer devFile.Close()

			n, err := syscall.Pwrite(int(devFile.Fd()), totalBuf[offset:offset+length], phyOffset)
			if err != nil {
				fmt.Println("[ERROR] Failed to Pwrite:", err, "written=", n)
				return err
			}
			return nil
		}

		n, err := syscall.Pwrite(fd, totalBuf[offset:offset+length], phyOffset)
		if err != nil {
			fmt.Println("[ERROR] Failed to Pwrite:", err, "written=", n)
			return err
		}
		remaining -= length
		offset += length
	}

	return nil
}

func readChunk(reader io.Reader, chunkSize int) ([]byte, error) {
	buf := make([]byte, chunkSize)
	n, err := io.ReadFull(reader, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	return buf[:n], nil
}

func PwriteAlignedChunk(fd int, fileSize int64, ei []ExtentInfo, reader io.Reader) error {
	for _, extent := range ei {
		phyOffset := int64(extent.Physical)
		length := int64(extent.Length)

		remaining := length
		for remaining > 0 {
			chunkSize := int(math.Min(float64(remaining), float64(blockSize)))
			chunk, err := readChunk(reader, chunkSize)
			if err != nil {
				return err
			}
			n, err := syscall.Pwrite(fd, chunk, phyOffset)
			if err != nil {
				return err
			}
			remaining -= int64(n)
			phyOffset += int64(n)
		}
	}
	return nil
}

func PwriteAligned(fd int, fileSize int64, ei []ExtentInfo, reader io.Reader, isParity bool) error {
	offset := int64(0)
	remaining := int64(fileSize)

	alignedSize := func(size int) int {
		const blockSize = 4096
		if size%blockSize == 0 {
			return size
		}
		return ((size / blockSize) + 1) * blockSize
	}(int(fileSize))

	var totalBuf []byte
	if writeSyncPoolEnable {
		if alignedSize < MinExtentChunkSize || alignedSize > MaxExtentChunkSize {
			totalBuf = make([]byte, alignedSize)
		} else {
			totalBuf = GetExtentBufferChunk(int64(alignedSize))
			defer PutExtentBufferChunk(totalBuf)
		}
	} else {
		totalBuf = make([]byte, alignedSize)
	}

	/* XXX: must be performed */
	n, err := io.ReadFull(reader, totalBuf[:fileSize])
	if err != nil {
		//if isParity && err == io.EOF {
		if globalParityFreeWrite && err == io.EOF {
			return nil
		}
		fmt.Println("[ERROR] PwriteAligned: Failed to do ReadFull, err=", err, "n=", n, "size=", fileSize, "isParity=", isParity)
		return err
	}

	for _, extent := range ei {
		phyOffset := int64(extent.Physical)
		length := int64(extent.Length)

		if remaining-length < 0 {
			//length = remaining
			length = int64(math.Ceil(float64(remaining)/float64(4096))) * 4096
		}

		n, err := syscall.Pwrite(fd, totalBuf[offset:offset+length], phyOffset)
		if err != nil {
			fmt.Println("[ERROR] Failed to Pwrite:", err, "written=", n)
			return err
		}
		remaining -= length
		offset += length

		//fmt.Printf("[INFO] PwriteAligned, offset=%d, length=%d, phyOffset=%d\n",
		//	offset, length, phyOffset)
	}

	return nil
}

func PwriteAligned2(fd int, fileSize int64, ei []ExtentInfo, reader io.Reader) error {
	remaining := int64(fileSize)

	for _, extent := range ei {
		phyOffset := int64(extent.Physical)
		length := int64(extent.Length)
		var buf []byte
		var n int
		var err error

		if remaining-length < 0 {
			length = int64(math.Ceil(float64(remaining)/float64(4096))) * 4096
			buf = make([]byte, length)
			n, err = io.ReadFull(reader, buf[:remaining])
		} else {
			if length < MinExtentChunkSize || length > MaxExtentChunkSize {
				buf = make([]byte, length)
			} else {
				buf = GetExtentBufferChunk(length)
				defer PutExtentBufferChunk(buf)
			}
			n, err = io.ReadFull(reader, buf)
		}
		if err != nil {
			fmt.Println("[ERROR] PwriteAligned2: Failed to do ReadFull, err=", err, "n=", n, "size=", fileSize)
		}

		if true {
			var wn int
			//defWriteSize := 32 * 1024
			defWriteSize := 512 * 1024
			offset := int64(0)
			nextPhyOffset := phyOffset
			numIters := int(math.Ceil(float64(n) / float64(defWriteSize)))
			rem := int64(len(buf))

			writeSize := int64(defWriteSize)

			for i := 0; i < numIters; i++ {
				if rem-writeSize < 0 {
					writeSize = int64(math.Ceil(float64(rem)/float64(4096))) * 4096
				}

				//fmt.Println("[INFO] i=", i, "rem=", rem, "writeSize=", writeSize, "offset=", offset)
				wn, err = syscall.Pwrite(fd, buf[offset:offset+writeSize], nextPhyOffset)
				if err != nil {
					fmt.Println("[ERROR] Failed to Pwrite:", err, "written=", wn)
					return err
				}

				offset += int64(wn)
				nextPhyOffset += int64(wn)
				rem -= int64(wn)
			}

		} else {
			//startTS := time.Now()
			n, err = syscall.Pwrite(fd, buf, phyOffset)
			if err != nil {
				fmt.Println("[ERROR] Failed to Pwrite:", err, "written=", n)
				return err
			}
			//elapsed := time.Since(startTS).Microseconds()
			//fmt.Println(time.Now(), "[INFO] Pwrite() write=", n, "elapsed(us)=", elapsed)
		}

		remaining -= length
	}

	return nil
}

func PwriteCopyAligned(fd int, fileSize int64, ei []ExtentInfo, reader io.Reader) error {
	var bufp *[]byte

	if fileSize > 0 && fileSize >= largestFileThreshold {
		// use a larger 4MiB buffer for a really large streams.
		bufp = xioutil.ODirectPoolXLarge.Get().(*[]byte)
		defer xioutil.ODirectPoolXLarge.Put(bufp)
	} else {
		bufp = xioutil.ODirectPoolLarge.Get().(*[]byte)
		defer xioutil.ODirectPoolLarge.Put(bufp)
	}

	alignedBuf := *bufp
	totalRemaining := fileSize
	numExtents := len(ei)
	alignedBufSize := int64(len(alignedBuf))

	for i, extent := range ei {
		phyOffset := int64(extent.Physical)
		extentSize := int64(extent.Length)

		var written int64
		for {
			buf := alignedBuf
			remaining := extentSize - written

			if fileSize > alignedBufSize {
				if remaining < int64(len(buf)) {
					if i == numExtents-1 {
						if totalRemaining < remaining {
							buf = buf[:totalRemaining]
						} else {
							buf = buf[:remaining]
						}
					} else {
						buf = buf[:remaining]
					}
				}
			} else {
				if numExtents == 1 {
					buf = buf[:fileSize]
				} else {
					if i == numExtents-1 {
						if totalRemaining < remaining {
							buf = buf[:totalRemaining]
						} else {
							buf = buf[:remaining]
						}
					} else {
						buf = buf[:extentSize]
					}
				}
			}

			//var startTS time.Time
			//startTS = time.Now()
			nr, err := io.ReadFull(reader, buf) // overhead 512KB ~= 5ms
			//elapsed := time.Since(startTS).Microseconds()
			//fmt.Println(time.Now(), "[INFO] ReadFull size=", len(buf), "elapsed(us)=", elapsed)
			if err != nil {
				fmt.Println("[ERROR] Failed to ReadFull:", err, "nr=", nr,
					"totalRemaining=", totalRemaining, "remaining=", remaining,
					"extentSize=", extentSize, "bufSize", len(buf))
				return err
			}

			if len(buf)%4096 > 0 {
				origSize := len(buf)
				alignedSize := (origSize/4096 + 1) * 4096
				buf = buf[:alignedSize]
				//fmt.Println("needAlign, origSize", origSize, "after bufSize=", len(buf))
			}
			if len(buf)%4096 > 0 {
				fmt.Println("[ERROR] PwriteCopyAligned: buf is not page aligned",
					"totalRemaining=", totalRemaining, "remaining=", remaining,
					"extentSize=", extentSize, "written=", written,
					"alignedBufSize=", alignedBufSize, "bufSize=", len(buf))
				return fmt.Errorf("buf is not aligned")
			}

			n, err := syscall.Pwrite(fd, buf, phyOffset)
			if err != nil {
				fmt.Println("[ERROR] Failed to Pwrite:", err, "written=", n)
				return err
			}

			phyOffset += int64(n)
			written += int64(n)
			totalRemaining -= int64(n)

			//fmt.Println("totalRemaining=", totalRemaining,
			//	"remaining=", remaining, "extentSize=", extentSize,
			//	"written=", written, "bufSize=", len(buf))

			if written == extentSize || totalRemaining <= 0 {
				break
			}
		}
	}

	return nil
}

/* XXX: buffer append is bad, rather allocate part size at once */
func PreadAligned(fd int, fileSize int64, ei []ExtentInfo, offset, length int64) (io.ReadCloser, error) {
	var curIdx int64
	totalSize := 0

	for _, extent := range ei {
		totalSize += int(extent.Length)
	}

	var totalBuf []byte
	if !readSyncPoolEnable {
		totalBuf = make([]byte, totalSize)
	} else {
		if totalSize < MinExtentChunkSize || totalSize > MaxExtentChunkSize {
			totalBuf = make([]byte, totalSize)
		} else {
			totalBuf = GetExtentBufferChunk(int64(totalSize))
			defer PutExtentBufferChunk(totalBuf)
		}
	}

	for _, extent := range ei {
		phyOffset := int64(extent.Physical)
		extentSize := int64(extent.Length)

		n, err := syscall.Pread(fd, totalBuf[curIdx:curIdx+extentSize], phyOffset)
		if err != nil {
			fmt.Println("[ERROR] Failed to Pread:", err, "read=", n)
			return nil, err
		}
		curIdx += extentSize
	}

	//fmt.Println("[INFO] PreadAligned, buf=", totalBuf)
	readCloser := ioutil.NopCloser(bytes.NewBuffer(totalBuf[:fileSize]))

	return readCloser, nil
}

// handling offset, length, TODO!! not working
func PreadAligned3(fd int, fileSize int64, ei []ExtentInfo, offset, length int64) (io.ReadCloser, error) {
	curIdx := int64(0)
	readAll := true
	totalSize := int64(0)
	var readCloser io.ReadCloser

	for _, extent := range ei {
		totalSize += int64(extent.Length)
	}

	// partial read
	if offset != int64(0) {
		readAll = false
	}

	// buffer allocation
	var totalBuf []byte
	totalBuf = make([]byte, totalSize)
	if totalBuf == nil {
		fmt.Printf("[ERROR] Failed to allocate totalBuf")
	}

	if readAll {
		remaining := fileSize
		for i, extent := range ei {
			phyOffset := int64(extent.Physical)
			extentSize := int64(extent.Length)

			if i == len(ei)-1 {
				extentSize = int64(math.Ceil(float64(remaining)/float64(4096))) * 4096
				//fmt.Println("[INFO] extentSize=", extentSize)
			}
			//fmt.Printf("[INFO] PreadAligned3 readAll offset=%d, length=%d, curIdx=%d, extentSize=%d, totalSize=%d, phyOffset=%d\n",
			//offset, length, curIdx, extentSize, totalSize, phyOffset)

			n, err := syscall.Pread(fd, totalBuf[curIdx:curIdx+extentSize], phyOffset)
			if err != nil {
				fmt.Println("[ERROR] Failed to Pread:", err, "read=", n)
				return nil, err
			}
			curIdx += extentSize
			remaining -= int64(n)
		}

		//fmt.Printf("[INFO] buf=%v\n", totalBuf[:16])
		readCloser = ioutil.NopCloser(bytes.NewBuffer(totalBuf[:fileSize]))
	} else {
		extentOffset := int64(0)
		startUnaligned := int64(0)
		startExtent := 0
		bufIdx := int64(0)
		remaining := length

		// find start extent
		for i, extent := range ei {
			extentSize := int64(extent.Length)
			if offset >= extentOffset && offset < extentOffset+extentSize {
				startExtent = i
				break
			}
			extentOffset += extentSize
		}

		for i := startExtent; i < len(ei); i++ {
			alignedOffset := int64(0)
			lenToRead := int64(0)
			extentSize := int64(ei[i].Length)
			phyOffset := int64(0)

			if i == startExtent {
				if offset%4096 == 0 {
					alignedOffset = offset
				} else {
					alignedOffset = offset - (offset % 4096)
					startUnaligned = (offset % 4096)
				}
				phyOffset = int64(ei[i].Physical) + (alignedOffset - extentOffset)
			} else {
				alignedOffset = extentOffset
				phyOffset = int64(ei[i].Physical)
			}

			alignedLen := (extentOffset + extentSize) - alignedOffset

			if remaining > alignedLen {
				lenToRead = alignedLen
			} else {
				// 7/4 = 2.xx -> 3
				lenToRead = int64(math.Ceil(float64(remaining)/float64(4096))) * 4096
				///* TODO: more understandable logic */
				if offset+length > alignedOffset+lenToRead {
					//fmt.Printf("[INFO] offset=%d, length=%d, alignedOffset=%d, lenToRead=%d, remaining=%d\n",
					//	offset, length, alignedOffset, lenToRead, remaining)
					lenToRead += 4096
				}
			}

			//fmt.Printf("[INFO] offset=%d, bufIdx=%d, lenToRead=%d, remaining=%d, alignedLen=%d\n", offset, bufIdx, lenToRead, remaining, alignedLen)

			n, err := syscall.Pread(fd, totalBuf[bufIdx:bufIdx+lenToRead], phyOffset)
			if err != nil {
				fmt.Println("[ERROR] Failed to Pread:", err, "read=", n)
				return nil, err
			}

			bufIdx += lenToRead
			extentOffset += lenToRead
			remaining -= lenToRead

			if remaining <= 0 {
				break
			}
		}
		readCloser = ioutil.NopCloser(bytes.NewBuffer(totalBuf[startUnaligned : startUnaligned+length]))
	}
	return readCloser, nil
}

// XXX: bad performance...
func PreadAligned2(fd int, fileSize int64, ei []ExtentInfo) (io.ReadCloser, error) {
	var totalBuf []byte

	for _, extent := range ei {
		phyOffset := int64(extent.Physical)
		extentSize := int64(extent.Length)
		var buf []byte

		if !readSyncPoolEnable {
			buf = make([]byte, extentSize)
		} else {
			if extentSize < MinExtentChunkSize || extentSize > MaxExtentChunkSize {
				buf = make([]byte, extentSize)
			} else {
				buf = GetExtentBufferChunk(extentSize)
				defer PutExtentBufferChunk(buf)
			}
		}

		n, err := syscall.Pread(fd, buf[:extentSize], phyOffset)
		if err != nil {
			fmt.Println("[ERROR] Failed to Pread:", err, "read=", n)
			return nil, err
		}
		totalBuf = append(totalBuf, buf[:extentSize]...)
	}

	readCloser := ioutil.NopCloser(bytes.NewBuffer(totalBuf[:fileSize]))

	return readCloser, nil
}

func PreadXLAligned(fd int, fileSize int64, ei []ExtentInfo, mtime time.Time) (RawFileInfo, error) {
	var rf RawFileInfo
	var curIdx int64
	totalSize := 0

	for _, extent := range ei {
		totalSize += int(extent.Length)
	}

	totalBuf := make([]byte, totalSize)

	for _, extent := range ei {
		phyOffset := int64(extent.Physical)
		extentSize := int64(extent.Length)

		n, err := syscall.Pread(fd, totalBuf[curIdx:curIdx+extentSize], phyOffset)
		if err != nil {
			fmt.Println("[ERROR] Failed to Pread:", err, "read=", n)
			return rf, err
		}
		curIdx += extentSize
	}

	rf = RawFileInfo{
		Buf:       totalBuf[:fileSize],
		DiskMTime: mtime,
	}

	return rf, nil
}

func init() {
}

func FdatasyncAll(tmpDirPath, tgtDataDirPath string) {
	dir, err := os.Open(tmpDirPath)
	if err != nil {
		return
		//log.Fatalf("[ERROR] Failed to open dir=%v", err)
	}
	defer dir.Close()

	fileInfos, err := dir.Readdir(-1)
	if err != nil {
		log.Fatalf("[ERROR] Failed to read dir=%v", err)
	}

	for _, fileInfo := range fileInfos {
		filePath := pathJoin(tmpDirPath, fileInfo.Name())
		//file, err := os.Open(filePath)
		file, err := directio.OpenFile(filePath, os.O_WRONLY, 0666)

		if err != nil {
			/* tmp file may already be renamed...: it may affect recovery */
			//log.Printf("[WARN] Failed to open file=%v, it may already be renamed", err)
			if enableAsyncFdatasync {
				filePath = pathJoin(tgtDataDirPath, fileInfo.Name())
				//file, err = os.Open(filePath)
				file, err = directio.OpenFile(filePath, os.O_WRONLY, 0666)
				if err != nil {
					/* object may be deleted */
					//log.Printf("[WARN] Failed to open file=%v, it may be deleted", err)
					continue
				}
			} else {
				continue
			}
		}
		defer file.Close()

		disk.Fdatasync(file)

		//fmt.Println("[INFO] fdatasync file=", fileInfo.Name())
	}
}

/*******************************************************************************/

func (s *xlRawStorage) openFileSyncSDFS(filePath string, mode int) (f *os.File, err error) {
	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(pathutil.Dir(filePath), 0o777); err != nil {
		return nil, osErrToFileErr(err)
	}

	w, err := OpenFile(filePath, mode|writeMode, 0o666)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		switch {
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrIO(err):
			return nil, errFaultyDisk
		case isSysErrTooManyFiles(err):
			return nil, errTooManyOpenFiles
		default:
			return nil, err
		}
	}

	return w, nil
}

// CreateFile - creates the file.
func (s *xlRawStorage) CreateFileSDFS(ctx context.Context, volume, path string, fileSize int64, r io.Reader, bucket, object string) (err error) {
	if fileSize < -1 {
		return errInvalidArgument
	}

	//volumeDir, err := s.getVolDir(volume)
	//if err != nil {
	//	return nil, err
	//
	volumeDir := pathJoin(s.mntpnt, volume)

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	parentFilePath := pathutil.Dir(filePath)
	defer func() {
		if err != nil {
			if volume == minioMetaTmpBucket {
				// only cleanup parent path if the
				// parent volume name is minioMetaTmpBucket
				removeAll(parentFilePath)
			}
		}
	}()

	if fileSize >= 0 && fileSize <= smallFileThreshold {
		// For streams smaller than 128KiB we simply write them as O_DSYNC (fdatasync)
		// and not O_DIRECT to avoid the complexities of aligned I/O.
		w, err := s.openFileSyncSDFS(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL)
		if err != nil {
			return err
		}
		defer w.Close()

		written, err := io.Copy(w, r)
		if err != nil {
			return osErrToFileErr(err)
		}

		if written < fileSize {
			return errLessData
		} else if written > fileSize {
			return errMoreData
		}

		return nil
	}

	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(parentFilePath, 0o777); err != nil {
		return osErrToFileErr(err)
	}

	w, err := OpenFileDirectIO(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o666)
	if err != nil {
		return osErrToFileErr(err)
	}

	defer func() {
		disk.Fdatasync(w) // Only interested in flushing the size_t not mtime/atime
		w.Close()
	}()

	var bufp *[]byte
	if fileSize > 0 && fileSize >= largestFileThreshold {
		// use a larger 4MiB buffer for a really large streams.
		bufp = xioutil.ODirectPoolXLarge.Get().(*[]byte)
		defer xioutil.ODirectPoolXLarge.Put(bufp)
	} else {
		bufp = xioutil.ODirectPoolLarge.Get().(*[]byte)
		defer xioutil.ODirectPoolLarge.Put(bufp)
	}

	//fmt.Println("[INFO] CreateFileSDFS, filePath=", filePath, "fileSize=", fileSize, "len(*bufp)=", len(*bufp))

	written, err := xioutil.CopyAligned(diskHealthWriter(ctx, w), r, *bufp, fileSize, w)
	if err != nil {
		return err
	}

	if written < fileSize && fileSize >= 0 {
		return errLessData
	} else if written > fileSize && fileSize >= 0 {
		return errMoreData
	}

	return nil
}

// ReadFileStream - Returns the read stream of the file.
func (s *xlRawStorage) ReadFileStreamSDFS(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, errInvalidArgument
	}

	//volumeDir, err := s.getVolDir(volume)
	//if err != nil {
	//	return nil, err
	//}
	volumeDir := pathJoin(s.mntpnt, volume)
	var err error

	// Validate effective path length before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return nil, err
	}

	//fmt.Printf("[INFO] ReadFileStreamSDFS, volumeDir=%s, volume=%s, path=%s, filePath=%s, offset=%d, length=%d\n",
	//	volumeDir, volume, path, filePath, offset, length)

	file, err := OpenFileDirectIO(filePath, readMode, 0o666)
	if err != nil {
		switch {
		case osIsNotExist(err):
			if err = Access(volumeDir); err != nil && osIsNotExist(err) {
				return nil, errVolumeNotFound
			}
			return nil, errFileNotFound
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrNotDir(err):
			return nil, errFileAccessDenied
		case isSysErrIO(err):
			return nil, errFaultyDisk
		case isSysErrTooManyFiles(err):
			return nil, errTooManyOpenFiles
		case isSysErrInvalidArg(err):
			return nil, errUnsupportedDisk
		default:
			return nil, err
		}
	}

	st, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	// Verify it is a regular file, otherwise subsequent Seek is
	// undefined.
	if !st.Mode().IsRegular() {
		file.Close()
		return nil, errIsNotRegular
	}

	if st.Size() < offset+length {
		// Expected size cannot be satisfied for
		// requested offset and length
		file.Close()
		return nil, errFileCorrupt
	}

	alignment := offset%xioutil.DirectioAlignSize == 0
	if !alignment || globalAPIConfig.isDisableODirect() {
		if err = disk.DisableDirectIO(file); err != nil {
			file.Close()
			return nil, err
		}
	}

	if offset > 0 {
		if _, err = file.Seek(offset, io.SeekStart); err != nil {
			file.Close()
			return nil, err
		}
	}

	or := &xioutil.ODirectReader{
		File:      file,
		SmallFile: false,
	}

	if length <= smallFileThreshold {
		or = &xioutil.ODirectReader{
			File:      file,
			SmallFile: true,
		}
	}

	r := struct {
		io.Reader
		io.Closer
	}{Reader: io.LimitReader(diskHealthReader(ctx, or), length), Closer: closeWrapper(func() error {
		if !alignment || offset+length%xioutil.DirectioAlignSize != 0 {
			// invalidate page-cache for unaligned reads.
			if !globalAPIConfig.isDisableODirect() {
				// skip removing from page-cache only
				// if O_DIRECT was disabled.
				disk.FadviseDontNeed(file)
			}
		}
		return or.Close()
	})}

	return r, nil
}

func (s *xlRawStorage) readAllData(ctx context.Context, volumeDir string, filePath string) (buf []byte, dmTime time.Time, err error) {
	if contextCanceled(ctx) {
		return nil, time.Time{}, ctx.Err()
	}

	f, err := OpenFileDirectIO(filePath, readMode, 0o666)
	if err != nil {
		//fmt.Println("[INFO] readAllData orig err=", err)
		if osIsNotExist(err) {
			// Check if the object doesn't exist because its bucket
			// is missing in order to return the correct error.
			if err = Access(volumeDir); err != nil && osIsNotExist(err) {
				return nil, dmTime, errVolumeNotFound
			}
			return nil, dmTime, errFileNotFound
		} else if osIsPermission(err) {
			return nil, dmTime, errFileAccessDenied
		} else if isSysErrNotDir(err) || isSysErrIsDir(err) {
			return nil, dmTime, errFileNotFound
		} else if isSysErrHandleInvalid(err) {
			// This case is special and needs to be handled for windows.
			return nil, dmTime, errFileNotFound
		} else if isSysErrIO(err) {
			return nil, dmTime, errFaultyDisk
		} else if isSysErrTooManyFiles(err) {
			return nil, dmTime, errTooManyOpenFiles
		} else if isSysErrInvalidArg(err) {
			st, _ := Lstat(filePath)
			if st != nil && st.IsDir() {
				// Linux returns InvalidArg for directory O_DIRECT
				// we need to keep this fallback code to return correct
				// errors upwards.
				return nil, dmTime, errFileNotFound
			}
			return nil, dmTime, errUnsupportedDisk
		}
		return nil, dmTime, err
	}
	r := &xioutil.ODirectReader{
		File:      f,
		SmallFile: true,
	}
	defer r.Close()

	// Get size for precise allocation.
	stat, err := f.Stat()
	if err != nil {
		buf, err = ioutil.ReadAll(r)
		return buf, dmTime, osErrToFileErr(err)
	}
	if stat.IsDir() {
		return nil, dmTime, errFileNotFound
	}

	// Read into appropriate buffer.
	sz := stat.Size()
	if sz <= metaDataReadDefault {
		buf = metaDataPoolGet()
		buf = buf[:sz]
	} else {
		buf = make([]byte, sz)
	}
	// Read file...
	_, err = io.ReadFull(diskHealthReader(ctx, r), buf)

	return buf, stat.ModTime().UTC(), osErrToFileErr(err)
}

func (s *xlRawStorage) readRaw(ctx context.Context, volumeDir, filePath string, readData bool) (buf []byte, dmTime time.Time, err error) {
	if readData {
		buf, dmTime, err = s.readAllData(ctx, volumeDir, pathJoin(filePath, xlStorageFormatFile))
	} else {
		/*
			buf, dmTime, err = s.readMetadataWithDMTime(ctx, pathJoin(filePath, xlStorageFormatFile))
			if err != nil {
				if osIsNotExist(err) {
					if aerr := Access(volumeDir); aerr != nil && osIsNotExist(aerr) {
						return nil, time.Time{}, errVolumeNotFound
					}
				}
				err = osErrToFileErr(err)
			}
		*/
	}

	if err != nil {
		if err == errFileNotFound {
			buf, dmTime, err = s.readAllData(ctx, volumeDir, pathJoin(filePath, xlStorageFormatFileV1))
			if err != nil {
				return nil, time.Time{}, err
			}
		} else {
			return nil, time.Time{}, err
		}
	}

	if len(buf) == 0 {
		fmt.Println("[INFO] len(buf)=0 err=", err)
		return nil, time.Time{}, errFileNotFound
	}

	return buf, dmTime, nil
}

// ReadXL reads from path/xl.meta, does not interpret the data it read. This
// is a raw call equivalent of ReadVersion().
func (s *xlRawStorage) ReadXLSDFS(ctx context.Context, volume, path string, readData bool) (RawFileInfo, error) {
	//volumeDir, err := s.getVolDir(volume)
	//if err != nil {
	//	return RawFileInfo{}, err
	//}
	volumeDir := pathJoin(s.mntpnt, volume)
	var err error

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return RawFileInfo{}, err
	}

	buf, dmTime, err := s.readRaw(ctx, volumeDir, filePath, readData)
	return RawFileInfo{
		Buf:       buf,
		DiskMTime: dmTime,
	}, err
}
