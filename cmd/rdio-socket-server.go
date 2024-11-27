package cmd

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	pathutil "path"
	"strconv"
	"time"
)

const (
	tmpVolume = ".minio.sys/tmp"
)

type storageSocketServer struct {
	storage *xlStorage
}

func (s *storageSocketServer) WriteResponse(conn net.Conn, res *RawIOResponse) error {
	resBytes, err := res.MarshalMsg(nil)
	if err != nil {
		fmt.Println("[ERROR] Failed to encode request", err)
		return err
	}

	if false {
		alignedBytes := make([]byte, MaxMsgSize)
		copy(alignedBytes, resBytes)
		_, err = conn.Write(alignedBytes)
	} else {
		_, err = conn.Write(resBytes)
		//fmt.Println("[INFO] len=", len(resBytes))
	}
	if err != nil {
		fmt.Println("[ERROR] Failed to write response to connection:", err)
		return err
	}

	return nil
}

func (s *storageSocketServer) CreateFileHandler(conn net.Conn, req *Request) error {
	var res RawIOResponse

	filePath := req.FilePath
	fileSize := int64(req.FileSize)

	tmpBucketId := strconv.Itoa(getObj2MapShardId(req.Object))
	volumeDir, err := s.storage.getVolDir(tmpVolume + tmpBucketId)
	if err != nil {
		fmt.Println("[ERROR] Failed to get volume dir")
		s.WriteResponse(conn, &res)
		return err
	}
	fullFilePath := pathJoin(volumeDir, filePath)
	parentFilePath := pathutil.Dir(fullFilePath)

	defer func() {
		if err != nil {
			if (tmpVolume + tmpBucketId) == minioMetaTmpBucket {
				// only cleanup parent path if the
				// parent volume name is minioMetaTmpBucket
				removeAll(parentFilePath)
				fmt.Println("[WARN] removeAll parentFilePath=", parentFilePath)
			}
		}
	}()

	if err = mkdirAll(parentFilePath, 0o777); err != nil {
		s.WriteResponse(conn, &res)
		fmt.Println("[ERROR] Failed to create parentFilePath=", parentFilePath, "err=", err)
		return err
	}

	err = AllocateFileAndGetExtents(fullFilePath, fileSize, &res.Extents)
	if err != nil {
		return err
	}

	err = s.WriteResponse(conn, &res)
	if err != nil {
		return err
	}

	return nil
}

func (s *storageSocketServer) ReadFileStreamHandler(conn net.Conn, req *Request) error {
	var res RawIOResponse
	var err error
	var volumeDir string

	filePath := req.FilePath

	volumeDir, err = s.storage.getVolDir(req.Volume)
	if err != nil {
		fmt.Println("[ERROR] Failed to get volume dir")
		s.WriteResponse(conn, &res)
		return err
	}
	fullFilePath := pathJoin(volumeDir, filePath)

	/* 1. get extents info */
	_, err = GetExtents(fullFilePath, false, &res.Extents)
	if err != nil {
		s.WriteResponse(conn, &res)
		return err
	}
	//fmt.Println("fullFilePath=", fullFilePath)
	fileInfo, err := os.Stat(fullFilePath)
	if err != nil {
		fmt.Println("[ERROR] Failed to stat file:", err)
		s.WriteResponse(conn, &res)
		return err
	}
	res.FileSize = int(fileInfo.Size())

	/* 2. reply */
	err = s.WriteResponse(conn, &res)
	if err != nil {
		return err
	}

	return nil
}

func (s *storageSocketServer) ReadXLHandler(conn net.Conn, req *Request) error {
	var res RawIOResponse
	var err error
	var volumeDir string

	filePath := pathJoin(req.FilePath, xlStorageFormatFile)

	volumeDir, err = s.storage.getVolDir(req.Volume)
	if err != nil {
		res.Err = err.Error()
		fmt.Println("[ERROR] Failed to get volume dir")
		s.WriteResponse(conn, &res)
		return err
	}
	fullFilePath := pathJoin(volumeDir, filePath)

	//fmt.Println("[INFO] ReadXLHandler err", err, volumeDir, filePath, fullFilePath)
	/* 1. get extents info */
	_, err = GetExtents(fullFilePath, false, &res.Extents)
	if err != nil {
		/* XXX: logic from readRaw() */
		if err == errFileNotFound {
			filePath = pathJoin(req.FilePath, xlStorageFormatFileV1)
			fullFilePath = pathJoin(volumeDir, filePath)

			//fmt.Println("[INFO] ReadXLHandler retry err", err, fullFilePath)
			_, err = GetExtents(fullFilePath, false, &res.Extents)
			if err != nil {
				res.Err = err.Error()
				s.WriteResponse(conn, &res)
				return err
			}
		} else {
			res.Err = err.Error()
			s.WriteResponse(conn, &res)
			return err
		}
	}

	fileInfo, err := os.Stat(fullFilePath)
	if err != nil {
		res.Err = err.Error()
		fmt.Println("[ERROR] Failed to stat file:", err)
		s.WriteResponse(conn, &res)
		return err
	}
	res.FileSize = int(fileInfo.Size())
	res.MTime = fileInfo.ModTime().UTC()

	//fmt.Println("[INFO] size=", res.FileSize, "mtime=", res.MTime)
	/* 2. reply */
	err = s.WriteResponse(conn, &res)
	if err != nil {
		return err
	}

	if globalMappingCacheEnable && req.Volume == "clear-mapping-cache" {
		//fmt.Println(volumeDir)
		if volumeDir == "/mnt/minio0/data/clear-mapping-cache" {
			for _, value := range globalRESTClientMap {
				client := value
				client.rawStorage.cache.Purge()
				client.rawStorage.metaCache.Purge()
				//fmt.Println("[INFO] purge mapping cache", client.endpoint)

			}
		}
	}

	return nil
}

func allocateWorker(dirPath string, req *Request, jobs <-chan int, results chan<- FileSlabExtentInfo) {
	for i := range jobs {
		fileName := strconv.Itoa(i)
		shardId := i % numObj2SlabMapShards
		shardedDirPath := pathJoin(dirPath, strconv.Itoa(shardId))
		fullFilePath := pathJoin(shardedDirPath, fileName)

		var extents []ExtentInfo
		err := AllocateFileAndGetExtents(fullFilePath, int64(req.FileSize), &extents)
		if err != nil {
			return
		}

		//fmt.Printf("[INFO] dirPath=%s, fullFilePath=%s\n", shardedDirPath, fullFilePath)

		fsei := FileSlabExtentInfo{Extents: extents, FileSize: req.FileSize}
		results <- fsei
	}
}

func (s *storageSocketServer) FileSlabHandler(conn net.Conn, req *Request) error {
	var res FileSlabResponse

	/* /mnt/minio0/data/apache1 */
	dirPath := pathJoin(s.storage.diskPath, req.FilePath)
	_, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		if err := mkdirAll(dirPath, 0o777); err != nil {
			fmt.Println("[ERROR] Failed to create dirPath=", dirPath, "err=", err)
			return err
		}
	}

	/* /mnt/minio0/data/apache1/8192 */
	dirPath = pathJoin(dirPath, strconv.Itoa(req.FileSize))
	_, err = os.Stat(dirPath)
	if os.IsNotExist(err) {
		if err := mkdirAll(dirPath, 0o777); err != nil {
			fmt.Println("[ERROR] Failed to create dirPath=", dirPath, "err=", err)
			return err
		}
	}

	for i := 0; i < numObj2SlabMapShards; i++ {
		/* /mnt/minio0/data/apache1/8192/{0-255} */
		shardedDirPath := pathJoin(dirPath, strconv.Itoa(i))
		_, err = os.Stat(shardedDirPath)
		if os.IsNotExist(err) {
			if err := mkdirAll(shardedDirPath, 0o777); err != nil {
				fmt.Println("[ERROR] Failed to create dirPath=", shardedDirPath, "err=", err)
				return err
			}
		}
	}

	startId := int(req.Id)
	if false {
		for i := startId; i < startId+int(req.Len); i++ {
			fileName := strconv.Itoa(i)
			fullFilePath := pathJoin(dirPath, fileName)

			var extents []ExtentInfo
			err := AllocateFileAndGetExtents(fullFilePath, int64(req.FileSize), &extents)
			if err != nil {
				return err
			}

			//fmt.Println("[INFO] fullFilePath=", fullFilePath, i, startId+int(req.Len))
			fsei := FileSlabExtentInfo{Extents: extents, FileSize: req.FileSize}
			res.Fsei = append(res.Fsei, fsei)
		}
	} else {
		numJobs := int(req.Len)
		jobs := make(chan int, numJobs)
		results := make(chan FileSlabExtentInfo, numJobs)
		//numWorkers := runtime.NumCPU()
		numWorkers := 1

		for w := 0; w < numWorkers; w++ {
			go allocateWorker(dirPath, req, jobs, results)
		}

		for i := startId; i < startId+numJobs; i++ {
			jobs <- i
		}

		for i := startId; i < startId+numJobs; i++ {
			result := <-results
			res.Fsei = append(res.Fsei, result)
		}
	}

	/* 2. reply */
	resBytes, err := res.MarshalMsg(nil)
	if err != nil {
		fmt.Println("[ERROR] Failed to encode request", err)
		return err
	}

	if len(resBytes) > 1<<20 {
		fmt.Println("[INFO] len(resBytes)", len(resBytes))
	}

	_, err = conn.Write(resBytes)
	if err != nil {
		fmt.Println("[ERROR] Failed to write response to connection:", err)
		return err
	}

	return nil
}

func (s *storageSocketServer) MkdirHandler(conn net.Conn, req *Request) error {
	var res RawIOResponse

	filePath := req.FilePath

	volumeDir, err := s.storage.getVolDir(tmpVolume)
	if err != nil {
		fmt.Println("[ERROR] Failed to get volume dir")
		s.WriteResponse(conn, &res)
		return err
	}
	fullFilePath := pathJoin(volumeDir, filePath)
	parentFilePath := pathutil.Dir(fullFilePath)

	defer func() {
		if err != nil {
			if tmpVolume == minioMetaTmpBucket {
				// only cleanup parent path if the
				// parent volume name is minioMetaTmpBucket
				removeAll(parentFilePath)
				fmt.Println("[WARN] removeAll parentFilePath=", parentFilePath)
			}
		}
	}()

	if err = mkdirAll(parentFilePath, 0o777); err != nil {
		s.WriteResponse(conn, &res)
		fmt.Println("[ERROR] Failed to create parentFilePath=", parentFilePath, "err=", err)
		return err
	}

	err = s.WriteResponse(conn, &res)
	if err != nil {
		return err
	}

	return nil
}

func (s *storageSocketServer) handleRequest(conn net.Conn) error {
	//fmt.Println("[INFO] Client connected:", conn.RemoteAddr())

	reqBytes := make([]byte, MaxMsgSize)

	for {
		// Read data from the connection
		_, err := conn.Read(reqBytes)
		if err != nil {
			//fmt.Println("[ERROR] Failed to read data from connection:", err)
			return err
		}

		var req Request
		_, err = req.UnmarshalMsg(reqBytes)
		if err != nil {
			fmt.Println("[ERROR] Failed to decode Request:", err)
			return err
		}

		//fmt.Println("[INFO] handleRequest req=", req)
		msgType := int(req.Type)
		if msgType == MsgReadFileStream {
			s.ReadFileStreamHandler(conn, &req)
		} else if msgType == MsgCreateFile {
			s.CreateFileHandler(conn, &req)
		} else if msgType == MsgReadXL {
			s.ReadXLHandler(conn, &req)
		} else if msgType == MsgFileSlab {
			s.FileSlabHandler(conn, &req)
		} else if msgType == MsgMkdir {
			s.MkdirHandler(conn, &req)
		} else {
			fmt.Println("[ERROR] Unknown msgType:", msgType, err)
		}
	}

	fmt.Println("[ERROR] return handleRequest")

	return nil
}

func registerStorageSocketHandlers(storage *xlStorage) *storageSocketServer {
	server := &storageSocketServer{storage: storage}

	portNum := 0
	tmpPort := extractNumberWithRegex(storage.endpoint.Path)
	if tmpPort != -1 {
		portNum = BasePort + tmpPort
	} else {
		portNum = BasePort
	}

	addrPort := ":" + strconv.Itoa(portNum)

	listener, err := net.Listen("tcp", addrPort)
	if err != nil {
		fmt.Println("[ERROR] Failed to start socket server:", err)
		return nil
	}

	go func(server *storageSocketServer) {
		fmt.Println("[INFO] Server started. Waiting for connections...")

		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Println("[ERROR] Failed to accept connection:", err)
					time.Sleep(1 * time.Second)
					continue
				} else {
					fmt.Println("[ERROR] Failed to accept connection:", err)
					break
				}
			}

			go server.handleRequest(conn)
		}
	}(server)

	return server
}
