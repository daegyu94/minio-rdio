package cmd

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/frostschutz/go-fibmap"
)

//go:generate msgp -file=$GOFILE

type ExtentInfo struct {
	Physical int64
	Length   int32
}

/* private in go-fiemap: based on struct fiemap from <linux/fiemap.h> */
type fiemap struct {
	Start          uint64 // logical offset (inclusive) at which to start mapping (in)
	Length         uint64 // logical length of mapping which userspace wants (in)
	Flags          uint32 // FIEMAP_FLAG_* flags for request (in/out)
	Mapped_extents uint32 // number of extents that were mapped (out)
	Extent_count   uint32 // size of fm_extents array (in)
	Reserved       uint32
	// Extents [0]Extent // array of mapped extents (out)
}

func ShowExtents(ei *[]ExtentInfo) {
	for i, extent := range *ei {
		fmt.Printf("Extent=%d\n", i)
		fmt.Printf("Physical block address: %d\n", extent.Physical)
		fmt.Printf("Length: %d\n", extent.Length)
		fmt.Println("---------------------")
	}
}

func Fiemap(file *os.File, ei *[]ExtentInfo) error {
	/* step 1, get Mapped_extents */
	fm := make([]fiemap, 1)

	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(&fm[0])))

	t := (*fiemap)(ptr)
	t.Start = 0
	t.Length = fibmap.FIEMAP_MAX_OFFSET
	t.Flags = fibmap.FIEMAP_FLAG_SYNC
	t.Mapped_extents = 0
	t.Extent_count = 0

	_, _, err := syscall.Syscall(syscall.SYS_IOCTL, file.Fd(),
		fibmap.FS_IOC_FIEMAP, uintptr(ptr))
	if err != syscall.Errno(0) {
		fmt.Printf("[ERROR] Failed to get extent information: %v", err)
		return err
	}

	fibmapFile := fibmap.NewFibmapFile(file)
	/* step 2, do a real fiemap */
	extents, err := fibmapFile.Fiemap(uint32(t.Mapped_extents))
	if err != syscall.Errno(0) {
		fmt.Printf("[ERROR] Failed to get extent information: %v", err)
		return err
	}

	/* convert to simple Extent type */
	for _, extent := range extents {
		info := ExtentInfo{
			Physical: int64(extent.Physical),
			Length:   int32(extent.Length),
		}
		*ei = append(*ei, info)
	}

	return nil
}

func Fallocate(file *os.File, size int64) error {
	mode := uint32(0)
	offset := int64(0)

	err := syscall.Fallocate(int(file.Fd()), mode, offset, size)
	if err != nil {
		fmt.Println("[ERROR] Failed to fallocate file:", err)
		return err
	}

	return nil
}

func GetExtents(filePath string, needStat bool, ei *[]ExtentInfo) (int64, error) {
	flags := readMode
	file, err := os.OpenFile(filePath, flags, 0666)
	fileSize := int64(0)

	if err != nil {
		if osIsNotExist(err) {
			return fileSize, errFileNotFound
		} else {
			fmt.Println("[ERROR] Failed to open file:", err)
			return fileSize, err
		}
	}
	defer file.Close()

	if needStat {
		fileInfo, err := file.Stat()
		if err != nil {
			fmt.Printf("Failed to stat file: %v\n", err)
			return fileSize, err
		}
		fileSize = fileInfo.Size()
	}
	err = Fiemap(file, ei)

	return fileSize, err
}

func MountRdev(dev, dir string) error {
	flags := 0 // 마운트 플래그 (예: syscall.MS_RDONLY for read-only)
	opt := ""

	startTS := time.Now()
	err := syscall.Mount(dev, dir, "xfs", uintptr(flags), opt)
	if err != nil {
		fmt.Printf("Mount failed: %v\n", err)
	}
	elapsed := uint64(time.Since(startTS))
	fmt.Printf("[INFO] mount dev=%s, dir=%s, elapsed(us)=%d\n", dev, dir, elapsed/1000)
	return err
}

func UmountRdev(dir string) error {
	err := syscall.Unmount(dir, 0)
	if err != nil {
		fmt.Printf("Unmount failed: %v\n", err)
	}
	return err
}
