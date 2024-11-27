package cmd

import (
	"hash/fnv"
	"math"
	"math/bits"
	"math/rand"
	"sync"
)

const (
	fileSlabAligedSize = 4 << 10

	numFileSlabAllocatorShards = 40
	numObj2SlabMapShards       = 256
)

type FLValue struct {
	id      int
	extents []ExtentInfo
}

// Node represents an element in the queue
type Node struct {
	value interface{}
	next  *Node
}

// FreeList is a queue-like structure for reusing objects
type FreeList struct {
	head *Node
	tail *Node
	size int
	mu   sync.Mutex
	idx  int
}

// NewFreeList creates a new FreeList
func NewFreeList(i int) *FreeList {
	return &FreeList{idx: i}
}

// Enqueue adds an element to the FreeList
func (f *FreeList) Enqueue(value interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()

	newNode := &Node{value: value}
	if f.tail != nil {
		f.tail.next = newNode
	}
	f.tail = newNode
	if f.head == nil {
		f.head = newNode
	}
	f.size++
}

// Dequeue removes and returns an element from the FreeList
func (f *FreeList) Dequeue() interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.head == nil {
		return nil
	}

	value := f.head.value
	f.head = f.head.next
	if f.head == nil {
		f.tail = nil
	}
	f.size--

	return value
}

// Size returns the number of elements in the FreeList
func (f *FreeList) Size() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.size
}

// FileSlabAllocator manages multiple FreeLists of different sizes
type FileSlabAllocatorShard struct {
	freeLists []*FreeList // List of FreeLists
}

type FileSlabAllocator struct {
	shards []*FileSlabAllocatorShard
}

func getFileSlabAllocatorShardId() int {
	return rand.Int() % numFileSlabAllocatorShards
}

// NewFileSlabAllocator creates a new FileSlabAllocator with FreeLists of specified sizes
func NewFileSlabAllocatorShard() *FileSlabAllocatorShard {
	sizes := globalMaxFileSlabSize / fileSlabAligedSize
	freeLists := make([]*FreeList, sizes)

	for i := 0; i < sizes; i++ {
		freeLists[i] = NewFreeList(i)
	}

	return &FileSlabAllocatorShard{
		freeLists: freeLists,
	}
}

func NewFileSlabAllocator() *FileSlabAllocator {
	shards := make([]*FileSlabAllocatorShard, numFileSlabAllocatorShards)

	for i := 0; i < numFileSlabAllocatorShards; i++ {
		shards[i] = NewFileSlabAllocatorShard()
	}

	return &FileSlabAllocator{
		shards: shards,
	}
}

func ceilTo4096(n int) int {
	const unit = 4096
	if n%unit == 0 {
		return n
	}
	return ((n / unit) + 1) * unit
}

func nextPowerOfTwo(n int) int {
	if n <= 0 {
		return 1
	}

	// minimum size
	if n < 4096 {
		return 4096
	}

	msb := 1 << uint(bits.Len(uint(n))-1)
	if n&(n-1) == 0 {
		return n
	}
	return msb << 1
}

func alignSlabSize(n int) int {
	return nextPowerOfTwo(n)
}

// GetFreeList returns the FreeList corresponding to the given size
func (fsa *FileSlabAllocatorShard) GetFreeList(size int) *FreeList {
	roundedSize := 0
	if size < 4096 {
		roundedSize = 4096
	} else {
		roundedSize = alignSlabSize(size)
		if roundedSize < fileSlabAligedSize || roundedSize > globalMaxFileSlabSize {
			return nil
		}
	}
	n := roundedSize / globalMinFileSlabSize
	idx := int(math.Log2(float64(n & -n)))
	//fmt.Println("[INFO]", size, roundedSize, n, idx)
	return fsa.freeLists[idx]
}

func (fsa *FileSlabAllocator) GetFreeListFileSize(size int) int {
	roundedSize := alignSlabSize(size)
	if roundedSize < fileSlabAligedSize || roundedSize > globalMaxFileSlabSize {
		return -1
	}
	return roundedSize
}

func (fsa *FileSlabAllocator) Alloc(size int) FLValue {
	shardId := getFileSlabAllocatorShardId()
	fl := fsa.shards[shardId].GetFreeList(size)
	if fl == nil {
		return FLValue{id: -1}
	}

	dequeuedValue := fl.Dequeue()
	//fmt.Println("[INFO] Alloc", size, fl.Size(), fl.idx, dequeuedValue)
	//fmt.Printf("Type of dequeuedValue: %T\n", dequeuedValue)

	if dequeuedValue == nil {
		return FLValue{id: -1}
	}

	if val, ok := dequeuedValue.(FLValue); ok {
		return val
	}

	return FLValue{id: -1}
}

func (fsa *FileSlabAllocator) Free(size int, flValue FLValue) {
	shardId := getFileSlabAllocatorShardId()
	fl := fsa.shards[shardId].GetFreeList(size)
	if fl != nil {
		fl.Enqueue(flValue)
		//fmt.Println("[INFO] Free", size, flValue, fl.Size(), fl.idx)
	}
}

type IdSize struct {
	ids  []int
	size int
}

type Obj2SlabMapShard struct {
	obj2slabIdsMap map[string]IdSize
	sync.RWMutex
}

type Obj2SlabMap struct {
	shards []*Obj2SlabMapShard
}

func getObj2MapShardId(key string) int {
	start := len(key) - 32
	if start < 0 {
		start = 0
	}
	h := fnv.New32a()
	h.Write([]byte(key[start:]))
	return int(h.Sum32()) % numObj2SlabMapShards
}

func NewObj2SlabMap() *Obj2SlabMap {
	shards := make([]*Obj2SlabMapShard, numObj2SlabMapShards)
	for i := 0; i < numObj2SlabMapShards; i++ {
		shards[i] = &Obj2SlabMapShard{
			obj2slabIdsMap: make(map[string]IdSize),
		}
	}
	return &Obj2SlabMap{
		shards: shards,
	}
}

func (m *Obj2SlabMap) Put(object string, partId int, slabId int, size int) {
	shardIndex := getObj2MapShardId(object)
	shard := m.shards[shardIndex]
	shard.Lock()
	defer shard.Unlock()

	if _, ok := shard.obj2slabIdsMap[object]; !ok {
		// first
		mySlice := make([]int, 8) // XXX: maximum # of parts
		mySlice[partId] = slabId  // assuming flValue.ids has at least one element
		idSize := IdSize{
			ids:  mySlice,
			size: alignSlabSize(size),
		}
		shard.obj2slabIdsMap[object] = idSize
	} else {
		// exist
		idSize := shard.obj2slabIdsMap[object]
		idSize.ids[partId] = slabId // assuming flValue.ids has at least one element
		shard.obj2slabIdsMap[object] = idSize
	}
}

func (m *Obj2SlabMap) Get(key string) (IdSize, bool) {
	shardIndex := getObj2MapShardId(key)
	shard := m.shards[shardIndex]
	shard.RLock()
	defer shard.RUnlock()
	idSize, exists := shard.obj2slabIdsMap[key]
	return idSize, exists
}

func (m *Obj2SlabMap) Delete(key string) {
	shardIndex := getObj2MapShardId(key)
	shard := m.shards[shardIndex]
	shard.Lock()
	defer shard.Unlock()
	delete(shard.obj2slabIdsMap, key)
}

func InitXLRawStorageSlab(hostname string, client *storageRESTClient) {
	numBatch := 20
	for i := 0; i < globalNumFileSlabs; i += numBatch {
		for j := globalMinFileSlabSize; j <= globalMaxFileSlabSize; j *= 2 {
			req := Request{Type: byte(MsgFileSlab), Id: int64(i + 1),
				FilePath: hostname, FileSize: j, Len: numBatch}
			res := client.sockClient.WriteReadFileSlab(&req)
			//fmt.Println("[INFO]", j, res.Fsei)
			for k, fsei := range res.Fsei {
				client.fsa.Free(j, FLValue{id: i + 1 + k, extents: fsei.Extents})
			}
		}
	}
}
