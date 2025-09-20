package hercules

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/common"
	downloadbuffer "github.com/caleberi/distributed-system/download_buffer"
	"github.com/caleberi/distributed-system/rpc_struct"
	"github.com/caleberi/distributed-system/shared"
	"github.com/caleberi/distributed-system/utils"
	"github.com/rs/zerolog/log"
)

// HerculesClient represents a client for interacting with a distributed file system.
// It manages leases for chunk servers and communicates with the master server for file operations.
type HerculesClient struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cache    map[common.ChunkHandle]*common.Lease
	cacheMux sync.RWMutex
	master   common.ServerAddr
}

// NewHerculesClient creates a new HerculesClient instance.
// It initializes the client with a context, master server address, and a cleanup duration for expired leases.
// The cleanup goroutine is started to periodically remove expired leases from the cache.
//
// Parameters:
//   - ctx: The context for managing the client's lifecycle.
//   - address: The address of the master server.
//   - cleanup: The duration between lease cleanup operations.
//
// Returns:
//   - A pointer to the initialized HerculesClient.
func NewHerculesClient(ctx context.Context, address common.ServerAddr, cleanup time.Duration) *HerculesClient {
	actx, cancelFunc := context.WithCancel(ctx)
	hercules := &HerculesClient{
		ctx:    actx,
		master: address,
		cancel: cancelFunc,
		cache:  make(map[common.ChunkHandle]*common.Lease),
	}

	go hercules.cleanLease(cleanup)
	return hercules
}

// cleanLease periodically removes expired leases from the cache.
// It runs in a goroutine and checks for expired leases at the specified interval.
// The cleanup stops when the client's context is canceled.
//
// Parameters:
//   - d: The duration between cleanup operations.
func (hercules *HerculesClient) cleanLease(d time.Duration) {
	cleanup := func(handle common.ChunkHandle, lease *common.Lease) {
		if lease.IsExpired(time.Now().Add(common.LeaseTimeout)) {
			delete(hercules.cache, lease.Handle)
		}
	}
	for {
		select {
		case <-hercules.ctx.Done():
			return
		default:
		}
		<-time.After(d)
		hercules.cacheMux.Lock()
		utils.IterateOverMap(hercules.cache, cleanup)
		hercules.cacheMux.Unlock()
	}
}

// GetChunkServers retrieves the lease information for a given chunk handle.
// It checks the cache first and, if not found, queries the master server for primary and secondary server information.
// The retrieved lease is cached for future use.
//
// Parameters:
//   - handle: The chunk handle to retrieve lease information for.
//
// Returns:
//   - A pointer to the lease information.
//   - An error if the lease cannot be retrieved or is expired.
func (hercules *HerculesClient) GetChunkServers(handle common.ChunkHandle) (*common.Lease, error) {
	hercules.cacheMux.RLock()
	lease, exists := hercules.cache[handle]
	hercules.cacheMux.RUnlock()
	if exists {
		return lease, nil
	}

	var primaryAndSecondaryServersReply rpc_struct.PrimaryAndSecondaryServersInfoReply

	err := shared.UnicastToRPCServer(
		string(hercules.master), rpc_struct.MRPCGetPrimaryAndSecondaryServersInfoHandler,
		rpc_struct.PrimaryAndSecondaryServersInfoArg{Handle: handle}, &primaryAndSecondaryServersReply,
	)
	if err != nil {
		return nil, err
	}

	newLease := &common.Lease{
		Handle:      handle,
		Expire:      primaryAndSecondaryServersReply.Expire,
		Primary:     primaryAndSecondaryServersReply.Primary,
		Secondaries: primaryAndSecondaryServersReply.SecondaryServers,
	}

	if newLease.IsExpired(time.Now()) {
		return nil, fmt.Errorf("GetChunkServers = %v has expired before use", lease)
	}

	hercules.cacheMux.Lock()
	defer hercules.cacheMux.Unlock()
	hercules.cache[handle] = newLease

	return hercules.cache[handle], nil
}

// ObtainLease retrieves or creates a lease for a given chunk handle and offset.
// It checks the cache for an existing lease and, if not found, calls GetChunkServers to retrieve one.
//
// Parameters:
//   - handle: The chunk handle to obtain a lease for.
//   - offset: The offset within the chunk (currently unused in this implementation).
//
// Returns:
//   - A pointer to the lease information.
//   - The offset (always 0 in this implementation).
//   - An error if the lease cannot be retrieved.
func (hercules *HerculesClient) ObtainLease(handle common.ChunkHandle, offset common.Offset) (*common.Lease, common.Offset, error) {
	hercules.cacheMux.RLock()
	lease, exists := hercules.cache[handle]
	if exists {
		hercules.cacheMux.RUnlock()
		return lease, 0, nil
	}
	hercules.cacheMux.RUnlock()

	lease, err := hercules.GetChunkServers(handle)
	if err != nil {
		return nil, offset, common.Error{
			Code: common.UnknownError,
			Err:  "could not retrieve lease",
		}
	}
	return lease, 0, nil
}

// GetChunkHandle retrieves the chunk handle for a given file path and chunk index.
// It sends an RPC request to the master server to obtain the chunk handle.
//
// Parameters:
//   - path: The file path.
//   - offset: The chunk index.
//
// Returns:
//   - The chunk handle.
//   - An error if the RPC call fails.
func (hercules *HerculesClient) GetChunkHandle(path common.Path, offset common.ChunkIndex) (common.ChunkHandle, error) {
	var reply rpc_struct.GetChunkHandleReply
	err := shared.UnicastToRPCServer(string(hercules.master),
		rpc_struct.MRPCGetChunkHandleHandler, rpc_struct.GetChunkHandleArgs{Path: path, Index: offset}, &reply)
	if err != nil {
		return -1, err
	}
	return reply.Handle, nil
}

// MkDir creates a directory at the specified path.
// It sends an RPC request to the master server to create the directory.
//
// Parameters:
//   - path: The path of the directory to create.
//
// Returns:
//   - An error if the RPC call fails.
func (hercules *HerculesClient) MkDir(path common.Path) error {
	reply := &rpc_struct.MakeDirectoryReply{}
	return shared.UnicastToRPCServer(
		string(hercules.master),
		rpc_struct.MRPCMkdirHandler,
		rpc_struct.MakeDirectoryArgs{Path: path}, reply)
}

// CreateFile creates a file at the specified path.
// It sends an RPC request to the master server to create the file.
//
// Parameters:
//   - path: The path of the file to create.
//
// Returns:
//   - An error if the RPC call fails.
func (hercules *HerculesClient) CreateFile(path common.Path) error {
	reply := &rpc_struct.CreateFileReply{}
	return shared.UnicastToRPCServer(
		string(hercules.master),
		rpc_struct.MRPCCreateFileHandler,
		rpc_struct.CreateFileArgs{Path: path}, reply)
}

// List retrieves the list of file or directory entries at the specified path.
// It sends an RPC request to the master server to obtain the path information.
//
// Parameters:
//   - path: The path to list entries for.
//
// Returns:
//   - A slice of PathInfo containing the entries.
//   - An error if the RPC call fails.
func (hercules *HerculesClient) List(path common.Path) ([]common.PathInfo, error) {
	reply := &rpc_struct.GetPathInfoReply{}
	err := shared.UnicastToRPCServer(string(hercules.master), rpc_struct.MRPCListHandler, rpc_struct.GetPathInfoArgs{}, reply)
	if err != nil {
		return nil, err
	}
	return reply.Entries, nil
}

// DeleteFile deletes a file at the specified path.
// It sends an RPC request to the master server to delete the file.
//
// Parameters:
//   - path: The path of the file to delete.
//
// Returns:
//   - An error if the RPC call fails.
func (hercules *HerculesClient) DeleteFile(path common.Path) error {
	reply := &rpc_struct.DeleteFileReply{}
	return shared.UnicastToRPCServer(
		string(hercules.master), rpc_struct.MRPCDeleteFileHandler,
		rpc_struct.DeleteFileArgs{Path: path}, reply)
}

// RenameFile renames a file from the source path to the target path.
// It sends an RPC request to the master server to perform the rename operation.
//
// Parameters:
//   - source: The current path of the file.
//   - target: The new path for the file.
//
// Returns:
//   - An error if the RPC call fails.
func (hercules *HerculesClient) RenameFile(source, target common.Path) error {
	reply := &rpc_struct.RenameFileReply{}
	return shared.UnicastToRPCServer(
		string(hercules.master), rpc_struct.MRPCRenameHandler, rpc_struct.RenameFileArgs{Source: source, Target: target}, reply)
}

// GetFile retrieves the file information for the specified path.
// It sends an RPC request to the master server to obtain the file details.
//
// Parameters:
//   - path: The path of the file to retrieve information for.
//
// Returns:
//   - A pointer to FileInfo containing the file details.
//   - An error if the RPC call fails.
func (hercules *HerculesClient) GetFile(path common.Path) (*common.FileInfo, error) {
	reply := &rpc_struct.GetFileInfoReply{}
	err := shared.UnicastToRPCServer(
		string(hercules.master),
		rpc_struct.MRPCGetFileInfoHandler, rpc_struct.GetFileInfoArgs{Path: path}, reply)
	if err != nil {
		return nil, err
	}

	return &common.FileInfo{
		Chunks: reply.Chunks,
		IsDir:  reply.IsDir,
		Length: reply.Length}, err
}

// Read reads data from a file at the specified path and offset into the provided buffer.
// It retrieves the file information, validates the offset, and reads data from the appropriate chunk servers.
//
// Parameters:
//   - path: The path of the file to read from.
//   - offset: The offset within the file to start reading.
//   - data: The buffer to store the read data.
//
// Returns:
//   - The number of bytes read.
//   - An error if the read operation fails, including EOF if the end of the file is reached.
func (hercules *HerculesClient) Read(path common.Path, offset common.Offset, data []byte) (n int, err error) {

	args := rpc_struct.GetFileInfoArgs{Path: path}
	reply := &rpc_struct.GetFileInfoReply{}
	err = shared.UnicastToRPCServer(string(hercules.master), rpc_struct.MRPCGetFileInfoHandler, args, reply)
	if err != nil {
		return -1, err
	}

	if offset/common.ChunkMaxSizeInByte > common.Offset(reply.Chunks) {
		return -1, fmt.Errorf("offset [%v] cannot be greater than the file size", offset)
	}

	if reply.IsDir {
		return -1, fmt.Errorf("cannot read %s since it is a directory", path)
	}

	pos := 0
	for pos < len(data) {
		index := common.Offset(offset / common.ChunkMaxSizeInByte)
		chunkOffset := offset % common.ChunkMaxSizeInByte

		if index > common.Offset(reply.Chunks) {
			return -1, common.Error{Code: common.ReadEOF, Err: "EOF over chunk"}
		}

		var handle common.ChunkHandle
		handle, err = hercules.GetChunkHandle(args.Path, common.ChunkIndex(index))
		if err != nil {
			return -1, err
		}

		n, err := hercules.ReadChunk(handle, chunkOffset, data[pos:])

		if err != nil {
			if err.(common.Error).Code == common.ReadEOF {
				break
			}
			return -1, err
		}

		offset += common.Offset(n)
		pos += n
	}

	if err != nil && err.(common.Error).Code == common.ReadEOF {
		return pos, io.EOF
	}

	return pos, err
}

// ReadChunk reads data from a specific chunk at the given handle and offset into the provided buffer.
// It queries the master server for replica locations and reads from a randomly chosen replica.
//
// Parameters:
//   - handle: The chunk handle to read from.
//   - offset: The offset within the chunk to start reading.
//   - data: The buffer to store the read data.
//
// Returns:
//   - The number of bytes read.
//   - An error if the read operation fails, including EOF if the end of the chunk is reached.
func (hercules *HerculesClient) ReadChunk(handle common.ChunkHandle, offset common.Offset, data []byte) (int, error) {
	var readLength int

	if common.ChunkMaxSizeInByte-offset > common.Offset(len(data)) {
		readLength = len(data)
	} else {
		readLength = int(common.ChunkMaxSizeInByte - offset)
	}

	var (
		replicasArgs  rpc_struct.RetrieveReplicasArgs
		replicasReply rpc_struct.RetrieveReplicasReply
	)
	replicasArgs.Handle = handle
	err := shared.UnicastToRPCServer(
		string(hercules.master),
		rpc_struct.MRPCGetReplicasHandler,
		replicasArgs, &replicasReply)
	if err != nil {
		log.Err(err).Stack().Msg(err.Error())
		return 0, common.Error{Code: common.UnknownError, Err: err.Error()}
	}
	locations := replicasReply.Locations

	if len(locations) == 0 {
		return 0, common.Error{Code: common.UnknownError, Err: "no available replica"}
	}

	chosenReadServer := locations[rand.Intn(len(replicasReply.Locations))]

	var (
		readChunkArg   rpc_struct.ReadChunkArgs
		readChunkReply rpc_struct.ReadChunkReply
	)
	readChunkArg.Handle = handle
	readChunkArg.Data = data
	readChunkArg.Length = int64(readLength)
	readChunkArg.Offset = offset
	err = shared.UnicastToRPCServer(
		string(chosenReadServer),
		rpc_struct.CRPCReadChunkHandler,
		readChunkArg,
		&readChunkReply)

	if err != nil {
		return 0, common.Error{Code: common.UnknownError, Err: err.Error()}
	}

	if readChunkReply.ErrorCode == common.ReadEOF {
		return int(readChunkReply.Length), common.Error{Code: common.ReadEOF, Err: "EOF error during read"}
	}

	copy(data, readChunkReply.Data)
	return int(readChunkReply.Length), nil

}

// Write writes data to a file at the specified path and offset.
// It retrieves the file information, validates the offset, and writes data to the appropriate chunk servers.
//
// Parameters:
//   - path: The path of the file to write to.
//   - offset: The offset within the file to start writing.
//   - data: The data to write.
//
// Returns:
//   - The number of bytes written.
//   - An error if the write operation fails.
func (hercules *HerculesClient) Write(path common.Path, offset common.Offset, data []byte) (int, error) {

	args := rpc_struct.GetFileInfoArgs{Path: path}
	reply := rpc_struct.GetFileInfoReply{}
	if err := shared.UnicastToRPCServer(string(hercules.master), rpc_struct.MRPCGetFileInfoHandler, args, &reply); err != nil {
		return -1, err
	}

	if offset/common.ChunkMaxSizeInByte > common.Offset(reply.Chunks) {
		return -1, fmt.Errorf("write offset [%v] cannot be greater than the file size", offset)
	}

	if reply.IsDir {
		return -1, fmt.Errorf("cannot read %s since it is a directory", path)
	}

	pos := 0
	for pos < len(data) {
		index := common.Offset(offset / common.ChunkMaxSizeInByte)
		chunkOffset := offset % common.ChunkMaxSizeInByte
		handle, err := hercules.GetChunkHandle(args.Path, common.ChunkIndex(index))
		if err != nil {
			return -1, err
		}

		writeMax := int(common.ChunkMaxSizeInByte - chunkOffset)
		var writeLength int
		if pos+writeMax > len(data) {
			writeLength = len(data) - pos
		} else {
			writeLength = writeMax
		}
		n, err := hercules.WriteChunk(handle, chunkOffset, data[pos:pos+writeLength])
		if err != nil {
			return -1, err
		}
		offset += common.Offset(writeLength)
		pos += n
		if pos == len(data) {
			break
		}
	}

	return int(offset), nil
}

// WriteChunk writes data to a specific chunk at the given handle and offset.
// It obtains a lease, forwards the data to secondary replicas, and writes to the primary replica.
//
// Parameters:
//   - handle: The chunk handle to write to.
//   - offset: The offset within the chunk to start writing.
//   - data: The data to write.
//
// Returns:
//   - The number of bytes written.
//   - An error if the write operation fails.
func (hercules *HerculesClient) WriteChunk(handle common.ChunkHandle, offset common.Offset, data []byte) (int, error) {
	totalDataLengthToWrite := len(data) + int(offset)

	if totalDataLengthToWrite > common.ChunkMaxSizeInByte {
		return -1, fmt.Errorf("totalDataLengthToWrite = %v is greater than the max chunk size %v", totalDataLengthToWrite, common.ChunkMaxSizeInByte)
	}

	writeLease, offset, err := hercules.ObtainLease(handle, offset)
	if err != nil {
		return -1, err
	}
	servers := append(writeLease.Secondaries, writeLease.Primary)
	copy(utils.Filter(servers, func(v common.ServerAddr) bool { return string(v) != "" }), servers)
	if len(servers) == 0 {
		return -1, common.Error{Code: common.UnknownError, Err: "no replica"}
	}

	if writeLease.Primary == "" {
		writeLease.Primary = servers[0]
		servers = servers[1:]
	}

	dataID := downloadbuffer.NewDownloadBufferId(handle)

	var errs []string
	utils.ForEach(servers, func(addr common.ServerAddr) {
		var d rpc_struct.ForwardDataReply
		if addr != "" {
			replicas := utils.Filter(servers, func(v common.ServerAddr) bool { return v != addr })
			err = shared.UnicastToRPCServer(string(addr),
				rpc_struct.CRPCForwardDataHandler,
				rpc_struct.ForwardDataArgs{
					DownloadBufferId: dataID,
					Data:             data,
					Replicas:         replicas,
				}, &d)
			if err != nil {
				errs = append(errs, err.Error())
			}
		}
	})
	if len(errs) != 0 {
		errStr := strings.Join(errs, ";")
		log.Err(errors.New(errStr)).Stack()
	}

	writeArgs := rpc_struct.WriteChunkArgs{
		DownloadBufferId: dataID,
		Offset:           offset,
		Replicas:         servers,
	}

	writeReply := &rpc_struct.WriteChunkReply{}
	err = shared.UnicastToRPCServer(
		string(writeLease.Primary),
		rpc_struct.CRPCWriteChunkHandler,
		writeArgs,
		writeReply,
	)
	if err != nil {
		return -1, err
	}

	return writeReply.Length, err
}

// Append appends the given data to the end of the file at the specified path.
// It retrieves the current file information to determine the starting chunk for
// the append operation. Data is appended to the last existing chunk or new chunks
// as needed. If the data exceeds the maximum append size, an error is returned.
// The function handles chunk boundaries by continuing to the next chunk if the
// current one would exceed its size limit.
//
// Parameters:
//   - path: The path of the file to append to.
//   - data: The byte slice containing the data to append.
//
// Returns:
//   - offset: The absolute offset in the file where the appended data starts.
//   - err: An error if the operation fails, such as invalid data size, RPC errors,
//     or chunk append failures.
func (hercules *HerculesClient) Append(path common.Path, data []byte) (offset common.Offset, err error) {
	if len(data) > common.AppendMaxSizeInByte {
		return 0, fmt.Errorf("len of data [%v] > max append size [%v]", len(data), common.AppendMaxSizeInByte)
	}

	args := rpc_struct.GetFileInfoArgs{Path: path}
	reply := rpc_struct.GetFileInfoReply{}

	err = shared.UnicastToRPCServer(string(hercules.master), rpc_struct.MRPCGetFileInfoHandler, args, &reply)
	if err != nil {
		return -1, err
	}

	// use the last chunk we created on the master server since
	// we are doing an appended mutation
	start := common.ChunkIndex(math.Max(float64(reply.Chunks-1), 0.0))
	var (
		handle      common.ChunkHandle
		chunkOffset common.Offset
	)

	for {
		handle, err = hercules.GetChunkHandle(args.Path, common.ChunkIndex(start))
		if err != nil {
			return -1, err
		}

		chunkOffset, err = hercules.AppendChunk(handle, data)
		if err != nil {
			if e, ok := err.(common.Error); ok && e.Code == common.AppendExceedChunkSize {
				start++
				continue
			}
			return -1, err
		}

		break
	}
	offset = common.Offset(start)*common.ChunkMaxSizeInByte + chunkOffset
	return
}

// AppendChunk appends the given data to the specified chunk handle.
// It first obtains an append lease for the chunk to ensure coordinated access.
// The data is forwarded to all replicas (including the primary) for consistency.
// If the append would exceed the chunk's maximum size, it returns a specific error
// code indicating this condition, allowing the caller to proceed to the next chunk.
// The function assumes the data length does not exceed the maximum append size,
// which should be validated by the caller.
//
// Parameters:
//   - handle: The chunk handle identifying the chunk to append to.
//   - data: The byte slice containing the data to append.
//
// Returns:
//   - offset: The offset within the chunk where the data was appended.
//   - err: An error if the operation fails, such as lease acquisition failure,
//     replication errors, or exceeding chunk size (with code common.AppendExceedChunkSize).
func (hercules *HerculesClient) AppendChunk(handle common.ChunkHandle, data []byte) (common.Offset, error) {
	var offset common.Offset

	if len(data) > common.AppendMaxSizeInByte {
		return offset, common.Error{
			Code: common.UnknownError,
			Err:  fmt.Sprintf("len(data)[%v]  > max append size (%v)", len(data), common.AppendMaxSizeInByte),
		}
	}

	appendLease, offset, err := hercules.ObtainLease(handle, 0)
	if err != nil {
		return offset, err
	}

	if appendLease.Primary == "" {
		appendLease.Primary = appendLease.Secondaries[0]
		appendLease.Secondaries = appendLease.Secondaries[1:]
	}

	dataID := downloadbuffer.NewDownloadBufferId(handle)
	forwardReply := rpc_struct.ForwardDataReply{}
	err = shared.UnicastToRPCServer(string(appendLease.Primary),
		rpc_struct.CRPCForwardDataHandler,
		rpc_struct.ForwardDataArgs{
			DownloadBufferId: dataID,
			Data:             data,
			Replicas:         appendLease.Secondaries,
		}, &forwardReply)
	if err != nil {
		return offset, err
	}

	appendArgs := rpc_struct.AppendChunkArgs{
		DownloadBufferId: dataID,
		Replicas:         appendLease.Secondaries}
	appendReply := rpc_struct.AppendChunkReply{}
	err = shared.UnicastToRPCServer(
		string(appendLease.Primary),
		rpc_struct.CRPCAppendChunkHandler, appendArgs, &appendReply)
	if err != nil {
		return offset, common.Error{Code: common.UnknownError, Err: err.Error()}
	}
	if appendReply.ErrorCode == common.AppendExceedChunkSize {
		return appendReply.Offset, common.Error{
			Code: common.AppendExceedChunkSize,
			Err:  "exceed append chunk size",
		}
	}
	return appendReply.Offset, nil
}
