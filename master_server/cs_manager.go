package master_server

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/common"
	"github.com/caleberi/distributed-system/rpc_struct"
	"github.com/caleberi/distributed-system/shared"
	"github.com/caleberi/distributed-system/utils"
	"github.com/rs/zerolog/log"
)

// ChunkServerManager manages chunk servers, chunks, and file metadata in a distributed file system.
// It maintains mappings of servers, chunks, and files, and handles synchronization and replica migration.
// The struct uses multiple locks to ensure thread-safe access to its fields.
type ChunkServerManager struct {
	sync.RWMutex                                                      // RWMutex provides a global lock for coordinating access to the ChunkServerManager's fields.
	serverMutex                sync.RWMutex                           // serverMutex protects concurrent access to the servers map.
	chunkMutex                 sync.RWMutex                           // chunkMutex protects concurrent access to the chunks map.
	servers                    map[common.ServerAddr]*chunkServerInfo // servers maps server addresses to their corresponding chunk server information.
	chunks                     map[common.ChunkHandle]*chunkInfo      // chunks maps chunk handles to their corresponding chunk information.
	replicaMigration           []common.ChunkHandle                   // replicaMigration stores chunk handles involved in replica migration operations.
	files                      map[common.Path]*fileInfo              // files maps file paths to their corresponding file information.
	handleToPathMapping        map[common.ChunkHandle]common.Path     // handleToPathMapping maps chunk handles to their associated file paths.
	numberOfCreatedChunkHandle common.ChunkHandle                     // numberOfCreatedChunkHandle tracks the total number of chunk handles created.
}

func NewChunkServerManager() *ChunkServerManager {
	return &ChunkServerManager{
		chunkMutex:                 sync.RWMutex{},
		serverMutex:                sync.RWMutex{},
		replicaMigration:           make([]common.ChunkHandle, 0),
		servers:                    make(map[common.ServerAddr]*chunkServerInfo),
		chunks:                     make(map[common.ChunkHandle]*chunkInfo),
		files:                      make(map[common.Path]*fileInfo),
		handleToPathMapping:        make(map[common.ChunkHandle]common.Path),
		numberOfCreatedChunkHandle: 0,
	}
}

// getReplicas retrieves the list of server addresses hosting replicas for a given chunk handle.
// It performs a thread-safe read of the chunk information and returns the associated server locations.
// If the chunk handle does not exist, an error is returned.
//
// Parameters:
//   - handle: The chunk handle identifying the chunk whose replicas are to be retrieved.
//
// Returns:
//   - A slice of server addresses hosting the chunk's replicas.
//   - An error if the chunk handle is not found in the chunks map.
func (csm *ChunkServerManager) getReplicas(handle common.ChunkHandle) ([]common.ServerAddr, error) {
	csm.chunkMutex.RLock()
	chunkInfo, ok := csm.chunks[handle]
	csm.chunkMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("could not retrieve replica for chunk(%v)", handle)
	}

	return chunkInfo.locations, nil
}

// registerReplicas adds a server address to the list of replicas for a given chunk handle.
// It performs a thread-safe update to the chunk's location list, using either a read lock or a global lock based on the readLock parameter.
// If the chunk handle does not exist, an error is returned.
//
// Parameters:
//   - handle: The chunk handle identifying the chunk to register a replica for.
//   - addr: The server address to be added to the chunk's replica locations.
//   - readLock: If true, uses a read lock (RLock) for accessing the chunks map; otherwise, uses a global write lock.
//
// Returns:
//   - An error if the chunk handle is not found in the chunks map; otherwise, nil.
func (csm *ChunkServerManager) registerReplicas(
	handle common.ChunkHandle, addr common.ServerAddr, readLock bool) error {
	var (
		chunkInfo *chunkInfo
		ok        bool
	)

	if readLock {
		csm.chunkMutex.RLock()
		chunkInfo, ok = csm.chunks[handle]
		csm.chunkMutex.RUnlock()
	} else {
		csm.Lock()
		defer csm.Unlock()
		chunkInfo, ok = csm.chunks[handle]
	}

	if !ok {
		return fmt.Errorf("cannot find chunk %v", handle)
	}

	chunkInfo.locations = append(chunkInfo.locations, addr)
	log.Info().Msgf(
		"Registering replicas for handle=%v\n with location=%v\n data=%#v",
		handle, chunkInfo.locations, chunkInfo)
	return nil
}

// detectDeadServer identifies servers that are considered dead based on their last heartbeat.
// A server is deemed dead if its last heartbeat is zero (never set) or if the time since the last heartbeat
// exceeds the configured ServerHealthCheckTimeout. The function is thread-safe, using a lock on the servers map.
//
// Returns:
//   - A slice of server addresses that are considered dead.
func (csm *ChunkServerManager) detectDeadServer() []common.ServerAddr {
	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()

	deadServers := make([]common.ServerAddr, 0, len(csm.servers))
	for serverAddr, chk := range csm.servers {
		if chk.lastHeatBeat.IsZero() || time.Now().After(chk.lastHeatBeat.Add(common.ServerHealthCheckTimeout)) {
			deadServers = append(deadServers, serverAddr)
		}
	}
	return deadServers
}

// removeChunks removes a specified server from the replica locations of the given chunk handles.
// It updates the chunk's locations by filtering out the specified server and checks if the remaining
// replicas meet the minimum replication factor. If the number of replicas falls below the minimum,
// the chunk is added to the replica migration list. If no replicas remain, an error is logged.
// The function is thread-safe, using locks on the chunks map and individual chunk info.
//
// Parameters:
//   - handles: A slice of chunk handles identifying the chunks to update.
//   - server: The server address to remove from each chunk's replica locations.
//
// Returns:
//   - An error containing a semicolon-separated list of error messages if any chunks are not found
//     or if all replicas for a chunk are lost; otherwise, nil.
func (csm *ChunkServerManager) removeChunks(handles []common.ChunkHandle, server common.ServerAddr) error {
	csm.chunkMutex.Lock()
	defer csm.chunkMutex.Unlock()
	errs := []string{}

	utils.ForEach(handles, func(handle common.ChunkHandle) {
		chk, exist := csm.chunks[handle]
		if !exist {
			errs = append(errs, fmt.Sprintf("chunk handle (%v) does not exist", handle))
			return
		}

		chk.Lock()
		chk.locations = utils.Filter(
			chk.locations,
			func(v common.ServerAddr) bool {
				return v != server
			})
		chk.expire = time.Now()
		num := len(chk.locations) //  calulate the number of chunk replica if it is less that the
		// the replication factor which is ususally 3 then we need more server to fulfill this
		chk.Unlock()

		if num < common.MinimumReplicationFactor {
			csm.replicaMigration = append(csm.replicaMigration, handle)
			if num == 0 {
				msg := fmt.Sprintf("Lost all replicas of chk (%v)", handle)
				log.Info().Msg(msg)
				errs = append(errs, msg)
			}
		}

	})

	errStr := strings.Join(errs, ";")

	if len(errs) != 0 {
		return errors.New(errStr)
	}
	return nil
}

// removeServer removes a server from the ChunkServerManager's servers map and returns the chunk handles associated with it.
// It performs a thread-safe removal of the server and collects all chunk handles that were stored on the server.
// If the server is not found, an error is returned.
//
// Parameters:
//   - addr: The server address to remove from the servers map.
//
// Returns:
//   - A slice of chunk handles that were associated with the removed server.
//   - An error if the server address is not found in the servers map; otherwise, nil.
func (csm *ChunkServerManager) removeServer(addr common.ServerAddr) ([]common.ChunkHandle, error) {
	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()

	chk, ok := csm.servers[addr]
	if !ok {
		return nil, fmt.Errorf("server %v not found", addr)
	}

	var handles []common.ChunkHandle
	utils.LoopOverMap(chk.chunks, func(handle common.ChunkHandle, _ bool) {
		handles = append(handles, handle)
	})

	delete(csm.servers, addr)
	return handles, nil
}

// addChunk adds a chunk handle to the chunk maps of the specified servers.
// It performs a thread-safe update to each server's chunk map, associating the given chunk handle with the server.
// If a server is not found in the servers map, a warning is logged, and the operation is skipped for that server.
//
// Parameters:
//   - addrs: A slice of server addresses to associate with the chunk handle.
//   - handle: The chunk handle to add to each server's chunk map.
func (csm *ChunkServerManager) addChunk(addrs []common.ServerAddr, handle common.ChunkHandle) {
	for _, v := range addrs {
		csm.serverMutex.RLock()
		sv, exists := csm.servers[v]
		csm.serverMutex.RUnlock()
		if exists {
			sv.Lock()
			sv.chunks[handle] = true
			sv.Unlock()
		} else {
			log.Warn().Msg(fmt.Sprintf("add chunk in removed server %v", sv))
		}
	}
}

// addGarbage adds a chunk handle to the garbage list of a specified server.
// It performs a thread-safe update to the server's garbage list, appending the given chunk handle.
// If the server is not found in the servers map, the operation is silently ignored.
//
// Parameters:
//   - addr: The server address whose garbage list will be updated.
//   - handle: The chunk handle to add to the server's garbage list.
func (csm *ChunkServerManager) addGarbage(addr common.ServerAddr, handle common.ChunkHandle) {
	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()

	sv, exists := csm.servers[addr]
	if exists {
		sv.Lock()
		sv.garbages = append(sv.garbages, handle)
		sv.Unlock()
	}
}

// replicationMigration updates and returns the list of chunk handles requiring replica migration.
// It checks the replica count for each chunk in the replicaMigration list and retains only those
// with fewer replicas than the MinimumReplicationFactor. The function ensures thread-safety by
// locking the entire ChunkServerManager. It also removes duplicates from the migration list and
// sorts the handles for consistency.
//
// Returns:
//   - A slice of chunk handles that need replica migration due to insufficient replicas.
func (csm *ChunkServerManager) replicationMigration() []common.ChunkHandle {
	csm.Lock()
	defer csm.Unlock()

	slices.Sort(csm.replicaMigration)
	csm.replicaMigration = make([]common.ChunkHandle, 0)
	for i, v := range csm.replicaMigration {
		if i == 0 || v != common.ChunkHandle(csm.replicaMigration[i-1]) { // avoid duplicate
			csm.replicaMigration = append(csm.replicaMigration, common.ChunkHandle(v))
		}
	}

	return csm.replicaMigration
}

func (csm *ChunkServerManager) extendLease(handle common.ChunkHandle, primary common.ServerAddr) (*chunkInfo, error) {
	csm.chunkMutex.RLock()
	chk, ok := csm.chunks[handle]
	csm.chunkMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("cannot extend lease for %v", primary)
	}

	if chk.primary != primary && chk.expire.After(time.Now()) {
		return nil, fmt.Errorf("%v does not hold lease for %v ", primary, handle)
	}

	chk.expire = chk.expire.Add(common.LeaseTimeout)
	return chk, nil

}

func (csm *ChunkServerManager) getLeaseHolder(handle common.ChunkHandle) (*common.Lease, []common.ServerAddr, error) {
	csm.chunkMutex.RLock()
	chk, ok := csm.chunks[handle]
	csm.chunkMutex.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("cannot find chunk handle %v - Invalid most likely", handle)
	}

	chk.Lock()
	defer chk.Unlock()

	var staleServers []common.ServerAddr
	lease := &common.Lease{}

	// log.Info().Msgf("reading chunk [%#v] in order to produce new lease with locations -> %v", chk, chk.locations)
	if chk.isExpired(time.Now()) { // chunk has expired so move it
		chk.version++

		arg := rpc_struct.CheckChunkVersionArg{
			Version: chk.version,
			Handle:  handle,
		}

		var newLocationList []string
		var lock sync.Mutex

		var wg sync.WaitGroup
		wg.Add(len(chk.locations))

		for _, v := range chk.locations {
			go func(addr common.ServerAddr) {
				defer wg.Done()

				var reply rpc_struct.CheckChunkVersionReply

				err := shared.UnicastToRPCServer(string(addr), rpc_struct.CRPCCheckChunkVersionHandler, arg, &reply)
				lock.Lock()
				defer lock.Unlock()
				if err != nil || reply.Stale {
					log.Warn().Msg(fmt.Sprintf("stale chunk %v detected in %v ", handle, addr))
					staleServers = append(staleServers, addr)
					return
				}
				newLocationList = append(newLocationList, string(addr))
			}(v)
		}

		wg.Wait()

		chk.locations = make([]common.ServerAddr, 0)
		utils.ForEach(newLocationList, func(v string) { chk.locations = append(chk.locations, common.ServerAddr(v)) })

		if len(chk.locations) < common.MinimumReplicationFactor {
			csm.Lock()
			csm.replicaMigration = append(csm.replicaMigration, handle) // migrate the chunk
			csm.Unlock()

			if len(chk.locations) == 0 {
				chk.version--
				return nil, nil, fmt.Errorf("no replica for %v", handle)
			}
		} else {
			chk.primary = chk.locations[0]
			chk.expire = chk.expire.Add(common.LeaseTimeout)
		}
	}

	lease.Primary = chk.primary
	lease.Secondaries = utils.Filter(chk.locations, func(v common.ServerAddr) bool { return v != chk.primary })

	if lease.Primary == "" && len(lease.Secondaries) > 0 {
		lease.Primary = lease.Secondaries[0]
		lease.Secondaries = lease.Secondaries[1:]
	}
	lease.Expire = time.Now().Add(common.LeaseTimeout)
	go func() {
		var (
			args  rpc_struct.GrantLeaseInfoArgs
			reply rpc_struct.GrantLeaseInfoReply
		)
		args.Expire = lease.Expire
		args.Primary = lease.Primary
		args.Secondaries = lease.Secondaries
		args.Handle = lease.Handle
		err := shared.UnicastToRPCServer(string(lease.Primary), rpc_struct.CRPCGrantLeaseHandler, args, &reply)
		if err != nil {
			log.Err(err).Stack().Send()
			log.Warn().Msg(fmt.Sprintf("could not grant lease to primary = %v", chk.primary))
		}
	}()

	return lease, staleServers, nil
}

func (csm *ChunkServerManager) chooseServers(num int) ([]common.ServerAddr, error) {
	type addrToRRTL struct {
		addr common.ServerAddr
		rrtl float64
	}

	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()

	if num > len(csm.servers) {
		return nil, fmt.Errorf("no enough servers for %v replicas", num)
	}

	var (
		intermediateArr []addrToRRTL
		ret             []common.ServerAddr
	)

	for a, server := range csm.servers {
		intermediateArr = append(intermediateArr, addrToRRTL{
			addr: a,
			rrtl: server.serverInfo.RoundTripProximityTime,
		})
	}

	slices.SortFunc(intermediateArr, func(a, b addrToRRTL) int {
		if a.rrtl < b.rrtl {
			return -1
		} else if a.rrtl > b.rrtl {
			return 1
		}
		return 0
	})

	all := utils.Map(intermediateArr, func(d addrToRRTL) common.ServerAddr { return d.addr })
	choose, err := utils.Sample(len(all), num)
	if err != nil {
		return nil, err
	}
	for _, v := range choose {
		ret = append(ret, all[v])
	}

	log.Info().Msg(fmt.Sprintf("Chose servers %v for replication", ret))
	return ret, nil
}

func (csm *ChunkServerManager) createChunk(path common.Path, addrs []common.ServerAddr) (common.ChunkHandle, []common.ServerAddr, error) {
	csm.Lock()
	defer csm.Unlock()

	currentHandle := csm.numberOfCreatedChunkHandle
	csm.numberOfCreatedChunkHandle++

	file, exist := csm.files[path]
	if !exist {
		csm.files[path] = &fileInfo{
			handles: make([]common.ChunkHandle, 0),
		}
		file = csm.files[path]
	}
	file.handles = append(file.handles, currentHandle) // record the new chunkhandle for this path
	csm.handleToPathMapping[currentHandle] = path

	// create a chunk and update the record on master
	chk := &chunkInfo{
		path:   path,
		expire: time.Now().Add(common.LeaseTimeout),
	}
	csm.chunkMutex.Lock()
	csm.chunks[currentHandle] = chk // record the chunk on the master for later persistence
	csm.chunkMutex.Unlock()

	errs := []error{}
	success := []string{}

	args := rpc_struct.CreateChunkArgs{Handle: currentHandle}

	utils.ForEach(addrs, func(addr common.ServerAddr) {
		var reply rpc_struct.CreateChunkReply
		err := shared.UnicastToRPCServer(
			string(addr), rpc_struct.CRPCCreateChunkHandler, args, &reply)
		if err != nil {
			errs = append(errs, err)
		} else {
			// update this particular chunk information before handing it o
			chk.Lock()
			chk.locations = append(chk.locations, addr)
			success = append(success, string(addr))
			chk.Unlock()
			log.Info().Msg(fmt.Sprintf("chk location updated after creation locations => %v", chk.locations))
		}
	})

	servers := utils.Map(success, func(v string) common.ServerAddr { return common.ServerAddr(v) })
	err := errors.Join(errs...)

	// if err occurred during the creation of chunk, then
	// we register chunk for re-migration
	if err != nil {
		log.Err(err).Stack().Send()
		csm.replicaMigration = append(csm.replicaMigration, currentHandle)
		return currentHandle, servers, err
	}
	return currentHandle, servers, nil
}

func (csm *ChunkServerManager) chooseReplicationServer(handle common.ChunkHandle) (from, to common.ServerAddr, err error) {
	csm.serverMutex.RLock()
	defer csm.serverMutex.RUnlock()

	from = ""
	to = ""
	err = nil
	for a, v := range csm.servers {
		if v.chunks[handle] {
			from = a
		} else {
			to = a
		}
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("no enough server for replica %v", handle)
	return
}

func (csm *ChunkServerManager) SerializeChunks() []serialChunkInfo {
	csm.RLock()
	defer csm.RUnlock()

	var ret []serialChunkInfo
	for k, v := range csm.files {
		var chunks []common.PersistedChunkInfo
		for _, handle := range v.handles {
			chunks = append(chunks, common.PersistedChunkInfo{
				Handle:   handle,
				Version:  csm.chunks[handle].version,
				Checksum: "",
				Length:   0,
			})
		}
		ret = append(ret, serialChunkInfo{Path: k, Info: chunks})
	}
	return ret
}

func (csm *ChunkServerManager) HeartBeat(addr common.ServerAddr, info common.MachineInfo, reply *rpc_struct.HeartBeatReply) bool {
	csm.serverMutex.Lock()
	defer csm.serverMutex.Unlock()
	srv, ok := csm.servers[addr]
	if !ok {
		log.Info().Msg(fmt.Sprintf("adding new server %v to master", addr))
		csm.servers[addr] = &chunkServerInfo{
			lastHeatBeat: time.Now(),
			garbages:     make([]common.ChunkHandle, 0),
			chunks:       make(map[common.ChunkHandle]bool),
			serverInfo:   info,
		}
		return true
	}

	srv.Lock()
	defer srv.Unlock()
	if reply.Garbage == nil {
		reply.Garbage = make([]common.ChunkHandle, 0)
	}
	copy(reply.Garbage, srv.garbages)

	srv.garbages = make([]common.ChunkHandle, 0)
	srv.lastHeatBeat = time.Now()
	reply.LastHeartBeat = srv.lastHeatBeat

	return false
}

func (csm *ChunkServerManager) DeserializeChunks(chunkInfos []serialChunkInfo) {
	csm.chunkMutex.Lock()
	defer csm.chunkMutex.Unlock()

	for _, chunk := range chunkInfos {
		log.Info().Msg("ChunkServerManager restore files " + string(chunk.Path))
		fileInfo := &fileInfo{
			handles: make([]common.ChunkHandle, 0),
		}
		for _, info := range chunk.Info {
			log.Info().Msg(fmt.Sprintf("ChunkServerManager restore handle %v", info.Handle))
			fileInfo.handles = append(fileInfo.handles, info.Handle)
			csm.chunks[info.Handle] = &chunkInfo{
				version:  info.Version,
				checksum: info.Checksum,
				path:     chunk.Path,
				expire:   time.Now(),
			}
			csm.handleToPathMapping[info.Handle] = chunk.Path
		}
		csm.files[chunk.Path] = fileInfo
		csm.numberOfCreatedChunkHandle += common.ChunkHandle(len(fileInfo.handles))
	}
}

func (csm *ChunkServerManager) getChunkHandle(filePath common.Path, idx common.ChunkIndex) (common.ChunkHandle, error) {
	csm.RLock()
	defer csm.RUnlock()

	fileInfo, ok := csm.files[filePath]
	if !ok {
		return -1, fmt.Errorf(" => %v-%v", filePath, idx)
	}

	if idx < 0 || int(idx) >= len(fileInfo.handles) {
		return -1, fmt.Errorf("invalid chunk index %v", idx)
	}
	return fileInfo.handles[common.ChunkHandle(idx)], nil
}

func (csm *ChunkServerManager) getChunk(handle common.ChunkHandle) (*chunkInfo, bool) {
	csm.chunkMutex.RLock()
	defer csm.chunkMutex.RUnlock()
	chk, ok := csm.chunks[handle]
	return chk, ok
}

func (csm *ChunkServerManager) deleteChunk(handle common.ChunkHandle) {
	csm.chunkMutex.Lock()
	defer csm.chunkMutex.Unlock()
	delete(csm.chunks, handle)
}

func (csm *ChunkServerManager) GetChunkHandles(filePath common.Path) ([]common.ChunkHandle, error) {
	csm.RLock()
	defer csm.RUnlock()

	fileInfo, ok := csm.files[filePath]
	if !ok {
		return nil, fmt.Errorf("cannot get handles for path => %v", filePath)
	}

	var ret []common.ChunkHandle
	copy(ret, fileInfo.handles)
	return ret, nil
}

func (csm *ChunkServerManager) UpdateFilePath(source common.Path, target common.Path) error {
	csm.chunkMutex.Lock()
	defer csm.chunkMutex.Unlock()

	if _, exists := csm.files[source]; !exists {
		return fmt.Errorf("could not locate file with path = %s", source)
	}

	val := csm.files[source]
	delete(csm.files, source)
	csm.files[target] = val
	return nil
}
