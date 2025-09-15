package chunkserver

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/common"
	"github.com/caleberi/distributed-system/master_server"
	"github.com/caleberi/distributed-system/rpc_struct"
	"github.com/caleberi/distributed-system/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupChunkServer(
	t *testing.T, root, address, masterAddress string) *ChunkServer {
	assert.NotEmpty(t, root)
	assert.NotEmpty(t, address)
	assert.NotEmpty(t, masterAddress)
	server, err := NewChunkServer(
		common.ServerAddr(address),
		common.ServerAddr(masterAddress),
		root)
	require.NoError(t, err)
	assert.False(t, server.isDead)
	return server
}

func setupMasterServer(
	t *testing.T, ctx context.Context, root, address string) *master_server.MasterServer {
	assert.NotEmpty(t, root)
	assert.NotEmpty(t, address)

	server := master_server.NewMasterServer(
		ctx, common.ServerAddr(address), root)
	assert.NotNil(t, server)
	return server
}

func generateRandomChunks() *chunkInfo {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	chunk := &chunkInfo{
		length:       common.Offset(rand.Int63n(1_000_000_000)),
		checksum:     common.Checksum("test"),
		version:      common.ChunkVersion(rand.Intn(100) + 1),
		completed:    rand.Float32() < 0.7,
		abandoned:    rand.Float32() < 0.2,
		isCompressed: false,
		replication:  rand.Intn(4) + 1,
		serverStatus: rand.Intn(4),
		creationTime: time.Now().Add(-time.Hour * 24 * time.Duration(rand.Intn(30))),
		lastModified: time.Now().Add(-time.Hour * 24 * time.Duration(rand.Intn(7))),
		accessTime:   time.Now().Add(-time.Hour * 24 * time.Duration(rand.Intn(7))),
		mutations:    make(map[common.ChunkVersion]common.Mutation),
	}

	numMutations := rand.Intn(6)
	for range numMutations {
		mutationVersion := common.ChunkVersion(rand.Intn(100) + 1)
		chunk.mutations[mutationVersion] = common.Mutation{}
	}

	if chunk.lastModified.Before(chunk.creationTime) {
		chunk.lastModified = chunk.creationTime.Add(time.Hour * time.Duration(rand.Intn(24)))
	}
	if chunk.accessTime.Before(chunk.creationTime) {
		chunk.accessTime = chunk.creationTime.Add(time.Hour * time.Duration(rand.Intn(24)))
	}

	return chunk
}

func TestRPCHandler(t *testing.T) {

	master := setupMasterServer(
		t, context.Background(),
		t.TempDir(), "127.0.0.1:9090")
	slaves := []*ChunkServer{}
	handles := []common.ChunkHandle{}
	chunks := make(map[common.ChunkHandle]*chunkInfo)
	for range 4 {
		slave := setupChunkServer(
			t, t.TempDir(),
			fmt.Sprintf("127.0.0.1:%d", 8000+rand.Intn(1000)), "127.0.0.1:9090")
		slave.testMode = true
		slaves = append(slaves, slave)
		for range 100 {
			handle := common.ChunkHandle(rand.Intn(1000))
			chunks[handle] = generateRandomChunks()
			handles = append(handles, handle)
		}
		slave.chunks = chunks
	}

	defer func(t *testing.T) {
		master.Shutdown()
		for _, slave := range slaves {
			assert.NoError(t, slave.Shutdown())
		}
	}(t)

	type testcase struct {
		Handler string
		DoTest  func(t *testing.T)
	}

	testCases := []testcase{
		{
			Handler: rpc_struct.MRPCHeartBeatHandler,
			DoTest: func(t *testing.T) {
				slave := slaves[rand.Intn(len(slaves))]
				reply := &rpc_struct.HeartBeatReply{}
				err := shared.UnicastToRPCServer(
					string(master.ServerAddr),
					rpc_struct.MRPCHeartBeatHandler,
					rpc_struct.HeartBeatArg{
						Address:       slave.ServerAddr,
						PendingLeases: make([]*common.Lease, 0),
						MachineInfo: common.MachineInfo{
							RoundTripProximityTime: 10,
							Hostname:               "hostname-12",
						},
						ExtendLease: false,
					}, reply)
				assert.NoError(t, err)
				assert.False(t, reply.LastHeartBeat.IsZero())
			},
		},
		{
			Handler: rpc_struct.CRPCSysReportHandler,
			DoTest: func(t *testing.T) {
				slave := slaves[rand.Intn(len(slaves))]
				reply := &rpc_struct.SysReportInfoReply{}
				err := shared.UnicastToRPCServer(
					string(slave.ServerAddr),
					rpc_struct.CRPCSysReportHandler,
					rpc_struct.SysReportInfoArg{}, reply)
				assert.NoError(t, err)
				assert.NotEmpty(t, reply.Chunks)
				assert.NotEmpty(t, reply.SysMem)
				assert.NotZero(t, reply.SysMem.TotalAlloc)
			},
		},
		{
			Handler: rpc_struct.CRPCCheckChunkVersionHandler,
			DoTest: func(t *testing.T) {
				slave := slaves[rand.Intn(len(slaves))]
				reply := &rpc_struct.CheckChunkVersionReply{}
				subTestcases := []struct {
					name              string
					shouldBumpVersion bool
					handle            common.ChunkHandle
				}{
					{
						name:              "VersionBump",
						shouldBumpVersion: true,
						handle:            handles[0],
					},
					{
						name:              "NoVersionBump",
						shouldBumpVersion: false,
						handle:            handles[0],
					},
				}
				for _, subTestcase := range subTestcases {
					args := rpc_struct.CheckChunkVersionArg{
						Handle: handles[0],
					}
					if subTestcase.shouldBumpVersion {
						args.Version = chunks[handles[0]].version + 1
					}
					if !subTestcase.shouldBumpVersion {
						args.Version = chunks[handles[0]].version
					}
					t.Run(t.Name()+"_"+subTestcase.name, func(t *testing.T) {

						err := shared.UnicastToRPCServer(
							string(slave.ServerAddr),
							rpc_struct.CRPCCheckChunkVersionHandler,
							args, reply)
						assert.NoError(t, err)
						if subTestcase.shouldBumpVersion {
							assert.False(t, reply.Stale)
							assert.False(t, chunks[handles[0]].abandoned)
						}
						if !subTestcase.shouldBumpVersion {
							assert.True(t, reply.Stale)
							assert.True(t, chunks[handles[0]].abandoned)
						}

					})
				}

			},
		},
	}

	for _, tc := range testCases {
		t.Run(t.Name()+"_"+tc.Handler, func(t *testing.T) {
			tc.DoTest(t)
		})
	}
}
