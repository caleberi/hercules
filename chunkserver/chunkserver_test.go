package chunkserver

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

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

func TestRPCHandler(t *testing.T) {
	master := setupMasterServer(
		t, context.Background(),
		t.TempDir(), "127.0.0.1:9090")
	slaves := []*ChunkServer{}
	for range 4 {
		slave := setupChunkServer(
			t, t.TempDir(),
			fmt.Sprintf("127.0.0.1:%d", 8000+rand.Intn(1000)), "127.0.0.1:9090")
		slaves = append(slaves, slave)
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
				assert.Empty(t, reply.Chunks)
				assert.NotEmpty(t, reply.SysMem)
				assert.NotZero(t, reply.SysMem.TotalAlloc)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(t.Name()+"_"+tc.Handler, func(t *testing.T) {
			tc.DoTest(t)
		})
	}
}
