package chunkserver

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/common"
	downloadbuffer "github.com/caleberi/distributed-system/download_buffer"
	"github.com/caleberi/distributed-system/master_server"
	"github.com/caleberi/distributed-system/rpc_struct"
	"github.com/caleberi/distributed-system/shared"
	"github.com/jaswdr/faker/v2"
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

func populateServers(t *testing.T, masterAddress common.ServerAddr) []common.ChunkHandle {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	chunkHandles := []common.ChunkHandle{}

	fake := faker.New()

	for range 5 {
		fakePath := fmt.Sprintf(
			"/%s/%s/file-%d", fake.Music().Name(), fake.Music().Author(), rand.Intn(1000))
		getChunkHandleReply := &rpc_struct.GetChunkHandleReply{}
		index := common.ChunkIndex(0)
		err := shared.UnicastToRPCServer(
			string(masterAddress),
			rpc_struct.MRPCGetChunkHandleHandler,
			rpc_struct.GetChunkHandleArgs{
				Path:  common.Path(fakePath),
				Index: index,
			}, getChunkHandleReply)
		require.NoError(t, err)
		require.GreaterOrEqual(t, getChunkHandleReply.Handle, common.ChunkHandle(index))

		primaryAndSecondaryInfoReply := &rpc_struct.PrimaryAndSecondaryServersInfoReply{}
		err = shared.UnicastToRPCServer(
			string(masterAddress),
			rpc_struct.MRPCGetPrimaryAndSecondaryServersInfoHandler,
			rpc_struct.PrimaryAndSecondaryServersInfoArg{
				Handle: getChunkHandleReply.Handle,
			}, primaryAndSecondaryInfoReply)
		require.NoError(t, err)
		require.NotEmpty(t, primaryAndSecondaryInfoReply.Primary)
		require.NotEmpty(t, primaryAndSecondaryInfoReply.SecondaryServers)

		downloadBufferId := downloadbuffer.NewDownloadBufferId(getChunkHandleReply.Handle)
		forwardDataReply := &rpc_struct.ForwardDataReply{}
		lorem := strings.Join(faker.Lorem.Paragraphs(fake.Lorem(), 100), "\n")
		err = shared.UnicastToRPCServer(
			string(primaryAndSecondaryInfoReply.Primary),
			rpc_struct.CRPCForwardDataHandler,
			rpc_struct.ForwardDataArgs{
				DownloadBufferId: downloadBufferId,
				Data:             []byte(lorem),
				Replicas:         primaryAndSecondaryInfoReply.SecondaryServers,
			}, forwardDataReply)
		require.NoError(t, err)
		require.Zero(t, forwardDataReply.ErrorCode)

		time.Sleep(1 * time.Second) // wait for data to be forwarded
		writeChunkReply := &rpc_struct.WriteChunkReply{}
		err = shared.UnicastToRPCServer(
			string(primaryAndSecondaryInfoReply.Primary),
			rpc_struct.CRPCWriteChunkHandler,
			rpc_struct.WriteChunkArgs{
				DownloadBufferId: downloadBufferId,
				Offset:           common.Offset(0),
				Replicas:         primaryAndSecondaryInfoReply.SecondaryServers,
			}, writeChunkReply)
		require.NoError(t, err)
		require.Zero(t, writeChunkReply.ErrorCode)

		require.GreaterOrEqual(t, writeChunkReply.Length, 20)

		chunkHandles = append(chunkHandles, getChunkHandleReply.Handle)
	}
	return chunkHandles
}

func TestRPCHandler(t *testing.T) {
	dirPath := t.TempDir()
	master := setupMasterServer(t, context.Background(), dirPath, "127.0.0.1:9090")
	slaves := []*ChunkServer{}
	for range 4 {
		slave := setupChunkServer(
			t, t.TempDir(),
			fmt.Sprintf("127.0.0.1:%d", 10000+rand.Intn(1000)), "127.0.0.1:9090")
		slaves = append(slaves, slave)
	}

	defer func(t *testing.T) {
		for _, slave := range slaves {
			assert.NoError(t, slave.Shutdown())
		}
		master.Shutdown()
	}(t)

	time.Sleep(2 * time.Second)
	handles := populateServers(t, master.ServerAddr)

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
						name:              "NoVersionBump",
						shouldBumpVersion: false,
						handle:            handles[rand.Intn(len(handles))],
					},
				}
				for _, subTestcase := range subTestcases {
					args := rpc_struct.CheckChunkVersionArg{
						Handle: subTestcase.handle,
					}
					t.Run(t.Name()+"_"+subTestcase.name, func(t *testing.T) {
						err := shared.UnicastToRPCServer(
							string(slave.ServerAddr),
							rpc_struct.CRPCCheckChunkVersionHandler,
							args, reply)
						assert.NoError(t, err)
						if !subTestcase.shouldBumpVersion {
							assert.True(t, reply.Stale)
						}

					})
				}

			},
		},
		{
			Handler: rpc_struct.CRPCReadChunkHandler,
			DoTest: func(t *testing.T) {
				slave := slaves[rand.Intn(len(slaves))]
				handle := handles[rand.Intn(len(handles))]

				reply := &rpc_struct.ReadChunkReply{}
				err := shared.UnicastToRPCServer(
					string(slave.ServerAddr),
					rpc_struct.CRPCReadChunkHandler,
					rpc_struct.ReadChunkArgs{
						Handle: handle,
						Offset: 0,
						Length: int64(20),
					}, reply)

				assert.NoError(t, err)
				assert.NotEmpty(t, reply.Data)
				assert.Equal(t, 20, len(reply.Data))
			},
		},
		{
			Handler: rpc_struct.CRPCWriteChunkHandler,
			DoTest: func(t *testing.T) {
				fake := faker.New()
				fakePath := fmt.Sprintf("/%s/%s/file-%d", fake.Music().Name(), fake.Music().Author(), rand.Intn(1000))
				getChunkHandleReply := &rpc_struct.GetChunkHandleReply{}
				index := common.ChunkIndex(0)
				err := shared.UnicastToRPCServer(
					string(master.ServerAddr),
					rpc_struct.MRPCGetChunkHandleHandler,
					rpc_struct.GetChunkHandleArgs{
						Path:  common.Path(fakePath),
						Index: index,
					}, getChunkHandleReply)
				require.NoError(t, err)
				require.GreaterOrEqual(t, getChunkHandleReply.Handle, common.ChunkHandle(index))

				primaryAndSecondaryInfoReply := &rpc_struct.PrimaryAndSecondaryServersInfoReply{}
				err = shared.UnicastToRPCServer(
					string(master.ServerAddr),
					rpc_struct.MRPCGetPrimaryAndSecondaryServersInfoHandler,
					rpc_struct.PrimaryAndSecondaryServersInfoArg{
						Handle: getChunkHandleReply.Handle,
					}, primaryAndSecondaryInfoReply)
				require.NoError(t, err)
				require.NotEmpty(t, primaryAndSecondaryInfoReply.Primary)
				require.NotEmpty(t, primaryAndSecondaryInfoReply.SecondaryServers)

				forwardDataReply := &rpc_struct.ForwardDataReply{}
				downloadBufferId := downloadbuffer.NewDownloadBufferId(getChunkHandleReply.Handle)

				err = shared.UnicastToRPCServer(
					string(primaryAndSecondaryInfoReply.Primary),
					rpc_struct.CRPCForwardDataHandler,
					rpc_struct.ForwardDataArgs{
						DownloadBufferId: downloadBufferId,
						Data:             []byte(fake.Lorem().Paragraph(5)),
						Replicas:         primaryAndSecondaryInfoReply.SecondaryServers,
					}, forwardDataReply)
				require.NoError(t, err)
				require.Zero(t, forwardDataReply.ErrorCode)

				time.Sleep(1 * time.Second) // wait for data to be forwarded
				subtests := []struct {
					name    string
					subtest func(*testing.T)
				}{
					{
						name: rpc_struct.CRPCWriteChunkHandler,
						subtest: func(t *testing.T) {
							reply := &rpc_struct.WriteChunkReply{}
							err := shared.UnicastToRPCServer(
								string(primaryAndSecondaryInfoReply.Primary),
								rpc_struct.CRPCWriteChunkHandler,
								rpc_struct.WriteChunkArgs{
									DownloadBufferId: downloadBufferId,
									Offset:           0,
									Replicas:         primaryAndSecondaryInfoReply.SecondaryServers,
								}, reply)

							assert.NoError(t, err)
							assert.Zero(t, reply.ErrorCode)
							assert.Greater(t, reply.Length, 0)
						},
					},
					{
						name: rpc_struct.CRPCAppendChunkHandler,
						subtest: func(t *testing.T) {
							forwardDataReply := &rpc_struct.ForwardDataReply{}
							downloadBufferId := downloadbuffer.NewDownloadBufferId(getChunkHandleReply.Handle)
							contents := fake.Lorem().Sentence(1)
							err = shared.UnicastToRPCServer(
								string(primaryAndSecondaryInfoReply.Primary),
								rpc_struct.CRPCForwardDataHandler,
								rpc_struct.ForwardDataArgs{
									DownloadBufferId: downloadBufferId,
									Data:             []byte(contents),
									Replicas:         primaryAndSecondaryInfoReply.SecondaryServers,
								}, forwardDataReply)
							require.NoError(t, err)
							require.Zero(t, forwardDataReply.ErrorCode)

							reply := &rpc_struct.AppendChunkReply{}
							err := shared.UnicastToRPCServer(
								string(primaryAndSecondaryInfoReply.Primary),
								rpc_struct.CRPCAppendChunkHandler,
								rpc_struct.AppendChunkArgs{
									DownloadBufferId: downloadBufferId,
									Replicas:         primaryAndSecondaryInfoReply.SecondaryServers,
								}, reply)

							assert.NoError(t, err)
							assert.Zero(t, reply.ErrorCode)
							assert.Greater(t, reply.Offset, common.Offset(0))

							readReply := &rpc_struct.ReadChunkReply{}
							err = shared.UnicastToRPCServer(
								string(primaryAndSecondaryInfoReply.Primary),
								rpc_struct.CRPCReadChunkHandler,
								rpc_struct.ReadChunkArgs{
									Handle: getChunkHandleReply.Handle,
									Offset: common.Offset(0),
									Length: int64(len(contents)),
								}, readReply)

							assert.NoError(t, err)
							assert.Zero(t, readReply.ErrorCode)
							assert.NotEmpty(t, readReply.Data)
						},
					},
				}

				for _, subtest := range subtests {
					t.Run(subtest.name, func(t *testing.T) {
						subtest.subtest(t)
					})
				}
			},
		},
		{
			Handler: rpc_struct.CRPCApplyMutationHandler,
			DoTest: func(t *testing.T) {
				fake := faker.New()
				fakePath := fmt.Sprintf("/%s/%s/file-%d", fake.Music().Name(), fake.Music().Author(), rand.Intn(1000))
				getChunkHandleReply := &rpc_struct.GetChunkHandleReply{}
				index := common.ChunkIndex(0)
				err := shared.UnicastToRPCServer(
					string(master.ServerAddr),
					rpc_struct.MRPCGetChunkHandleHandler,
					rpc_struct.GetChunkHandleArgs{
						Path:  common.Path(fakePath),
						Index: index,
					}, getChunkHandleReply)
				require.NoError(t, err)
				require.GreaterOrEqual(t, getChunkHandleReply.Handle, common.ChunkHandle(index))

				primaryAndSecondaryInfoReply := &rpc_struct.PrimaryAndSecondaryServersInfoReply{}
				err = shared.UnicastToRPCServer(
					string(master.ServerAddr),
					rpc_struct.MRPCGetPrimaryAndSecondaryServersInfoHandler,
					rpc_struct.PrimaryAndSecondaryServersInfoArg{
						Handle: getChunkHandleReply.Handle,
					}, primaryAndSecondaryInfoReply)
				require.NoError(t, err)
				require.NotEmpty(t, primaryAndSecondaryInfoReply.Primary)
				require.NotEmpty(t, primaryAndSecondaryInfoReply.SecondaryServers)

				forwardDataReply := &rpc_struct.ForwardDataReply{}
				downloadBufferId := downloadbuffer.NewDownloadBufferId(getChunkHandleReply.Handle)

				err = shared.UnicastToRPCServer(
					string(primaryAndSecondaryInfoReply.Primary),
					rpc_struct.CRPCForwardDataHandler,
					rpc_struct.ForwardDataArgs{
						DownloadBufferId: downloadBufferId,
						Data:             []byte(fake.Lorem().Paragraph(5)),
						Replicas:         primaryAndSecondaryInfoReply.SecondaryServers,
					}, forwardDataReply)
				require.NoError(t, err)
				require.Zero(t, forwardDataReply.ErrorCode)

				reply := &rpc_struct.ApplyMutationReply{}
				err = shared.UnicastToRPCServer(
					string(primaryAndSecondaryInfoReply.Primary),
					rpc_struct.CRPCApplyMutationHandler,
					rpc_struct.ApplyMutationArgs{
						MutationType:     common.MutationAppend,
						Offset:           0,
						DownloadBufferId: downloadBufferId},
					reply)

				assert.NoError(t, err)
				assert.Zero(t, reply.ErrorCode)
				assert.Greater(t, reply.Length, 0)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(t.Name()+"_"+tc.Handler, func(t *testing.T) {
			tc.DoTest(t)
		})
	}

}
