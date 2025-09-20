package hercules

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/chunkserver"
	"github.com/caleberi/distributed-system/common"
	"github.com/caleberi/distributed-system/master_server"
	"github.com/jaswdr/faker/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMasterServer(t *testing.T, ctx context.Context, root, address string) *master_server.MasterServer {
	assert.NotEmpty(t, root)
	assert.NotEmpty(t, address)

	server := master_server.NewMasterServer(ctx, common.ServerAddr(address), root)
	assert.NotNil(t, server)
	return server
}

func setupChunkServer(t *testing.T, root, address, masterAddress string) *chunkserver.ChunkServer {
	assert.NotEmpty(t, root)
	assert.NotEmpty(t, address)
	assert.NotEmpty(t, masterAddress)

	server, err := chunkserver.NewChunkServer(common.ServerAddr(address), common.ServerAddr(masterAddress), root)
	require.NoError(t, err)
	return server
}

func populateServers(t *testing.T, client *HerculesClient) []common.ChunkHandle {
	rand.NewSource(time.Now().UnixNano())
	chunkHandles := []common.ChunkHandle{}
	fake := faker.New()

	for range 3 {
		fakePath := fmt.Sprintf("/%s/%s/file-%d",
			fake.Music().Genre(), fake.Music().Author(), rand.Intn(1000))

		handle, err := client.GetChunkHandle(common.Path(fakePath), 0)
		require.NoError(t, err)

		data := []byte(fake.Lorem().Paragraph(5))
		n, err := client.Write(common.Path(fakePath), 0, data)
		require.NoError(t, err)
		require.Equal(t, len(data), n)
		chunkHandles = append(chunkHandles, handle)
	}

	return chunkHandles
}

func TestHerculesClientIntegration(t *testing.T) {
	ctx := t.Context()
	dirPath := t.TempDir()

	master := setupMasterServer(t, ctx, dirPath, "127.0.0.1:9090")

	slaves := []*chunkserver.ChunkServer{}
	for range 4 {
		slave := setupChunkServer(t, t.TempDir(),
			fmt.Sprintf("127.0.0.1:%d", 10000+rand.Intn(1000)), "127.0.0.1:9090")
		slaves = append(slaves, slave)
	}

	defer func() {
		for _, slave := range slaves {
			assert.NoError(t, slave.Shutdown())
		}
		master.Shutdown()
	}()

	time.Sleep(2 * time.Second)
	client := NewHerculesClient(ctx, "127.0.0.1:9090", 150*time.Millisecond)

	populateServers(t, client)
	time.Sleep(2 * time.Second)

	type testcase struct {
		name   string
		doTest func(t *testing.T)
	}

	testCases := []testcase{
		{
			name: "WriteAndRead",
			doTest: func(t *testing.T) {
				fake := faker.New()
				path := common.Path(fmt.Sprintf(
					"/%s/%s/file-%d", fake.Music().Genre(),
					fake.Music().Author(), rand.Intn(1000)))
				data := []byte(fake.Lorem().Sentence(10))

				handle, err := client.GetChunkHandle(common.Path(path), 0)
				require.NoError(t, err)
				require.GreaterOrEqual(t, int(handle), 0)
				n, err := client.Write(path, 0, data)
				assert.NoError(t, err)
				assert.Equal(t, len(data), n)

				readBuffer := make([]byte, len(data))
				n, err = client.Read(path, 0, readBuffer)
				assert.NoError(t, err)
				assert.Equal(t, len(data), n)
				assert.Equal(t, data, readBuffer)
			},
		},
		{
			name: "AppendAndRead",
			doTest: func(t *testing.T) {
				fake := faker.New()
				path := common.Path(fmt.Sprintf("/%s/%s/file-%d",
					fake.Music().Genre(), fake.Music().Author(), rand.Intn(1000)))
				data := []byte(fake.Lorem().Sentence(5))

				handle, err := client.GetChunkHandle(common.Path(path), 0)
				require.NoError(t, err)
				require.GreaterOrEqual(t, int(handle), 0)
				n, err := client.Write(path, 0, data)
				assert.NoError(t, err)
				assert.Equal(t, len(data), n)

				data = []byte(strings.Join(fake.Lorem().Paragraphs(5), "\n"))
				_, err = client.Append(path, data)
				assert.NoError(t, err)

				readBuffer := make([]byte, n+len(data))
				n, err = client.Read(path, common.Offset(0), readBuffer)
				assert.NoError(t, err)
				assert.Equal(t, len(readBuffer), n)
				assert.Contains(t, string(readBuffer), string(data))
			},
		},
		{
			name: "ListAndDelete",
			doTest: func(t *testing.T) {
				fake := faker.New()
				dirPath := common.Path(fmt.Sprintf(
					"/%s/%s", fake.Music().Genre(), fake.Music().Author()))
				filePath := common.Path(fmt.Sprintf("%s/file-%d", dirPath, rand.Intn(1000)))

				handle, err := client.GetChunkHandle(common.Path(filePath), 0)
				require.NoError(t, err)
				require.GreaterOrEqual(t, int(handle), 0)

				entries, err := client.List(dirPath)
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, len(entries), 1)

				err = client.DeleteFile(filePath)
				assert.NoError(t, err)

				entries, err = client.List(dirPath)
				assert.NoError(t, err)
				assert.NotEmpty(t, entries)
			},
		},
		{
			name: "RenameFile",
			doTest: func(t *testing.T) {
				fake := faker.New()
				path := common.Path(fmt.Sprintf(
					"/%s/%s/file-%d", fake.Music().Genre(), fake.File().FilenameWithExtension(), rand.Intn(1000)))
				newPath := common.Path(fmt.Sprintf(
					"/%s/%s/file-%d", fake.Music().Genre(), fake.File().FilenameWithExtension(), rand.Intn(1000)))

				handle, err := client.GetChunkHandle(common.Path(path), 0)
				require.NoError(t, err)
				require.GreaterOrEqual(t, int(handle), 0)

				data := []byte(fake.Lorem().Sentence(5))
				n, err := client.Write(path, 0, data)
				assert.NoError(t, err)
				assert.Equal(t, len(data), n)

				err = client.RenameFile(path, newPath)
				assert.NoError(t, err)

				time.Sleep(5 * time.Second)
				readBuffer := make([]byte, len(data))
				n, err = client.Read(newPath, 0, readBuffer)
				assert.NoError(t, err)
				assert.Equal(t, len(data), n)
				assert.Equal(t, data, readBuffer)

				_, err = client.GetFile(path)
				assert.Error(t, err)
			},
		},
		{
			name: "LeaseManagement",
			doTest: func(t *testing.T) {
				fake := faker.New()
				path := common.Path(fmt.Sprintf(
					"/%s/%s/file-%d", fake.Music().Genre(), fake.Music().Author(), rand.Intn(1000)))

				handle, err := client.GetChunkHandle(path, 0)
				assert.NoError(t, err)
				lease, _, err := client.ObtainLease(handle, 0)
				assert.NoError(t, err)
				assert.NotEmpty(t, lease.Primary)
				assert.False(t, lease.IsExpired(time.Now()))

				time.Sleep(10 * time.Millisecond)
				client.cacheMux.RLock()
				_, exists := client.cache[handle]
				client.cacheMux.RUnlock()
				assert.True(t, exists, "Lease should still be in cache")
			},
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.doTest(t)
		})
	}
}

func TestMain(m *testing.M) {
	rand.New(rand.NewSource(42))
	m.Run()
}
