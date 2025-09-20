package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/chunkserver"
	"github.com/caleberi/distributed-system/common"
	"github.com/caleberi/distributed-system/hercules"
	"github.com/caleberi/distributed-system/master_server"
	"github.com/gin-gonic/gin"
	"github.com/jaswdr/faker/v2"
	"github.com/rs/zerolog/log"
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

func populateServers(t *testing.T, client *hercules.HerculesClient) ([]common.ChunkHandle, []string) {
	chunkHandles := []common.ChunkHandle{}
	paths := []string{}
	fake := faker.New()

	for i := range 3 {
		genre := sanitizePathComponent(fake.Music().Genre())
		artist := sanitizePathComponent(fake.Music().Author())
		fakePath := fmt.Sprintf("/%s/%s/file-%d", genre, artist, i)

		handle, err := client.GetChunkHandle(common.Path(fakePath), 0)
		require.NoError(t, err, "Failed to get chunk handle for %s", fakePath)

		data := []byte(fake.Lorem().Paragraph(5))
		n, err := client.Write(common.Path(fakePath), 0, data)
		require.NoError(t, err, "Failed to write to %s", fakePath)
		require.Equal(t, len(data), n, "Write length mismatch for %s", fakePath)
		chunkHandles = append(chunkHandles, handle)
		paths = append(paths, fakePath)
	}

	return chunkHandles, paths
}
func sanitizePathComponent(s string) string {
	return strings.ReplaceAll(s, " ", "_")
}

func TestHerculesHTTPServerIntegration(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := t.Context()

	dirPath := t.TempDir()

	master := setupMasterServer(t, ctx, dirPath, "127.0.0.1:9090")
	slaves := []*chunkserver.ChunkServer{}
	for i := range 4 {
		slave := setupChunkServer(t, t.TempDir(),
			fmt.Sprintf("127.0.0.1:%d", 10000+i), "127.0.0.1:9090")
		slaves = append(slaves, slave)
	}

	defer func() {
		for _, slave := range slaves {
			assert.NoError(t, slave.Shutdown())
		}
		master.Shutdown()
	}()

	client := hercules.NewHerculesClient(ctx, "127.0.0.1:9090", 150*time.Millisecond)
	herculesHttpServer := NewHerculesHTTPServer(client)

	go func(addr string) {
		err := herculesHttpServer.Start(addr)
		if err != nil && err != http.ErrServerClosed {
			log.Err(err).Msg("Failed to start HTTP server")
		}
	}(":8081")

	time.Sleep(5 * time.Second)
	_, paths := populateServers(t, client)

	router := herculesHttpServer.server.Handler
	if router == nil {
		t.Fatal("server.Router is nil; ensure NewHerculesHTTPServer initializes Router field")
	}

	t.Run("MkDir_Success", func(t *testing.T) {
		fake := faker.New()
		dirPath := fmt.Sprintf("/%s/%s", fake.Music().Genre(), fake.Music().Name())
		reqBody, _ := json.Marshal(map[string]string{"path": dirPath})
		req := httptest.NewRequest(http.MethodPost, "/mkdir", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
		var resp map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err, "Failed to unmarshal response")
		assert.Equal(t, "directory created", resp["status"], "Expected directory created status")
	})

	// t.Run("CreateFile_Success", func(t *testing.T) {
	// 	fake := faker.New()
	// 	fakePath := fmt.Sprintf("/%s/%s/file-create", fake.Music().Genre(), fake.Music().Name())
	// 	reqBody, _ := json.Marshal(map[string]string{"path": fakePath})
	// 	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(reqBody))
	// 	req.Header.Set("Content-Type", "application/json")
	// 	w := httptest.NewRecorder()

	// 	router.ServeHTTP(w, req)

	// 	assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
	// 	var resp map[string]string
	// 	err := json.Unmarshal(w.Body.Bytes(), &resp)
	// 	require.NoError(t, err, "Failed to unmarshal response")
	// 	assert.Equal(t, "file created", resp["status"], "Expected file created status")
	// })

	t.Run("List_Success", func(t *testing.T) {
		fakeDirPath := path.Dir(paths[0]) // Get parent directory of a created file
		encodedPath := url.PathEscape(fakeDirPath)
		req := httptest.NewRequest(http.MethodGet, "/list/"+encodedPath, nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
		var resp map[string][]common.PathInfo
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		require.NoError(t, err, "Failed to unmarshal response")
		assert.NotEmpty(t, resp["entries"], "Expected non-empty entries")
	})

	// t.Run("DeleteFile_Success", func(t *testing.T) {
	// 	fake := faker.New()
	// 	fakePath := fmt.Sprintf("/%s/%s/file-0", fake.Music().Genre(), fake.Music().Name())
	// 	req := httptest.NewRequest(http.MethodDelete, "/delete"+fakePath, nil)
	// 	w := httptest.NewRecorder()

	// 	router.ServeHTTP(w, req)

	// 	assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
	// 	var resp map[string]string
	// 	err := json.Unmarshal(w.Body.Bytes(), &resp)
	// 	require.NoError(t, err, "Failed to unmarshal response")
	// 	assert.Equal(t, "file deleted", resp["status"], "Expected file deleted status")
	// })

	// t.Run("RenameFile_Success", func(t *testing.T) {
	// 	fake := faker.New()
	// 	genre := fake.Music().Genre()
	// .Name := fake.Music().Name()
	// 	sourcePath := fmt.Sprintf("/%s/%s/file-1", genre,.Name)
	// 	targetPath := fmt.Sprintf("/%s/%s/renamed-file", genre,.Name)
	// 	reqBody, _ := json.Marshal(map[string]string{"source": sourcePath, "target": targetPath})
	// 	req := httptest.NewRequest(http.MethodPost, "/rename", bytes.NewReader(reqBody))
	// 	req.Header.Set("Content-Type", "application/json")
	// 	w := httptest.NewRecorder()

	// 	router.ServeHTTP(w, req)

	// 	assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
	// 	var resp map[string]string
	// 	err := json.Unmarshal(w.Body.Bytes(), &resp)
	// 	require.NoError(t, err, "Failed to unmarshal response")
	// 	assert.Equal(t, "file renamed", resp["status"], "Expected file renamed status")
	// })

	// t.Run("GetFile_Success", func(t *testing.T) {
	// 	fake := faker.New()
	// 	fakePath := fmt.Sprintf("/%s/%s/file-2", fake.Music().Genre(), fake.Music().Name())
	// 	req := httptest.NewRequest(http.MethodGet, "/file"+fakePath, nil)
	// 	w := httptest.NewRecorder()

	// 	router.ServeHTTP(w, req)

	// 	assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
	// 	var resp map[string]interface{}
	// 	err := json.Unmarshal(w.Body.Bytes(), &resp)
	// 	require.NoError(t, err, "Failed to unmarshal response")
	// 	assert.False(t, resp["is_dir"].(bool), "Expected is_dir to be false")
	// 	assert.Greater(t, resp["length"].(float64), float64(0), "Expected file length > 0")
	// })

	// t.Run("Read_Success", func(t *testing.T) {
	// 	fake := faker.New()
	// 	fakePath := fmt.Sprintf("/%s/%s/file-2", fake.Music().Genre(), fake.Music().Name())
	// 	req := httptest.NewRequest(http.MethodGet, "/read"+fakePath+"?offset=0&length=100", nil)
	// 	w := httptest.NewRecorder()

	// 	router.ServeHTTP(w, req)

	// 	assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
	// 	assert.NotEmpty(t, w.Body.String(), "Expected non-empty response body")
	// })

	// t.Run("Write_Success", func(t *testing.T) {
	// 	fake := faker.New()
	// 	fakePath := fmt.Sprintf("/%s/%s/file-2", fake.Music().Genre(), fake.Music().Name())
	// 	data := []byte(fake.Lorem().Sentence(5))
	// 	req := httptest.NewRequest(http.MethodPost, "/write"+fakePath+"?offset=0", bytes.NewReader(data))
	// 	w := httptest.NewRecorder()

	// 	router.ServeHTTP(w, req)

	// 	assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
	// 	var resp map[string]int
	// 	err := json.Unmarshal(w.Body.Bytes(), &resp)
	// 	require.NoError(t, err, "Failed to unmarshal response")
	// 	assert.Equal(t, len(data), resp["bytes_written"], "Expected bytes_written to match data length")
	// })

	// t.Run("Append_Success", func(t *testing.T) {
	// 	fake := faker.New()
	// 	fakePath := fmt.Sprintf("/%s/%s/file-2", fake.Music().Genre(), fake.Music().Name())
	// 	data := []byte(fake.Lorem().Sentence(5))
	// 	req := httptest.NewRequest(http.MethodPost, "/append"+fakePath, bytes.NewReader(data))
	// 	w := httptest.NewRecorder()

	// 	router.ServeHTTP(w, req)

	// 	assert.Equal(t, http.StatusOK, w.Code, "Expected HTTP 200 OK")
	// 	var resp map[string]int64
	// 	err := json.Unmarshal(w.Body.Bytes(), &resp)
	// 	require.NoError(t, err, "Failed to unmarshal response")
	// 	assert.GreaterOrEqual(t, resp["offset"], int64(0), "Expected offset >= 0")
	// })

	herculesHttpServer.Close()
}
