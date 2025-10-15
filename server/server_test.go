package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/chunkserver"
	"github.com/caleberi/distributed-system/common"
	"github.com/caleberi/distributed-system/hercules"
	"github.com/caleberi/distributed-system/master_server"
	"github.com/jaswdr/faker/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMasterServerForGateway(t *testing.T, ctx context.Context, root, address string) *master_server.MasterServer {
	assert.NotEmpty(t, root)
	assert.NotEmpty(t, address)

	server := master_server.NewMasterServer(ctx, common.ServerAddr(address), root)
	assert.NotNil(t, server)
	return server
}

func setupChunkServerForGateway(t *testing.T, root, address, masterAddress string) *chunkserver.ChunkServer {
	assert.NotEmpty(t, root)
	assert.NotEmpty(t, address)
	assert.NotEmpty(t, masterAddress)

	server, err := chunkserver.NewChunkServer(common.ServerAddr(address), common.ServerAddr(masterAddress), root)
	require.NoError(t, err)
	return server
}

func populateGatewayTestData(t *testing.T, client *hercules.HerculesClient) ([]common.ChunkHandle, []string) {
	chunkHandles := []common.ChunkHandle{}
	paths := []string{}
	fake := faker.New()

	for i := range 3 {
		genre := sanitizePathForGateway(fake.Music().Genre())
		artist := sanitizePathForGateway(fake.Music().Author())
		fakePath := fmt.Sprintf("/%s/%s/file-%d.txt", genre, artist, i)

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

func sanitizePathForGateway(s string) string {
	return strings.ReplaceAll(s, " ", "_")
}

func TestHerculesHTTPGatewayIntegration(t *testing.T) {
	ctx := t.Context()
	masterDir := t.TempDir()

	masterAddr := "127.0.0.1:9091"
	master := setupMasterServerForGateway(t, ctx, masterDir, masterAddr)

	slaves := []*chunkserver.ChunkServer{}
	for i := range 4 {
		slave := setupChunkServerForGateway(
			t,
			t.TempDir(),
			fmt.Sprintf("127.0.0.1:%d", 11000+i),
			masterAddr,
		)
		slaves = append(slaves, slave)
	}

	defer func() {
		for _, slave := range slaves {
			assert.NoError(t, slave.Shutdown())
		}
		master.Shutdown()
	}()

	client := hercules.NewHerculesClient(ctx, common.ServerAddr(masterAddr), 150*time.Millisecond)

	config := DefaultGatewayConfig()
	config.ServerName = "test-gateway"
	config.Address = 8082
	config.Logger = os.Stdout
	config.EnableTLS = false
	config.ReadTimeout = 10 * time.Second
	config.WriteTimeout = 10 * time.Second

	gateway := NewHerculesHTTPGateway(ctx, client, config)
	gateway.Start()

	time.Sleep(2 * time.Second)

	defer func() {
		err := gateway.Shutdown()
		assert.NoError(t, err, "Gateway shutdown failed")
	}()

	_, paths := populateGatewayTestData(t, client)
	baseURL := "http://127.0.0.1:8082"

	t.Run("Health_Check", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/health")
		require.NoError(t, err, "Health check request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result map[string]any
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode health check response")
		assert.Equal(t, "healthy", result["status"], "Expected healthy status")
	})

	t.Run("MkDir_Success", func(t *testing.T) {
		fake := faker.New()
		dirPath := fmt.Sprintf("/%s/%s",
			sanitizePathForGateway(fake.Music().Genre()),
			sanitizePathForGateway(fake.Music().Name()))

		reqBody, _ := json.Marshal(map[string]string{"path": dirPath})
		resp, err := http.Post(
			baseURL+"/api/v1/mkdir",
			"application/json",
			bytes.NewReader(reqBody),
		)
		require.NoError(t, err, "MkDir request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")
		assert.Contains(t, result.Message, "Directory created", "Expected directory created message")
	})

	t.Run("CreateFile_Success", func(t *testing.T) {
		fake := faker.New()
		filePath := fmt.Sprintf("/%s/%s/test-file.txt",
			sanitizePathForGateway(fake.Music().Genre()),
			sanitizePathForGateway(fake.Music().Name()))

		reqBody, _ := json.Marshal(map[string]string{"path": filePath})
		resp, err := http.Post(
			baseURL+"/api/v1/create",
			"application/json",
			bytes.NewReader(reqBody),
		)
		require.NoError(t, err, "CreateFile request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.Truef(t, result.Success, "Expected success=true,%v", err)
		assert.Contains(t, result.Message, "File created", "Expected file created message")
	})

	t.Run("List_Success", func(t *testing.T) {
		if len(paths) == 0 {
			t.Skip("No paths available for listing")
		}

		dirPath := path.Dir(paths[0])
		encodedPath := url.QueryEscape(dirPath)

		resp, err := http.Get(fmt.Sprintf("%s/api/v1/list?path=%s", baseURL, encodedPath))
		require.NoError(t, err, "List request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")

		data, ok := result.Data.(map[string]any)
		require.True(t, ok, "Expected data to be a map")

		entries, ok := data["entries"]
		require.True(t, ok, "Expected entries field in response")
		assert.NotNil(t, entries, "Expected non-nil entries")
	})

	t.Run("GetFileInfo_Success", func(t *testing.T) {
		if len(paths) == 0 {
			t.Skip("No paths available for file info")
		}

		filePath := paths[0]
		encodedPath := url.QueryEscape(filePath)

		resp, err := http.Get(fmt.Sprintf("%s/api/v1/fileinfo?path=%s", baseURL, encodedPath))
		require.NoError(t, err, "GetFileInfo request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")

		data, ok := result.Data.(map[string]any)
		require.True(t, ok, "Expected data to be a map")

		_, hasIsDir := data["is_dir"]
		_, hasLength := data["length"]
		_, hasChunks := data["chunks"]

		assert.True(t, hasIsDir, "Expected is_dir field")
		assert.True(t, hasLength, "Expected length field")
		assert.True(t, hasChunks, "Expected chunks field")
	})

	t.Run("Write_Success", func(t *testing.T) {
		if len(paths) == 0 {
			t.Skip("No paths available for writing")
		}

		filePath := paths[0]
		testData := []byte("Hello from HTTP Gateway test!")

		reqURL := fmt.Sprintf("%s/api/v1/write?path=%s&offset=0",
			baseURL,
			url.QueryEscape(filePath))

		resp, err := http.Post(reqURL, "application/octet-stream", bytes.NewReader(testData))
		require.NoError(t, err, "Write request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")

		data, ok := result.Data.(map[string]any)
		require.True(t, ok, "Expected data to be a map")

		bytesWritten, ok := data["bytes_written"].(float64)
		require.True(t, ok, "Expected bytes_written field")
		assert.Equal(t, float64(len(testData)), bytesWritten, "Bytes written mismatch")
	})

	t.Run("Read_Success", func(t *testing.T) {
		if len(paths) == 0 {
			t.Skip("No paths available for reading")
		}

		filePath := paths[0]
		reqURL := fmt.Sprintf("%s/api/v1/read?path=%s&offset=0&length=100",
			baseURL,
			url.QueryEscape(filePath))

		resp, err := http.Get(reqURL)
		require.NoError(t, err, "Read request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")
		assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))

		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read response body")
		assert.NotEmpty(t, data, "Expected non-empty response body")
	})

	t.Run("Append_Success", func(t *testing.T) {
		if len(paths) == 0 {
			t.Skip("No paths available for appending")
		}

		filePath := paths[0]
		appendData := []byte(" Appended data!")

		reqURL := fmt.Sprintf("%s/api/v1/append?path=%s",
			baseURL,
			url.QueryEscape(filePath))

		resp, err := http.Post(reqURL, "application/octet-stream", bytes.NewReader(appendData))
		require.NoError(t, err, "Append request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")

		data, ok := result.Data.(map[string]any)
		require.True(t, ok, "Expected data to be a map")

		offset, ok := data["offset"]
		require.True(t, ok, "Expected offset field")
		assert.NotNil(t, offset, "Expected non-nil offset")
	})

	t.Run("GetChunkHandle_Success", func(t *testing.T) {
		if len(paths) == 0 {
			t.Skip("No paths available for chunk handle")
		}

		filePath := paths[0]
		reqURL := fmt.Sprintf("%s/api/v1/chunk/handle?path=%s&index=0",
			baseURL,
			url.QueryEscape(filePath))

		resp, err := http.Get(reqURL)
		require.NoError(t, err, "GetChunkHandle request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")

		data, ok := result.Data.(map[string]any)
		require.True(t, ok, "Expected data to be a map")

		handle, ok := data["handle"]
		require.True(t, ok, "Expected handle field")
		assert.NotNil(t, handle, "Expected non-nil handle")
	})

	t.Run("GetChunkServers_Success", func(t *testing.T) {
		if len(paths) == 0 {
			t.Skip("No paths available for chunk servers")
		}

		filePath := paths[0]
		handle, err := client.GetChunkHandle(common.Path(filePath), 0)
		require.NoError(t, err, "Failed to get chunk handle")

		reqURL := fmt.Sprintf("%s/api/v1/chunk/servers?handle=%d", baseURL, handle)

		resp, err := http.Get(reqURL)
		require.NoError(t, err, "GetChunkServers request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")

		data, ok := result.Data.(map[string]any)
		require.True(t, ok, "Expected data to be a map")

		primary, hasPrimary := data["primary"]
		secondaries, hasSecondaries := data["secondaries"]

		assert.True(t, hasPrimary, "Expected primary field")
		assert.True(t, hasSecondaries, "Expected secondaries field")
		assert.NotNil(t, primary, "Expected non-nil primary")
		assert.NotNil(t, secondaries, "Expected non-nil secondaries")
	})

	t.Run("RenameFile_Success", func(t *testing.T) {
		fake := faker.New()
		genre := sanitizePathForGateway(fake.Music().Genre())
		name := sanitizePathForGateway(fake.Music().Name())

		sourcePath := fmt.Sprintf("/%s/%s/original.txt", genre, name)
		targetPath := fmt.Sprintf("/%s/%s/renamed.txt", genre, name)

		createBody, _ := json.Marshal(map[string]string{"path": sourcePath})
		createResp, err := http.Post(
			baseURL+"/api/v1/create",
			"application/json",
			bytes.NewReader(createBody),
		)
		require.NoError(t, err, "Failed to create source file")
		assert.Equal(t, http.StatusOK, createResp.StatusCode, "Expected HTTP 200 OK")
		createResp.Body.Close()

		encodedPath := url.QueryEscape(".")

		resp, err := http.Get(fmt.Sprintf("%s/api/v1/list?path=%s", baseURL, encodedPath))
		require.NoError(t, err, "List request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")

		data, ok := result.Data.(map[string]any)
		require.True(t, ok, "Expected data to be a map")

		entries, ok := data["entries"]
		log.Printf("entries: \n %v\n", entries)
		require.True(t, ok, "Expected entries field in response")
		assert.NotNil(t, entries, "Expected non-nil entries")

		// time.Sleep(10 * time.Millisecond)
		renameBody, _ := json.Marshal(map[string]string{
			"source": sourcePath,
			"target": targetPath,
		})

		resp, err = http.Post(
			baseURL+"/api/v1/rename",
			"application/json",
			bytes.NewReader(renameBody),
		)
		require.NoError(t, err, "Rename request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		// var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")
		assert.Contains(t, result.Message, "renamed", "Expected rename message")

		resp, err = http.Get(fmt.Sprintf("%s/api/v1/list?path=%s", baseURL, encodedPath))
		require.NoError(t, err, "List request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")

		data, ok = result.Data.(map[string]any)
		require.True(t, ok, "Expected data to be a map")

		entries, ok = data["entries"]
		log.Printf("entries: \n %v\n", entries)
		require.True(t, ok, "Expected entries field in response")
		assert.NotNil(t, entries, "Expected non-nil entries")

	})

	t.Run("DeleteFile_Success", func(t *testing.T) {
		fake := faker.New()
		filePath := fmt.Sprintf("/%s/%s/to-delete.txt",
			sanitizePathForGateway(fake.Music().Genre()),
			sanitizePathForGateway(fake.Music().Name()))

		createBody, _ := json.Marshal(map[string]string{"path": filePath})
		createResp, err := http.Post(
			baseURL+"/api/v1/create",
			"application/json",
			bytes.NewReader(createBody),
		)
		require.NoError(t, err, "Failed to create file for deletion")
		createResp.Body.Close()

		deleteBody, _ := json.Marshal(map[string]string{"path": filePath})
		req, err := http.NewRequest(
			http.MethodDelete,
			baseURL+"/api/v1/delete",
			bytes.NewReader(deleteBody),
		)
		require.NoError(t, err, "Failed to create delete request")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err, "Delete request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected HTTP 200 OK")

		var result SuccessResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode response")
		assert.True(t, result.Success, "Expected success=true")
		assert.Contains(t, result.Message, "deleted", "Expected delete message")
	})

	t.Run("Error_PathRequired", func(t *testing.T) {
		reqBody, _ := json.Marshal(map[string]string{"path": ""})
		resp, err := http.Post(
			baseURL+"/api/v1/mkdir",
			"application/json",
			bytes.NewReader(reqBody),
		)
		require.NoError(t, err, "Request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "Expected HTTP 400 Bad Request")

		var result ErrorResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err, "Failed to decode error response")
		assert.NotEmpty(t, result.Error, "Expected error message")
	})

	t.Run("Error_MethodNotAllowed", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/api/v1/mkdir")
		require.NoError(t, err, "Request failed")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, "Expected HTTP 405 Method Not Allowed")
	})
}

func BenchmarkGatewayWrite(b *testing.B) {
	ctx := context.Background()
	masterDir := b.TempDir()
	masterAddr := "127.0.0.1:9092"

	master := master_server.NewMasterServer(ctx, common.ServerAddr(masterAddr), masterDir)
	defer master.Shutdown()

	slaves := []*chunkserver.ChunkServer{}
	for i := range 4 {
		slave, err := chunkserver.NewChunkServer(
			common.ServerAddr(fmt.Sprintf("127.0.0.1:%d", 12000+i)),
			common.ServerAddr(masterAddr),
			b.TempDir(),
		)
		if err != nil {
			b.Fatal(err)
		}
		slaves = append(slaves, slave)
	}
	defer func() {
		for _, slave := range slaves {
			slave.Shutdown()
		}
	}()

	client := hercules.NewHerculesClient(ctx, common.ServerAddr(masterAddr), 150*time.Millisecond)

	config := DefaultGatewayConfig()
	config.Address = 8083
	config.Logger = zerolog.Nop()

	gateway := NewHerculesHTTPGateway(ctx, client, config)
	gateway.Start()
	defer gateway.Shutdown()

	time.Sleep(2 * time.Second)

	baseURL := "http://127.0.0.1:8083"
	testData := bytes.Repeat([]byte("test"), 256)

	b.Logf("Creating %d test files and getting chunk handles...", b.N)
	for i := 0; b.Loop(); i++ {
		filePath := fmt.Sprintf("/bench/file-%d.txt", i)

		handle, err := client.GetChunkHandle(common.Path(filePath), 0)
		if err != nil {
			b.Fatalf("Failed to get chunk handle for %s: %v", filePath, err)
		}

		if handle < 0 {
			b.Fatalf("Invalid chunk handle %d for file %s", handle, filePath)
		}
	}
	b.Logf("File creation and chunk handle retrieval complete")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		filePath := fmt.Sprintf("/bench/file-%d.txt", i)
		reqURL := fmt.Sprintf("%s/api/v1/write?path=%s&offset=0", baseURL, url.QueryEscape(filePath))

		resp, err := http.Post(reqURL, "application/octet-stream", bytes.NewReader(testData))
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		resp.Body.Close()
	}
}

func BenchmarkGatewayRead(b *testing.B) {
	ctx := context.Background()
	masterDir := b.TempDir()
	masterAddr := "127.0.0.1:9093"

	master := master_server.NewMasterServer(ctx, common.ServerAddr(masterAddr), masterDir)
	defer master.Shutdown()

	slaves := []*chunkserver.ChunkServer{}
	for i := range 4 {
		slave, err := chunkserver.NewChunkServer(
			common.ServerAddr(fmt.Sprintf("127.0.0.1:%d", 13000+i)),
			common.ServerAddr(masterAddr),
			b.TempDir(),
		)
		if err != nil {
			b.Fatal(err)
		}
		slaves = append(slaves, slave)
	}
	defer func() {
		for _, slave := range slaves {
			slave.Shutdown()
		}
	}()

	client := hercules.NewHerculesClient(ctx, common.ServerAddr(masterAddr), 150*time.Millisecond)

	config := DefaultGatewayConfig()
	config.Address = 8084
	config.Logger = zerolog.Nop()

	gateway := NewHerculesHTTPGateway(ctx, client, config)
	gateway.Start()
	defer gateway.Shutdown()

	time.Sleep(2 * time.Second)

	baseURL := "http://127.0.0.1:8084"
	testData := bytes.Repeat([]byte("benchmark data "), 64) // 1KB of data

	b.Logf("Creating and populating %d test files...", b.N)
	for i := 0; b.Loop(); i++ {
		filePath := fmt.Sprintf("/bench-read/file-%d.txt", i)

		handle, err := client.GetChunkHandle(common.Path(filePath), 0)
		if err != nil {
			b.Fatalf("Failed to get chunk handle: %v", err)
		}
		if handle < 0 {
			b.Fatalf("Invalid chunk handle %d", handle)
		}

		n, err := client.Write(common.Path(filePath), 0, testData)
		if err != nil {
			b.Fatalf("Failed to write file: %v", err)
		}
		if n != len(testData) {
			b.Fatalf("Write size mismatch: expected %d, got %d", len(testData), n)
		}
	}
	b.Logf("File population complete")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		filePath := fmt.Sprintf("/bench-read/file-%d.txt", i)
		reqURL := fmt.Sprintf("%s/api/v1/read?path=%s&offset=0&length=1024",
			baseURL,
			url.QueryEscape(filePath))

		resp, err := http.Get(reqURL)
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}
		io.ReadAll(resp.Body)
		resp.Body.Close()
	}
}

func BenchmarkGatewayAppend(b *testing.B) {
	ctx := context.Background()
	masterDir := b.TempDir()
	masterAddr := "127.0.0.1:9094"

	master := master_server.NewMasterServer(ctx, common.ServerAddr(masterAddr), masterDir)
	defer master.Shutdown()

	slaves := []*chunkserver.ChunkServer{}
	for i := range 4 {
		slave, err := chunkserver.NewChunkServer(
			common.ServerAddr(fmt.Sprintf("127.0.0.1:%d", 14000+i)),
			common.ServerAddr(masterAddr),
			b.TempDir(),
		)
		if err != nil {
			b.Fatal(err)
		}
		slaves = append(slaves, slave)
	}
	defer func() {
		for _, slave := range slaves {
			slave.Shutdown()
		}
	}()

	client := hercules.NewHerculesClient(ctx, common.ServerAddr(masterAddr), 150*time.Millisecond)

	config := DefaultGatewayConfig()
	config.Address = 8085
	config.Logger = zerolog.Nop()

	gateway := NewHerculesHTTPGateway(ctx, client, config)
	gateway.Start()
	defer gateway.Shutdown()

	time.Sleep(2 * time.Second)

	baseURL := "http://127.0.0.1:8085"
	appendData := []byte("appended data\n")

	filePath := "/bench-append/shared-file.txt"

	handle, err := client.GetChunkHandle(common.Path(filePath), 0)
	if err != nil {
		b.Fatalf("Failed to get chunk handle: %v", err)
	}
	if handle < 0 {
		b.Fatalf("Invalid chunk handle %d", handle)
	}

	b.Logf("File created with chunk handle %d", handle)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reqURL := fmt.Sprintf("%s/api/v1/append?path=%s", baseURL, url.QueryEscape(filePath))

		resp, err := http.Post(reqURL, "application/octet-stream", bytes.NewReader(appendData))
		if err != nil {
			b.Fatalf("Append failed: %v", err)
		}
		resp.Body.Close()
	}
}
