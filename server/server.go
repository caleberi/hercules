package client

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/common"
	sdk "github.com/caleberi/distributed-system/hercules"
	"github.com/gin-gonic/gin"
)

// HerculesHTTPServer wraps a HerculesClient to provide an HTTP interface for file operations.
type HerculesHTTPServer struct {
	client *sdk.HerculesClient
	server *http.Server
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex // Mutex to synchronize access to client
}

// NewHerculesHTTPServer creates a new HTTP server with the given HerculesClient.
func NewHerculesHTTPServer(ctx context.Context, client *sdk.HerculesClient) *HerculesHTTPServer {
	// Create a cancellable context for better control
	ctx, cancel := context.WithCancel(ctx)
	return &HerculesHTTPServer{
		client: client,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start runs the HTTP server on the specified address, setting up routes for file operations.
func (s *HerculesHTTPServer) Start(addr string) error {
	r := gin.Default()

	// Register handlers
	r.POST("/get-handle", s.getChunkHandle)
	// r.GET("/ls", s.handleList)
	// r.DELETE("/delete/:path", s.handleDeleteFile)
	// r.POST("/rename", s.handleRenameFile)
	// r.GET("/file/:path", s.handleGetFile)
	// r.GET("/read/:path", s.handleRead)
	// r.POST("/write/:path", s.handleWrite)
	// r.POST("/append/:path", s.handleAppend)

	s.server = &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// Start server in a goroutine to allow graceful shutdown
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log or handle error (e.g., send to a logger or channel)
		}
	}()

	// Wait briefly to ensure server starts (useful for tests)
	time.Sleep(100 * time.Millisecond)

	return nil
}

// Close gracefully shuts down the HTTP server and the underlying HerculesClient.
func (s *HerculesHTTPServer) Close() error {
	// Cancel the context to signal goroutines to stop
	s.cancel()

	if s.server == nil {
		return nil
	}

	// Create a context with a timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.server.Shutdown(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Shutdown gracefully shuts down the HTTP server for testing purposes.
// It stops accepting new requests and waits for active connections to close.
// Safe to call multiple times and from concurrent test goroutines.
func (s *HerculesHTTPServer) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server == nil {
		return nil
	}

	// Cancel the context to signal goroutines to stop
	s.cancel()

	// Create a context with a timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.server.Shutdown(ctx)
	if err != nil {
		return err
	}

	// Mark server as nil to prevent further shutdown attempts
	s.server = nil

	return nil
}

// handleMkDir handles HTTP POST requests to create a directory.
func (s *HerculesHTTPServer) getChunkHandle(c *gin.Context) {
	var req struct {
		Path string `json:"path" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	fileInfo, err := s.client.GetFile(common.Path(req.Path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	handle, err := s.client.GetChunkHandle(common.Path(req.Path), common.ChunkIndex(fileInfo.Chunks))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "directory created", "handle": handle})
}

// handleCreateFile handles HTTP POST requests to create a file.
func (s *HerculesHTTPServer) handleCreateFile(c *gin.Context) {
	var req struct {
		Path string `json:"path" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.client.CreateFile(common.Path(req.Path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "file created"})
}

// handleList handles HTTP GET requests to list directory contents.
func (s *HerculesHTTPServer) handleList(c *gin.Context) {
	path := c.Query("path")

	// Lock to ensure thread-safe access to client
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := s.client.List(common.Path(path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"entries": entries})
}

// handleDeleteFile handles HTTP DELETE requests to delete a file.
func (s *HerculesHTTPServer) handleDeleteFile(c *gin.Context) {
	path := c.Param("path")

	// Lock to ensure thread-safe access to client
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.client.DeleteFile(common.Path(path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "file deleted"})
}

// handleRenameFile handles HTTP POST requests to rename a file.
func (s *HerculesHTTPServer) handleRenameFile(c *gin.Context) {
	var req struct {
		Source string `json:"source" binding:"required"`
		Target string `json:"target" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Lock to ensure thread-safe access to client
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.client.RenameFile(common.Path(req.Source), common.Path(req.Target))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "file renamed"})
}

// handleGetFile handles HTTP GET requests to retrieve file information.
func (s *HerculesHTTPServer) handleGetFile(c *gin.Context) {
	path := c.Param("path")

	// Lock to ensure thread-safe access to client
	s.mu.Lock()
	defer s.mu.Unlock()

	fileInfo, err := s.client.GetFile(common.Path(path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"is_dir": fileInfo.IsDir,
		"length": fileInfo.Length,
		"chunks": fileInfo.Chunks,
	})
}

// handleRead handles HTTP GET requests to read data from a file.
func (s *HerculesHTTPServer) handleRead(c *gin.Context) {
	path := c.Param("path")
	offsetStr := c.Query("offset")
	lengthStr := c.Query("length")

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid offset"})
		return
	}

	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid length"})
		return
	}

	data := make([]byte, length)

	// Lock to ensure thread-safe access to client
	s.mu.Lock()
	defer s.mu.Unlock()

	n, err := s.client.Read(common.Path(path), common.Offset(offset), data)
	if err != nil && err != io.EOF {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", data[:n])
}

// handleWrite handles HTTP POST requests to write data to a file.
func (s *HerculesHTTPServer) handleWrite(c *gin.Context) {
	path := c.Param("path")
	offsetStr := c.Query("offset")

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid offset"})
		return
	}

	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	// Lock to ensure thread-safe access to client
	s.mu.Lock()
	defer s.mu.Unlock()

	n, err := s.client.Write(common.Path(path), common.Offset(offset), data)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"bytes_written": n})
}

// handleAppend handles HTTP POST requests to append data to a file.
func (s *HerculesHTTPServer) handleAppend(c *gin.Context) {
	path := c.Param("path")
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	// Lock to ensure thread-safe access to client
	s.mu.Lock()
	defer s.mu.Unlock()

	offset, err := s.client.Append(common.Path(path), data)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"offset": offset})
}
