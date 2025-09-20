package client

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/caleberi/distributed-system/common"
	sdk "github.com/caleberi/distributed-system/hercules"
	"github.com/gin-gonic/gin"
)

// HerculesHTTPServer wraps a HerculesClient to provide an HTTP interface for file operations.
type HerculesHTTPServer struct {
	client *sdk.HerculesClient
	server *http.Server
}

// NewHerculesHTTPServer creates a new HTTP server with the given HerculesClient.
//
// Parameters:
//   - client: The HerculesClient instance to handle file operations.
//
// Returns:
//   - A pointer to the initialized HerculesHTTPServer.
func NewHerculesHTTPServer(client *sdk.HerculesClient) *HerculesHTTPServer {
	return &HerculesHTTPServer{
		client: client,
	}
}

// Start runs the HTTP server on the specified address, setting up routes for file operations.
//
// Parameters:
//   - addr: The address (host:port) to bind the HTTP server to.
//
// Returns:
//   - An error if the server fails to start.
func (s *HerculesHTTPServer) Start(addr string) error {
	r := gin.Default()

	r.POST("/mkdir", s.handleMkDir)
	r.POST("/create", s.handleCreateFile)
	r.GET("/list/:path", s.handleList)
	r.DELETE("/delete/:path", s.handleDeleteFile)
	r.POST("/rename", s.handleRenameFile)
	r.GET("/file/:path", s.handleGetFile)
	r.GET("/read/:path", s.handleRead)
	r.POST("/write/:path", s.handleWrite)
	r.POST("/append/:path", s.handleAppend)

	s.server = &http.Server{
		Addr:    addr,
		Handler: r,
	}

	return r.Run(addr)
}

// Close gracefully shuts down the HTTP server and the underlying HerculesClient.
//
// It stops accepting new requests, closes active connections, and cancels the client's context.
// The method allows up to 5 seconds for connections to close gracefully.
//
// Returns:
//   - An error if the server fails to shut down properly.
func (s *HerculesHTTPServer) Close() error {
	if s.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.server.Shutdown(ctx)
	if err != nil {
		return err
	}

	return nil
}

// handleMkDir handles HTTP POST requests to create a directory.
//
// It expects a JSON body with a "path" field specifying the directory path.
// Returns a 200 OK response on success, or an error response (400 or 500) on failure.
func (s *HerculesHTTPServer) handleMkDir(c *gin.Context) {
	var req struct {
		Path string `json:"path" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := s.client.MkDir(common.Path(req.Path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "directory created"})
}

// handleCreateFile handles HTTP POST requests to create a file.
//
// It expects a JSON body with a "path" field specifying the file path.
// Returns a 200 OK response on success, or an error response (400 or 500) on failure.
func (s *HerculesHTTPServer) handleCreateFile(c *gin.Context) {
	var req struct {
		Path string `json:"path" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := s.client.CreateFile(common.Path(req.Path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "file created"})
}

// handleList handles HTTP GET requests to list directory contents.
//
// It expects a path parameter in the URL.
// Returns a 200 OK response with the directory entries, or a 500 error on failure.
func (s *HerculesHTTPServer) handleList(c *gin.Context) {
	path := c.Param("path")
	entries, err := s.client.List(common.Path(path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"entries": entries})
}

// handleDeleteFile handles HTTP DELETE requests to delete a file.
//
// It expects a path parameter in the URL.
// Returns a 200 OK response on success, or a 500 error on failure.
func (s *HerculesHTTPServer) handleDeleteFile(c *gin.Context) {
	path := c.Param("path")
	err := s.client.DeleteFile(common.Path(path))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "file deleted"})
}

// handleRenameFile handles HTTP POST requests to rename a file.
//
// It expects a JSON body with "source" and "target" fields specifying the old and new paths.
// Returns a 200 OK response on success, or an error response (400 or 500) on failure.
func (s *HerculesHTTPServer) handleRenameFile(c *gin.Context) {
	var req struct {
		Source string `json:"source" binding:"required"`
		Target string `json:"target" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := s.client.RenameFile(common.Path(req.Source), common.Path(req.Target))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "file renamed"})
}

// handleGetFile handles HTTP GET requests to retrieve file information.
//
// It expects a path parameter in the URL.
// Returns a 200 OK response with file details (is_dir, length, chunks), or a 500 error on failure.
func (s *HerculesHTTPServer) handleGetFile(c *gin.Context) {
	path := c.Param("path")
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
//
// It expects a path parameter in the URL and query parameters "offset" and "length".
// Returns the read data as binary content with a 200 OK response, or an error response (400 or 500) on failure.
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
	n, err := s.client.Read(common.Path(path), common.Offset(offset), data)
	if err != nil && err != io.EOF {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", data[:n])
}

// handleWrite handles HTTP POST requests to write data to a file.
//
// It expects a path parameter in the URL, an "offset" query parameter, and binary data in the request body.
// Returns a 200 OK response with the number of bytes written, or an error response (400 or 500) on failure.
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

	n, err := s.client.Write(common.Path(path), common.Offset(offset), data)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"bytes_written": n})
}

// handleAppend handles HTTP POST requests to append data to a file.
//
// It expects a path parameter in the URL and binary data in the request body.
// Returns a 200 OK response with the offset where data was appended, or an error response (400 or 500) on failure.
func (s *HerculesHTTPServer) handleAppend(c *gin.Context) {
	path := c.Param("path")
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	offset, err := s.client.Append(common.Path(path), data)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"offset": offset})
}
