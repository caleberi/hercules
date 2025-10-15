package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/common"
	"github.com/caleberi/distributed-system/engine"
	sdk "github.com/caleberi/distributed-system/hercules"
	"github.com/rs/zerolog"
)

// HerculesHTTPGateway provides an HTTP interface for the Hercules distributed file system.
// It translates HTTP requests into HerculesClient RPC calls, following GFS specifications.
type HerculesHTTPGateway struct {
	client *sdk.HerculesClient
	server *engine.Server
	mux    *http.ServeMux
	ctx    context.Context
	cancel context.CancelFunc
	logger zerolog.Logger
	mu     sync.RWMutex
}

// GatewayConfig defines configuration options for the HTTP gateway.
type GatewayConfig struct {
	ServerName     string        // Server name for logging
	Address        int           // Server port
	Logger         io.Writer     // Logger writer
	TlsDir         string        // TLS certificate directory
	EnableTLS      bool          // Enable TLS
	MaxHeaderBytes int           // Maximum header size
	ReadTimeout    time.Duration // HTTP read timeout
	WriteTimeout   time.Duration // HTTP write timeout
	IdleTimeout    time.Duration // HTTP idle timeout
}

// DefaultGatewayConfig returns sensible default configuration values.
func DefaultGatewayConfig() GatewayConfig {
	return GatewayConfig{
		ServerName:     "hercules-gateway",
		Address:        8080,
		Logger:         zerolog.Nop(),
		TlsDir:         "",
		EnableTLS:      false,
		MaxHeaderBytes: 1 << 20, // 1 MB
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    120 * time.Second,
	}
}

// NewHerculesHTTPGateway creates a new HTTP gateway for the Hercules file system.
func NewHerculesHTTPGateway(
	ctx context.Context, client *sdk.HerculesClient, config GatewayConfig) *HerculesHTTPGateway {
	actx, cancel := context.WithCancel(ctx)

	mux := http.NewServeMux()

	gateway := &HerculesHTTPGateway{
		client: client,
		ctx:    actx,
		cancel: cancel,
		mux:    mux,
		mu:     sync.RWMutex{},
	}

	gateway.registerRoutes()

	loggerWriter := config.Logger
	if loggerWriter == nil {
		loggerWriter = zerolog.Nop()
	}

	gateway.server = engine.NewServer(
		config.ServerName,
		config.Address,
		loggerWriter,
		config.TlsDir,
		engine.ServerOpts{
			EnableTls:                    config.EnableTLS,
			MaxHeaderBytes:               config.MaxHeaderBytes,
			ReadHeaderTimeout:            config.ReadTimeout,
			WriteTimeout:                 config.WriteTimeout,
			IdleTimeout:                  config.IdleTimeout,
			DisableGeneralOptionsHandler: false,
			UseColorizedLogger:           true,
		},
	)

	gateway.server.Mux = mux

	if config.Logger != nil {
		gateway.logger = zerolog.New(config.Logger).With().Timestamp().Logger()
	} else {
		gateway.logger = zerolog.Nop()
	}

	return gateway
}

// registerRoutes sets up all HTTP endpoints following GFS operations.
func (g *HerculesHTTPGateway) registerRoutes() {

	// Chunk operations
	g.mux.HandleFunc("/api/v1/chunk/handle", g.handleGetChunkHandle)
	g.mux.HandleFunc("/api/v1/chunk/servers", g.handleGetChunkServers)

	// File system operations
	g.mux.HandleFunc("/api/v1/mkdir", g.handleMkDir)
	g.mux.HandleFunc("/api/v1/create", g.handleCreateFile)
	g.mux.HandleFunc("/api/v1/delete", g.handleDeleteFile)
	g.mux.HandleFunc("/api/v1/rename", g.handleRenameFile)
	g.mux.HandleFunc("/api/v1/list", g.handleList)
	g.mux.HandleFunc("/api/v1/fileinfo", g.handleGetFileInfo)

	// Data operations
	g.mux.HandleFunc("/api/v1/read", g.handleRead)
	g.mux.HandleFunc("/api/v1/write", g.handleWrite)
	g.mux.HandleFunc("/api/v1/append", g.handleAppend)

	// Health check
	g.mux.HandleFunc("/health", g.handleHealth)
}

// Start begins listening for HTTP requests using engine.Server.
func (g *HerculesHTTPGateway) Start() {
	g.logger.Info().
		Str("address", string(g.server.Address)).
		Str("server_name", g.server.ServerName).
		Msg("Starting Hercules HTTP Gateway")

	go g.server.Serve()
}

// Shutdown gracefully stops the HTTP server using engine.Server's shutdown.
func (g *HerculesHTTPGateway) Shutdown() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.cancel()

	if g.server == nil {
		return nil
	}

	g.logger.Info().Msg("Shutting down Hercules HTTP Gateway")
	g.server.Shutdown()

	return nil
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type SuccessResponse struct {
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Message string `json:"message,omitempty"`
}

func (g *HerculesHTTPGateway) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (g *HerculesHTTPGateway) respondError(w http.ResponseWriter, status int, err error) {
	g.logger.Error().Err(err).Int("status", status).Msg("Request error")
	g.respondJSON(w, status, ErrorResponse{
		Error:   err.Error(),
		Message: "Operation failed",
	})
}

func (g *HerculesHTTPGateway) handleMkDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	var req struct {
		Path string `json:"path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, http.StatusBadRequest, err)
		return
	}

	if req.Path == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("path is required"))
		return
	}

	g.mu.RLock()
	err := g.client.MkDir(common.Path(req.Path))
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Message: "Directory created successfully",
		Data:    map[string]string{"path": req.Path},
	})
}

func (g *HerculesHTTPGateway) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	var req struct {
		Path string `json:"path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, http.StatusBadRequest, err)
		return
	}

	if req.Path == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("path is required"))
		return
	}

	dirPath := path.Dir(req.Path)
	var err error
	if dirPath != "/" && dirPath != "." {
		g.mu.Lock()
		err = g.client.MkDir(common.Path(req.Path))
		g.mu.Unlock()
		if err != nil {
			g.respondError(w, http.StatusInternalServerError, err)
			return
		}
	}

	g.mu.Lock()
	err = g.client.CreateFile(common.Path(req.Path))
	g.mu.Unlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	// g.mu.RLock()
	// entries, err := g.client.List(common.Path(req.Path))
	// g.mu.RUnlock()
	// if err != nil {
	// 	g.respondError(w, http.StatusInternalServerError, err)
	// 	return
	// }

	// for i, e := range entries {
	// 	log.Printf("entry{%d} - %s\n", i, e.Path)
	// }

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Message: "File created successfully",
		Data:    map[string]string{"path": req.Path},
	})
}

func (g *HerculesHTTPGateway) handleDeleteFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	var req struct {
		Path string `json:"path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, http.StatusBadRequest, err)
		return
	}

	if req.Path == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("path is required"))
		return
	}

	g.mu.RLock()
	err := g.client.DeleteFile(common.Path(req.Path), false)
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Message: "File deleted successfully",
		Data:    map[string]string{"path": req.Path},
	})
}

func (g *HerculesHTTPGateway) handleRenameFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	var req struct {
		Source string `json:"source"`
		Target string `json:"target"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.respondError(w, http.StatusBadRequest, err)
		return
	}

	if req.Source == "" || req.Target == "" {
		g.respondError(
			w, http.StatusBadRequest,
			fmt.Errorf("source and target paths are required"))
		return
	}

	g.mu.RLock()
	err := g.client.RenameFile(common.Path(req.Source), common.Path(req.Target))
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Message: "File renamed successfully",
		Data: map[string]string{
			"source": req.Source,
			"target": req.Target,
		},
	})
}

func (g *HerculesHTTPGateway) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		path = "/"
	}

	g.mu.RLock()
	entries, err := g.client.List(common.Path(path))
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Data: map[string]any{
			"path":    path,
			"entries": entries,
		},
	})
}

func (g *HerculesHTTPGateway) handleGetFileInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("path is required"))
		return
	}

	g.mu.RLock()
	fileInfo, err := g.client.GetFile(common.Path(path))
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Data: map[string]any{
			"path":   path,
			"is_dir": fileInfo.IsDir,
			"length": fileInfo.Length,
			"chunks": fileInfo.Chunks,
		},
	})
}

func (g *HerculesHTTPGateway) handleRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	path := r.URL.Query().Get("path")
	offsetStr := r.URL.Query().Get("offset")
	lengthStr := r.URL.Query().Get("length")

	if path == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("path is required"))
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		offset = 0
	}

	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil || length <= 0 {
		length = 4096
	}

	data := make([]byte, length)

	g.mu.RLock()
	n, err := g.client.Read(common.Path(path), common.Offset(offset), data)
	g.mu.RUnlock()

	if err != nil && err != io.EOF {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Bytes-Read", strconv.Itoa(n))
	w.WriteHeader(http.StatusOK)
	w.Write(data[:n])
}

func (g *HerculesHTTPGateway) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	path := r.URL.Query().Get("path")
	offsetStr := r.URL.Query().Get("offset")

	if path == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("path is required"))
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		offset = 0
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		g.respondError(w, http.StatusBadRequest, err)
		return
	}

	g.mu.RLock()
	n, err := g.client.Write(common.Path(path), common.Offset(offset), data)
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Message: "Data written successfully",
		Data: map[string]any{
			"bytes_written": n,
			"offset":        offset,
		},
	})
}

func (g *HerculesHTTPGateway) handleAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("path is required"))
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		g.respondError(w, http.StatusBadRequest, err)
		return
	}

	g.mu.RLock()
	offset, err := g.client.Append(common.Path(path), data)
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Message: "Data appended successfully",
		Data: map[string]any{
			"offset":      offset,
			"bytes_added": len(data),
		},
	})
}

func (g *HerculesHTTPGateway) handleGetChunkHandle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	path := r.URL.Query().Get("path")
	indexStr := r.URL.Query().Get("index")

	if path == "" || indexStr == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("path and index are required"))
		return
	}

	index, err := strconv.ParseInt(indexStr, 10, 64)
	if err != nil {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("invalid index"))
		return
	}

	g.mu.RLock()
	handle, err := g.client.GetChunkHandle(common.Path(path), common.ChunkIndex(index))
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Data: map[string]any{
			"handle": handle,
			"path":   path,
			"index":  index,
		},
	})
}

func (g *HerculesHTTPGateway) handleGetChunkServers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		g.respondError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	handleStr := r.URL.Query().Get("handle")
	if handleStr == "" {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("handle is required"))
		return
	}

	handle, err := strconv.ParseInt(handleStr, 10, 64)
	if err != nil {
		g.respondError(w, http.StatusBadRequest, fmt.Errorf("invalid handle"))
		return
	}

	g.mu.RLock()
	lease, err := g.client.GetChunkServers(common.ChunkHandle(handle))
	g.mu.RUnlock()

	if err != nil {
		g.respondError(w, http.StatusInternalServerError, err)
		return
	}

	g.respondJSON(w, http.StatusOK, SuccessResponse{
		Success: true,
		Data: map[string]any{
			"handle":      lease.Handle,
			"primary":     lease.Primary,
			"secondaries": lease.Secondaries,
			"expire":      lease.Expire,
		},
	})
}

func (g *HerculesHTTPGateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	g.respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "healthy",
		"service": "hercules-http-gateway",
		"time":    time.Now().Unix(),
	})
}
