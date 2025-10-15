package engine

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

// ServerAddress represents the network address for an HTTP server.
type ServerAddress string

// ServerOpts defines configuration options for the HTTP server.
type ServerOpts struct {
	EnableTls                    bool          // Whether to enable TLS for secure connections
	MaxHeaderBytes               int           // Maximum size of request headers in bytes
	ReadHeaderTimeout            time.Duration // Timeout for reading request headers
	WriteTimeout                 time.Duration // Timeout for writing responses
	IdleTimeout                  time.Duration // Timeout for idle connections
	DisableGeneralOptionsHandler bool          // Whether to disable the default OPTIONS handler
	UseColorizedLogger           bool          // Whether to use a colorized console logger
}

// Server represents an HTTP server with support for TLS and graceful shutdown.
type Server struct {
	server          *http.Server   // Underlying HTTP server instance
	shutdownChannel chan os.Signal // Channel for receiving shutdown signals
	errorLogger     *log.Logger    // Logger for server errors
	ServerName      string         // Name of the server for logging purposes
	Opts            ServerOpts     // Server configuration options
	Address         ServerAddress  // Network address to listen on
	TlsConfigDir    string         // Directory containing TLS certificate and key files
	ExternalLogger  zerolog.Logger // External logger for server events
	ColorizedLogger zerolog.Logger // Colorized console logger (used if enabled)
	Mux             http.Handler   // HTTP request multiplexer
}

// NewServer creates a new Server instance with the specified configuration.
//
// It initializes the server with a name, address, logger, TLS directory, and options.
// If UseColorizedLogger is enabled in opts, a colorized console logger is configured.
// A shutdown channel is created to handle graceful shutdown signals (SIGINT, SIGTERM).
//
// Parameters:
//   - serverName: The name of the server, used in logs.
//   - address: The network address to listen on (e.g., ":8080").
//   - logger: The io.Writer for the external zerolog logger.
//   - tlsDir: The directory containing TLS certificate (.cert) and key (.key) files.
//   - opts: The ServerOpts configuration for the server.
//
// Returns:
//   - A pointer to a new Server instance.
func NewServer(
	serverName string, address int, logger io.Writer, tlsDir string, opts ServerOpts) *Server {

	server := &Server{
		ServerName:      serverName,
		Address:         ServerAddress(fmt.Sprintf(":%d", address)),
		Opts:            opts,
		TlsConfigDir:    tlsDir,
		ExternalLogger:  zerolog.New(logger),
		shutdownChannel: make(chan os.Signal, 1),
		errorLogger: log.New(
			os.Stderr,
			fmt.Sprintf("[%s] ", serverName),
			log.Ldate|log.Llongfile|log.Ltime,
		),
	}

	if opts.UseColorizedLogger {
		colorizedLogger := zerolog.NewConsoleWriter()
		{
			colorizedLogger.NoColor = false
			colorizedLogger.FormatLevel = func(i interface{}) string {
				return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
			}
		}
		server.ColorizedLogger = zerolog.New(colorizedLogger).With().Timestamp().Logger()
	}

	return server
}

// Serve starts the HTTP server and listens for incoming requests.
//
// It configures the server with the provided options (e.g., MaxHeaderBytes, TLS settings)
// and starts listening on the specified address. If EnableTls is true, it loads TLS certificates
// from TlsConfigDir and serves over HTTPS. The server runs in a background goroutine and
// listens for shutdown signals (SIGINT, SIGTERM). On receiving a signal, it performs a graceful
// shutdown with a 10-second timeout. Errors during startup or shutdown are logged using the
// configured logger (colorized if enabled).
func (s *Server) Serve() {
	logger := s.ColorizedLogger
	if !s.Opts.UseColorizedLogger {
		logger = s.ExternalLogger
	}
	s.server = &http.Server{
		MaxHeaderBytes:               s.Opts.MaxHeaderBytes,
		Addr:                         string(s.Address),
		Handler:                      s.Mux,
		DisableGeneralOptionsHandler: s.Opts.DisableGeneralOptionsHandler,
		ErrorLog:                     s.errorLogger,
	}

	if s.Opts.EnableTls {
		certificates, err := collectTlsCertificates(s.TlsConfigDir)
		if err != nil {
			logger.Error().Msgf("error occurred loading tls certificate: %v", err)
		}

		s.server.TLSConfig = &tls.Config{
			Certificates:       certificates,
			InsecureSkipVerify: false,
			ClientAuth:         tls.VerifyClientCertIfGiven,
		}
	}

	signal.Notify(s.shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	go func(s *Server, logger zerolog.Logger) {
		logger.Info().Msgf("Starting server on %v", s.server.Addr)
		if !s.Opts.EnableTls {
			err := s.server.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error().Msg("Could not start server engine")
			}
		} else {
			err := s.server.ListenAndServeTLS("", "")
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error().Msg("Could not start server engine")
			}
		}
	}(s, logger)

	sig := <-s.shutdownChannel
	logger.Info().Msgf("=> Caught %v", sig.String())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.server.Shutdown(ctx); err != nil {
		logger.Error().Msgf("Could not shutdown server properly: %v", err)
	}

	<-ctx.Done()
	logger.Info().Msg("Server terminated successfully")
}

// Shutdown initiates a graceful shutdown of the server by sending a SIGTERM signal.
//
// It triggers the server's shutdown process, which is handled by the Serve method.
func (s *Server) Shutdown() {
	s.shutdownChannel <- syscall.SIGTERM
}

// collectTlsCertificates loads TLS certificate and key pairs from the specified directory.
//
// It walks the directory to find files with ".cert" and ".key" extensions, pairing them by name
// (e.g., "server.cert" with "server.key"). Each pair is loaded into a tls.Certificate.
//
// Parameters:
//   - directory: The directory containing TLS certificate (.cert) and key (.key) files.
//
// Returns:
//   - A slice of tls.Certificate objects for the loaded certificate-key pairs.
//   - An error if the directory cannot be read, a key is missing, or a certificate pair fails to load.
func collectTlsCertificates(directory string) ([]tls.Certificate, error) {
	certFiles := make(map[string]string)
	keyFiles := make(map[string]string)

	err := fs.WalkDir(
		os.DirFS(directory), ".",
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if !d.Type().IsRegular() {
				return nil
			}

			fullPath := filepath.Join(directory, path)
			switch {
			case strings.HasSuffix(d.Name(), ".cert"):
				certFiles[d.Name()] = fullPath
			case strings.HasSuffix(d.Name(), ".key"):
				keyFiles[d.Name()] = fullPath
			}
			return nil
		})

	if err != nil {
		return nil, fmt.Errorf("error walking directory: %w", err)
	}

	certificates := make([]tls.Certificate, 0, len(certFiles))
	for certName, certPath := range certFiles {
		keyName := strings.TrimSuffix(certName, ".cert") + ".key"
		keyPath, ok := keyFiles[keyName]
		if !ok {
			return nil, fmt.Errorf("missing key for certificate: %s", certName)
		}

		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate pair %s: %w", certName, err)
		}

		certificates = append(certificates, cert)
	}

	return certificates, nil
}
