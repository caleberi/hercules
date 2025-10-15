package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

type User struct {
	Id   int
	Name string
}

var testData = []User{
	{Id: 23, Name: "test2"},
	{Id: 12, Name: "test34"},
}

func setupTestServer() *Server {
	router := gin.Default()

	router.GET("/user", func(c *gin.Context) {
		c.JSON(http.StatusOK, testData)
	})
	server := NewServer(
		"Sami",
		8082,
		os.Stdout,
		"",
		ServerOpts{
			EnableTls:         false,
			MaxHeaderBytes:    4 * (1 << 20),
			IdleTimeout:       30 * time.Second,
			ReadHeaderTimeout: 1 * time.Second,
			WriteTimeout:      1 * time.Second,
		})
	server.Mux = router
	return server
}

func TestServerWithMux(t *testing.T) {

	server := setupTestServer()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go server.Serve()
	go func() {
		<-ctx.Done()
		server.Shutdown()
	}()

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://localhost:8082/user")
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var receivedData = []User{}
	err = json.NewDecoder(resp.Body).Decode(&receivedData)
	assert.NoError(t, err)
	assert.Equal(t, testData, receivedData)
}
