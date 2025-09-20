package shared

import (
	"net/rpc"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// UnicastToRPCServer sends an RPC request to a single server with retries.
func UnicastToRPCServer(addr string, method string, args any, reply any) error {
	log.Info().Msgf("addr=%s method=%s", addr, method)
	const maxRetries = 3
	const retryDelay = 500 * time.Millisecond

	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		client, dialErr := rpc.Dial("tcp", addr)
		if dialErr != nil {
			log.Warn().Msgf("attempt %d: err=%s addr=%s method=%s args=%#v", attempt, dialErr.Error(), addr, method, args)
			if attempt == maxRetries {
				return dialErr
			}
			time.Sleep(retryDelay)
			continue
		}
		defer client.Close()

		err = client.Call(method, args, reply)
		if err == nil {
			return nil
		}
		log.Warn().Msgf("attempt %d: RPC call failed: err=%s addr=%s method=%s", attempt, err.Error(), addr, method)
		if attempt == maxRetries {
			return err
		}
		time.Sleep(retryDelay)
	}
	return err
}

// BroadcastToRPCServers sends an RPC request to multiple servers concurrently.
func BroadcastToRPCServers(addrs []string, method string, args any, reply []any) []error {
	var (
		wg    = sync.WaitGroup{}
		errs  = make([]error, len(addrs))
		mutex = sync.Mutex{}
	)

	for i, addr := range addrs {
		wg.Add(1)
		go func(idx int, addr string) {
			defer wg.Done()
			err := UnicastToRPCServer(addr, method, args, reply[idx])
			if err != nil {
				mutex.Lock()
				errs[idx] = err
				mutex.Unlock()
			}
		}(i, addr)
	}
	wg.Wait()

	var filteredErrs []error
	for _, err := range errs {
		if err != nil {
			filteredErrs = append(filteredErrs, err)
		}
	}
	return filteredErrs
}
