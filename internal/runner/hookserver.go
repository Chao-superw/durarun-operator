package runner

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
)

type HookServer struct {
	socketPath string
	listener   net.Listener
	handler    HookHandler
	done       chan struct{}
	wg         sync.WaitGroup
	stopOnce   sync.Once
}

type HookHandler interface {
	HandleStepBegin(stepID string) (*StepBeginResponse, error)
	HandleStepEnd(stepID string, exitCode int, output string) (*StepEndResponse, error)
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id"`
}

type jsonRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *rpcError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type stepBeginParams struct {
	StepID string `json:"step_id"`
}

type stepEndParams struct {
	StepID   string `json:"step_id"`
	Exit     int    `json:"exit"`
	Output   string `json:"output"`
}

func NewHookServer(socketPath string, handler HookHandler) *HookServer {
	return &HookServer{
		socketPath: socketPath,
		handler:    handler,
		done:       make(chan struct{}),
	}
}

func (s *HookServer) Start() error {
	os.Remove(s.socketPath)

	ln, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("hookserver: listen: %w", err)
	}
	s.listener = ln

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-s.done:
					return
				default:
					continue
				}
			}
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				defer c.Close()
				s.handleConn(c)
			}(conn)
		}
	}()

	return nil
}

func (s *HookServer) Stop() error {
	s.stopOnce.Do(func() {
		close(s.done)
		if s.listener != nil {
			s.listener.Close()
		}
		s.wg.Wait()
		os.Remove(s.socketPath)
	})
	return nil
}

func (s *HookServer) handleConn(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	var req jsonRPCRequest
	if err := decoder.Decode(&req); err != nil {
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			Error:   &rpcError{Code: -32700, Message: "parse error"},
			ID:      nil,
		}
		json.NewEncoder(conn).Encode(resp)
		return
	}

	var result interface{}
	var rpcErr *rpcError

	switch req.Method {
	case "step_begin":
		var params stepBeginParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &rpcError{Code: -32602, Message: "invalid params"}
			break
		}
		res, err := s.handler.HandleStepBegin(params.StepID)
		if err != nil {
			rpcErr = &rpcError{Code: -32000, Message: err.Error()}
		} else {
			result = res
		}

	case "step_end":
		var params stepEndParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &rpcError{Code: -32602, Message: "invalid params"}
			break
		}
		res, err := s.handler.HandleStepEnd(params.StepID, params.Exit, params.Output)
		if err != nil {
			rpcErr = &rpcError{Code: -32000, Message: err.Error()}
		} else {
			result = res
		}

	default:
		rpcErr = &rpcError{Code: -32601, Message: "method not found"}
	}

	resp := jsonRPCResponse{
		JSONRPC: "2.0",
		Result:  result,
		Error:   rpcErr,
		ID:      req.ID,
	}
	json.NewEncoder(conn).Encode(resp)
}
