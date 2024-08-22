package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"os/exec"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/flashbots/gramine-andromeda-revm/sirrah-rpc/metrics"
)

type RevmService struct {
	log        *slog.Logger
	metricsSrv *metrics.MetricsServer

	sgxRevmCmd *exec.Cmd
	cmdLock    chan struct{}
}

func NewRevmService(log *slog.Logger, metricsSrv *metrics.MetricsServer) (*RevmService, error) {
	cmd := exec.Command("gramine-sgx", "./sgx-revm")
	cmd.Stdout = &bytes.Buffer{}
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return &RevmService{log: log, metricsSrv: metricsSrv, sgxRevmCmd: cmd, cmdLock: make(chan struct{}, 1)}, nil
}

func (r *RevmService) ExecuteLocked(ctx context.Context, cb func()) error {
	// TODO: monitor timeouts
	m_wait := r.metricsSrv.Float64Histogram("revm_lock_wait", "Revm Lock wait duration", metrics.UomMicroseconds, metrics.RevmLockWaitDuration...)
	m_timeout := r.metricsSrv.Float64Histogram("revm_lock_timeout", "Revm Lock timeout", metrics.UomMicroseconds, metrics.RevmLockWaitDuration...)
	start := time.Now()

	select {
	case r.cmdLock <- struct{}{}: // lock
		m_wait.Record(ctx, float64(time.Since(start).Microseconds()))
		cb()
		<-r.cmdLock // unlock
		return nil
	case <-ctx.Done():
		m_timeout.Record(context.Background(), float64(time.Since(start).Microseconds()))
		return ctx.Err()
	}
}

func (r *RevmService) ExecuteTx(ctx context.Context, tx *types.Transaction) ([]byte, []byte, error) {
	inPipe, err := r.sgxRevmCmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}

	outPipe, err := r.sgxRevmCmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}

	errPipe, err := r.sgxRevmCmd.StderrPipe()
	if err != nil {
		return nil, nil, err
	}

	txBytes, err := tx.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}

	txBytes = append(txBytes, []byte{'\n'}...)
	_, err = inPipe.Write(txBytes)
	if err != nil {
		return nil, nil, err
	}

	outData := make([]byte, 0)
	for {
		var buf [1024]byte
		nRead, err := outPipe.Read(buf[:])
		if err != nil {
			// TODO: how to recover? Could be impossible at this point. Simply panic and trigger a reboot.
			r.log.Error("revm service stdout corrupted, cannot recover", "err", err)
			panic(err)
		}
		if nRead == 0 {
			time.Sleep(time.Microsecond)
		}
		outData = append(outData, buf[:nRead]...)
		if buf[nRead-1] == '\n' {
			break
		}
	}

	outErr := make([]byte, 0)
	for {
		var buf [1024]byte
		nRead, err := errPipe.Read(buf[:])
		if err != nil {
			r.log.Error("revm service stdout corrupted, cannot recover", "err", err)
			panic(err)
		}
		if nRead == 0 {
			time.Sleep(time.Microsecond)
		}
		outErr = append(outErr, buf[:nRead]...)
		if buf[nRead-1] == '\n' {
			break
		}
	}

	return outData, outErr, nil
}

type JsonRpcRequest struct {
	Jsonrpc string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	Id      interface{}     `json:"id"`
}

type JsonRpcError struct {
	Code    int    `json:"code"`
	Message string `json:message""`
}

type JsonRpcErrorResponse struct {
	Jsonrpc string       `json:"jsonrpc"`
	Error   JsonRpcError `json:"error"`
	Id      interface{}  `json:"id"`
}

func NewJsonRpcErrorResponse(r *JsonRpcRequest, code int, msg string) JsonRpcErrorResponse {
	return JsonRpcErrorResponse{
		Id:      r.Id,
		Error:   JsonRpcError{Code: code, Message: msg},
		Jsonrpc: r.Jsonrpc,
	}
}

type JsonRpcSuccessResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	Result  interface{} `json:"result"`
	Id      interface{} `json:"id"`
}

func NewJsonRpcSuccessResponse(r *JsonRpcRequest, data interface{}) JsonRpcSuccessResponse {
	return JsonRpcSuccessResponse{
		Id:      r.Id,
		Result:  data,
		Jsonrpc: r.Jsonrpc,
	}
}

func parseJsonRpcRequest(r *http.Request) (*JsonRpcRequest, error) {
	var req JsonRpcRequest
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&req)
	if err != nil {
		return nil, err
	}
	return &req, nil
}

func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	// TODO: make logs traceable to a specific request!

	m := s.metricsSrv.Float64Histogram(
		"request_duration_api",
		"API request handling duration",
		metrics.UomMicroseconds,
		metrics.BucketsRequestDuration...,
	)
	defer func(start time.Time) {
		m.Record(r.Context(), float64(time.Since(start).Microseconds()))
	}(time.Now())

	// TODO: implement this within a proper jsonrpc server

	jr, err := parseJsonRpcRequest(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("could not parse request: %s", err.Error())))
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if jr.Method != "suave_offchainCall" {
		json.NewEncoder(w).Encode(NewJsonRpcErrorResponse(jr, -32600, "invalid method, expected suave_offchainCall"))
		return
	}

	var tx types.Transaction
	err = json.Unmarshal(jr.Params, &tx)
	if err != nil {
		json.NewEncoder(w).Encode(NewJsonRpcErrorResponse(jr, -32600, fmt.Sprintf("could not unmarshal transaction: %s", err.Error())))
		return
	}

	// Probably some more checks go here (signature is optional!)

	ctx := r.Context()
	outCh := make(chan []byte, 2)
	errCh := make(chan error, 1)
	s.revmService.ExecuteLocked(ctx, func() {
		stdout, stderr, err := s.revmService.ExecuteTx(ctx, &tx)
		if err != nil {
			errCh <- err
			return
		}

		outCh <- stdout
		outCh <- stderr
		errCh <- nil
	})

	if err = <-errCh; err != nil {
		s.log.Error("could not execute offchain call", "err", err)
		json.NewEncoder(w).Encode(NewJsonRpcErrorResponse(jr, -32600, fmt.Sprintf("could not execute: %s", err.Error())))
		return
	}

	json.NewEncoder(w).Encode(NewJsonRpcSuccessResponse(jr, <-outCh))

	if errData := <-errCh; errData != nil {
		s.log.Info("encountered execution error", "errData", errData)
	}
}

func (s *Server) handleLivenessCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleReadinessCheck(w http.ResponseWriter, r *http.Request) {
	if !s.isReady.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	if wasReady := s.isReady.Swap(false); !wasReady {
		return
	}
	// l := logutils.ZapFromRequest(r)
	s.log.Info("Server marked as not ready")
	time.Sleep(s.cfg.DrainDuration) // Give LB enough time to detect us not ready
}

func (s *Server) handleUndrain(w http.ResponseWriter, r *http.Request) {
	if wasReady := s.isReady.Swap(true); wasReady {
		return
	}
	// l := logutils.ZapFromRequest(r)
	s.log.Info("Server marked as ready")
}
