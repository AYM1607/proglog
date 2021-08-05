package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHTTPServer creates the configuration for a log http server.
func NewHTTPServer(addr string) *http.Server {
	// httpsrv is the struct that contains the log.
	httpsrv := newHTTPServer()
	// r is the http router.
	r := mux.NewRouter()

	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

// httpServer defines the handlers and the data necessary for the log server.
type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offsset uint64 `json:"offset"`
}

type ConsumerResponse struct {
	Record Record `json:"record"`
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		// the request does not conform to the specified type in ProduceRequest.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	off, err := s.Log.Append(req.Record)
	if err != nil {
		// The record could not be added to the log.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProduceResponse{Offset: off}
	// The header gets overwritten if there's an encoding error.
	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		// The information could not be encoded to the response struct.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		// The request does not conform to the specified type in ConsumeRequest.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(req.Offsset)
	// Handle possible errors when retrieving the log.
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumerResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
