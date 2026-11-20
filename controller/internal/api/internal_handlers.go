package api

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/cluster"
	"github.com/Sahas-out/DistributedObjectStorage/controller/internal/object"
	"github.com/Sahas-out/DistributedObjectStorage/pkg/protocol"
)

// maxRegisterBody bounds a register request body.
const maxRegisterBody = 4 << 10

// handleRegister handles a worker's registration or periodic announcement.
// The worker's address is taken from the connection's source IP and the port
// in the body.
func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req protocol.RegisterRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxRegisterBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, "body must be a JSON object {worker_id, port}")
		return
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	resp, err := s.members.Register(req, ip)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, resp)
	case errors.Is(err, cluster.ErrInvalidRegistration):
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, err.Error())
	case errors.Is(err, cluster.ErrWorkerRetired):
		writeError(w, http.StatusGone, protocol.CodeWorkerRetired, "worker "+req.WorkerID+" was deregistered and cannot register again")
	default:
		s.log.Error("register worker", "worker_id", req.WorkerID, "error", err)
		writeError(w, http.StatusInternalServerError, protocol.CodeInternalError, "internal server error")
	}
}

// handleDeregister removes a worker that is leaving voluntarily.
func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("worker_id")
	err := s.members.Deregister(id)
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, cluster.ErrUnknownWorker):
		writeError(w, http.StatusNotFound, protocol.CodeWorkerNotFound, "worker "+id+" is not registered")
	default:
		s.log.Error("deregister worker", "worker_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, protocol.CodeInternalError, "internal server error")
	}
}

// handleListWorkers responds with every registered worker and its status
// and chunk count.
func (s *Server) handleListWorkers(w http.ResponseWriter, _ *http.Request) {
	ws := s.members.Workers()
	out := make([]protocol.WorkerInfo, 0, len(ws))
	for _, wk := range ws {
		out = append(out, protocol.WorkerInfo{
			WorkerID:   wk.ID,
			Addr:       wk.Addr,
			Status:     string(wk.Status),
			ChunkCount: wk.ChunkCount,
			LastSeen:   wk.LastSeen.UTC(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// replicasResponse is the body of GET /internal/objects/{object_id}/replicas.
type replicasResponse struct {
	protocol.ObjectInfo
	Replicas []protocol.ReplicaInfo `json:"replicas"`
}

// handleReplicas shows where every copy of an object's chunks lives.
func (s *Server) handleReplicas(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	if !object.ValidObjectID(id) {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidObjectID, "object id must be 32 lowercase hex characters")
		return
	}
	obj, locs, err := s.objects.Replicas(r.Context(), id)
	if err != nil {
		s.writeObjectError(w, r, id, err)
		return
	}
	out := replicasResponse{ObjectInfo: objectInfo(obj), Replicas: make([]protocol.ReplicaInfo, 0, len(locs))}
	for _, l := range locs {
		out.Replicas = append(out.Replicas, protocol.ReplicaInfo{
			ChunkID:    l.ChunkID,
			SequenceNo: l.SeqNo,
			WorkerID:   l.WorkerID,
			WorkerAddr: l.Addr,
			Status:     string(l.Status),
		})
	}
	writeJSON(w, http.StatusOK, out)
}
