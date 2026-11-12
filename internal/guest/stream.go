package guest

import (
	"encoding/json"
	"net"
	"sync"

	"github.com/hami-sh/vm/internal/api"
)

// StreamEndpoint adapts a host-provisioned Unix/vsock bridge to the protocol.
// The host must bind the accepted connection to its VM; claimed identity in
// JSON is never an authentication source. The stream carries typed messages,
// not caller-supplied Envelope.From values.
type StreamEndpoint struct {
	conn  net.Conn
	addr  api.Addr
	mu    sync.Mutex
	once  sync.Once
	inbox chan api.Envelope
}

func NewStreamEndpoint(conn net.Conn, addr api.Addr) *StreamEndpoint {
	return &StreamEndpoint{conn: conn, addr: addr, inbox: make(chan api.Envelope)}
}
func (e *StreamEndpoint) Addr() api.Addr             { return e.addr }
func (e *StreamEndpoint) Inbox() <-chan api.Envelope { return e.inbox }
func (e *StreamEndpoint) Send(to api.Addr, port api.Port, msg any) error {
	if to.Kind != api.KindAgent || to.ID != e.addr.Host || port != api.PortVsock {
		return api.ErrDenied
	}
	kind := ""
	switch msg.(type) {
	case api.GuestRegister:
		kind = "register"
	case api.GuestHeartbeat:
		kind = "heartbeat"
	default:
		return api.ErrDenied
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return json.NewEncoder(e.conn).Encode(struct {
		Type    string `json:"type"`
		Message any    `json:"message"`
	}{kind, msg})
}
func (e *StreamEndpoint) Close() { e.once.Do(func() { _ = e.conn.Close(); close(e.inbox) }) }

var _ api.Endpoint = (*StreamEndpoint)(nil)
