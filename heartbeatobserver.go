package natsmembership

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultRollingWindow = 10 * time.Second
)

type HeartbeatObserver struct {
	mu              *sync.Mutex
	nc              *nats.Conn
	rollingWindow   time.Duration
	observedMembers map[string]time.Time
}

func (h *HeartbeatObserver) Listen(ctx context.Context, natsSubject string) error {
	handleMessage := func(msg *nats.Msg) {
		h.mu.Lock()
		defer h.mu.Unlock()

		h.observedMembers[string(msg.Data)] = time.Now()
	}

	sub, err := h.nc.Subscribe(natsSubject, handleMessage)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			_ = sub.Unsubscribe() // TODO: handle error
			return ctx.Err()

		// Occasionally clean up the observedMembers map to avoid a leak
		case <-time.After(h.rollingWindow):
			func() {
				h.mu.Lock()
				defer h.mu.Unlock()

				now := time.Now()
				for member, lastSeen := range h.observedMembers {
					if now.Sub(lastSeen) > h.rollingWindow {
						delete(h.observedMembers, member)
					}
				}
			}()
		}
	}
}

func (h *HeartbeatObserver) Members() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()

	members := []string{}
	for member, lastSeen := range h.observedMembers {
		if now.Sub(lastSeen) < h.rollingWindow {
			members = append(members, member)
		}
	}

	return members
}

func NewHeartbeatObserver(nc *nats.Conn, opts ...HeartbeatObserverOpt) *HeartbeatObserver {
	rtn := &HeartbeatObserver{
		mu:              &sync.Mutex{},
		nc:              nc,
		rollingWindow:   defaultRollingWindow,
		observedMembers: map[string]time.Time{},
	}

	for _, opt := range opts {
		opt(rtn)
	}

	return rtn
}

type HeartbeatObserverOpt func(*HeartbeatObserver)

func WithRollingWindow(d time.Duration) HeartbeatObserverOpt {
	return func(h *HeartbeatObserver) {
		h.rollingWindow = d
	}
}
