package natsmembership

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultPublishInterval = time.Second
	defaultMaxRetries      = 3
)

type HeartbeatPublisher struct {
	nc              *nats.Conn
	publishInterval time.Duration
	maxRetries      int
}

func (h *HeartbeatPublisher) Publish(ctx context.Context, natsSubject, memberName string) error {
	failedAttempts := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(h.publishInterval):
			if err := h.nc.Publish(natsSubject, []byte(memberName)); err != nil {
				failedAttempts++

				if failedAttempts > h.maxRetries {
					return err
				}

				// TODO: log error
				continue
			}

			failedAttempts = 0
		}
	}
}

func NewHeartbeatPublisher(nc *nats.Conn, opts ...HeartbeatPublisherOpt) *HeartbeatPublisher {
	rtn := &HeartbeatPublisher{
		nc:              nc,
		publishInterval: defaultPublishInterval,
		maxRetries:      defaultMaxRetries,
	}

	for _, opt := range opts {
		opt(rtn)
	}

	return rtn
}

type HeartbeatPublisherOpt func(*HeartbeatPublisher)

func WithPublishInterval(interval time.Duration) HeartbeatPublisherOpt {
	return func(h *HeartbeatPublisher) {
		h.publishInterval = interval
	}
}

func WithMaxRetries(maxRetries int) HeartbeatPublisherOpt {
	return func(h *HeartbeatPublisher) {
		h.maxRetries = maxRetries
	}
}
