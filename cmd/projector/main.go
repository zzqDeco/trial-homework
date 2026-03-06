package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"bidsrv/internal/event"
	"bidsrv/internal/platform"
	pgstore "bidsrv/internal/postgres"
	"bidsrv/internal/readmodel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	postgresDSN := platform.GetEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/bidsrv?sslmode=disable")
	redisAddr := platform.GetEnv("REDIS_ADDR", "localhost:6379")
	batchSize := 500
	pollInterval := 200 * time.Millisecond

	store, err := pgstore.New(ctx, postgresDSN)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer store.Close()

	model, err := readmodel.New(ctx, redisAddr)
	if err != nil {
		log.Fatalf("redis init failed: %v", err)
	}
	defer model.Close()

	for {
		if ctx.Err() != nil {
			return
		}
		// The projector only advances the Redis read model from the outbox; it does not own facts or schema.
		count, err := store.ProcessOutboxBatch(ctx, batchSize, func(batchCtx context.Context, records []pgstore.OutboxRecord) error {
			for _, record := range records {
				if err := applyRecord(batchCtx, model, record); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			log.Printf("projector batch failed: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if count == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
		}
	}
}

func applyRecord(ctx context.Context, model *readmodel.Model, record pgstore.OutboxRecord) error {
	// Projection is keyed by outbox type so Redis stays a derived read model, not a second fact store.
	switch record.EventType {
	case pgstore.OutboxBidSeen:
		var evt event.BidEvent
		if err := json.Unmarshal(record.Payload, &evt); err != nil {
			return err
		}
		return model.ProjectBid(ctx, evt)
	case pgstore.OutboxImpressionSeen:
		var evt event.ImpressionEvent
		if err := json.Unmarshal(record.Payload, &evt); err != nil {
			return err
		}
		return model.ProjectImpression(ctx, evt)
	default:
		return fmt.Errorf("unsupported outbox event type: %s", record.EventType)
	}
}
