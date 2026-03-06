package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"bidsrv/internal/platform"
	pgstore "bidsrv/internal/postgres"
	"bidsrv/internal/readmodel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	postgresDSN := platform.GetEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/bidsrv?sslmode=disable")
	redisAddr := platform.GetEnv("REDIS_ADDR", "localhost:6379")
	since := time.Now().UTC().Add(-30 * 24 * time.Hour)

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

	if err := model.Reset(ctx); err != nil {
		log.Fatalf("reset redis read model failed: %v", err)
	}

	bids, err := store.RecentBids(ctx, since)
	if err != nil {
		log.Fatalf("load recent bids failed: %v", err)
	}
	for _, row := range bids {
		if err := model.ProjectBid(ctx, row.Event); err != nil {
			log.Fatalf("project bid failed: %v", err)
		}
	}

	impressions, err := store.RecentImpressions(ctx, since)
	if err != nil {
		log.Fatalf("load recent impressions failed: %v", err)
	}
	for _, row := range impressions {
		if err := model.ProjectImpression(ctx, row.Event); err != nil {
			log.Fatalf("project impression failed: %v", err)
		}
	}

	log.Printf("redis backfill complete: bids=%d impressions=%d", len(bids), len(impressions))
}
