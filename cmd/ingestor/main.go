package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"bidsrv/internal/event"
	"bidsrv/internal/platform"
	pgstore "bidsrv/internal/postgres"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	kafkaBrokers := strings.Split(platform.GetEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	groupID := platform.GetEnv("KAFKA_GROUP", "bidsrv-ingestor")
	postgresDSN := platform.GetEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/bidsrv?sslmode=disable")

	store, err := pgstore.New(ctx, postgresDSN)
	if err != nil {
		log.Fatalf("postgres init failed: %v", err)
	}
	defer store.Close()

	// The ingestor owns schema creation so startup DDL runs in one place.
	if err := store.EnsureSchema(ctx); err != nil {
		log.Fatalf("postgres schema failed: %v", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics("bid-requests", "impressions"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("kafka client init failed: %v", err)
	}
	defer client.Close()

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				log.Printf("kafka fetch error topic=%s partition=%d: %v", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
			}
			time.Sleep(time.Second)
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			// Facts and outbox are written before committing Kafka offsets.
			commit, err := handleRecord(ctx, store, record)
			if err != nil {
				log.Fatalf("ingest failed topic=%s partition=%d offset=%d: %v", record.Topic, record.Partition, record.Offset, err)
			}
			if !commit {
				continue
			}
			if err := client.CommitRecords(ctx, record); err != nil {
				log.Fatalf("commit offset failed topic=%s partition=%d offset=%d: %v", record.Topic, record.Partition, record.Offset, err)
			}
		}
	}
}

func handleRecord(ctx context.Context, store *pgstore.Store, record *kgo.Record) (bool, error) {
	switch record.Topic {
	case "bid-requests":
		var evt event.BidEvent
		if err := json.Unmarshal(record.Value, &evt); err != nil {
			log.Printf("skip invalid bid message offset=%d: %v", record.Offset, err)
			return true, nil
		}
		if evt.BidID == "" || evt.CampaignID == "" {
			log.Printf("skip malformed bid message offset=%d missing required fields", record.Offset)
			return true, nil
		}
		// Bid ingestion only touches Postgres facts and the projection outbox.
		_, err := store.IngestBid(ctx, evt, record.Value)
		return true, err
	case "impressions":
		var evt event.ImpressionEvent
		if err := json.Unmarshal(record.Value, &evt); err != nil {
			log.Printf("skip invalid impression message offset=%d: %v", record.Offset, err)
			return true, nil
		}
		if evt.BidID == "" {
			log.Printf("skip malformed impression message offset=%d missing bid_id", record.Offset)
			return true, nil
		}
		// Impression ingestion follows the same facts-first path.
		_, err := store.IngestImpression(ctx, evt, record.Value)
		return true, err
	default:
		return true, errors.New("unexpected topic")
	}
}
