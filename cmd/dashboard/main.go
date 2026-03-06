package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"bidsrv/internal/metrics"
	"bidsrv/internal/platform"
	pgstore "bidsrv/internal/postgres"
	"bidsrv/internal/readmodel"
)

type dashboardServer struct {
	store *pgstore.Store
	model *readmodel.Model
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	postgresDSN := platform.GetEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/bidsrv?sslmode=disable")
	redisAddr := platform.GetEnv("REDIS_ADDR", "localhost:6379")
	port := platform.GetEnv("PORT", "8082")

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

	srv := &dashboardServer{store: store, model: model}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/api/metrics/summary", srv.handleSummary)
	r.Get("/api/metrics/by-campaign", srv.handleByCampaign)
	r.Get("/api/metrics/timeseries", srv.handleTimeSeries)

	httpServer := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("dashboard api starting on port %s", port)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("dashboard failed: %v", err)
	}
}

func (s *dashboardServer) handleSummary(w http.ResponseWriter, r *http.Request) {
	from, to, err := parseRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	summary, err := s.querySummary(r.Context(), from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, summary)
}

func (s *dashboardServer) handleByCampaign(w http.ResponseWriter, r *http.Request) {
	from, to, err := parseRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	items, source, err := s.queryByCampaign(r.Context(), from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, map[string]interface{}{
		"from":      from.UTC(),
		"to":        to.UTC(),
		"source":    source,
		"campaigns": items,
	})
}

func (s *dashboardServer) handleTimeSeries(w http.ResponseWriter, r *http.Request) {
	from, to, err := parseRange(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resolution := resolveResolution(strings.TrimSpace(r.URL.Query().Get("resolution")), from, to)
	series, err := s.queryTimeSeries(r.Context(), from, to, resolution)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, series)
}

func (s *dashboardServer) querySummary(ctx context.Context, from, to time.Time) (metrics.Summary, error) {
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)
	switch {
	case !to.After(cutoff):
		summary, err := s.store.Summary(ctx, from, to)
		summary.Source = "postgres"
		return summary, err
	case !from.Before(cutoff):
		summary, err := s.model.Summary(ctx, from, to)
		summary.Source = "redis"
		return summary, err
	default:
		left, err := s.store.Summary(ctx, from, cutoff)
		if err != nil {
			return metrics.Summary{}, err
		}
		right, err := s.model.Summary(ctx, cutoff, to)
		if err != nil {
			return metrics.Summary{}, err
		}
		merged := metrics.MergeSummary(left, right)
		merged.Source = "mixed"
		merged.Finalize(time.Now().UTC())
		return merged, nil
	}
}

func (s *dashboardServer) queryByCampaign(ctx context.Context, from, to time.Time) ([]metrics.CampaignMetrics, string, error) {
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)
	switch {
	case !to.After(cutoff):
		items, err := s.store.ByCampaign(ctx, from, to)
		return items, "postgres", err
	case !from.Before(cutoff):
		items, err := s.model.ByCampaign(ctx, from, to)
		return items, "redis", err
	default:
		left, err := s.store.ByCampaign(ctx, from, cutoff)
		if err != nil {
			return nil, "", err
		}
		right, err := s.model.ByCampaign(ctx, cutoff, to)
		if err != nil {
			return nil, "", err
		}
		return metrics.MergeCampaigns(left, right), "mixed", nil
	}
}

func (s *dashboardServer) queryTimeSeries(ctx context.Context, from, to time.Time, resolution string) (metrics.TimeSeries, error) {
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)
	switch {
	case !to.After(cutoff):
		series, err := s.store.TimeSeries(ctx, from, to, resolution)
		series.Source = "postgres"
		return series, err
	case !from.Before(cutoff):
		series, err := s.model.TimeSeries(ctx, from, to, resolution)
		series.Source = "redis"
		return series, err
	default:
		left, err := s.store.TimeSeries(ctx, from, cutoff, resolution)
		if err != nil {
			return metrics.TimeSeries{}, err
		}
		right, err := s.model.TimeSeries(ctx, cutoff, to, resolution)
		if err != nil {
			return metrics.TimeSeries{}, err
		}
		merged := metrics.MergeTimeSeries(left, right)
		merged.Source = "mixed"
		return merged, nil
	}
}

func parseRange(r *http.Request) (time.Time, time.Time, error) {
	fromRaw := strings.TrimSpace(r.URL.Query().Get("from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("to"))
	if fromRaw == "" || toRaw == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("from and to are required")
	}
	from, err := time.Parse(time.RFC3339, fromRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid from: %w", err)
	}
	to, err := time.Parse(time.RFC3339, toRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid to: %w", err)
	}
	if !from.Before(to) {
		return time.Time{}, time.Time{}, fmt.Errorf("from must be before to")
	}
	return from.UTC(), to.UTC(), nil
}

func resolveResolution(raw string, from, to time.Time) string {
	switch raw {
	case "minute", "hour", "day":
		return raw
	case "", "auto":
		delta := to.Sub(from)
		switch {
		case delta <= 6*time.Hour:
			return "minute"
		case delta <= 7*24*time.Hour:
			return "hour"
		default:
			return "day"
		}
	default:
		return "minute"
	}
}

func respondJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(value)
}
