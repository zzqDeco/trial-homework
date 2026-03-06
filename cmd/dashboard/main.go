package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
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
	r.Get("/", srv.handleIndex)
	r.Get("/api/metrics/summary", srv.handleSummary)
	r.Get("/api/metrics/by-campaign", srv.handleByCampaign)

	httpServer := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("dashboard starting on port %s", port)
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

func (s *dashboardServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	const page = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Mini Ads Dashboard</title>
<style>
:root { --bg: #f4efe7; --ink: #132530; --muted: #5e6e74; --card: #fffaf3; --line: #d7ccc1; --accent: #c05a2b; }
body { margin: 0; font-family: Georgia, "Times New Roman", serif; background: radial-gradient(circle at top, #fff7ed, var(--bg)); color: var(--ink); }
main { max-width: 1100px; margin: 0 auto; padding: 32px 20px 48px; }
h1 { margin: 0 0 8px; font-size: 42px; }
p { margin: 0; color: var(--muted); }
.controls { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 24px 0; }
label { display: block; font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted); margin-bottom: 6px; }
input, button { width: 100%; box-sizing: border-box; border: 1px solid var(--line); background: var(--card); padding: 12px; font: inherit; color: inherit; }
button { background: var(--ink); color: #fffaf3; cursor: pointer; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 20px 0 24px; }
.card { background: var(--card); border: 1px solid var(--line); padding: 16px; }
.card strong { display: block; font-size: 30px; margin-top: 8px; }
.card span { font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted); }
.table-wrap { background: var(--card); border: 1px solid var(--line); overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 12px 14px; border-bottom: 1px solid var(--line); text-align: left; }
th { font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted); }
.status { margin-top: 8px; color: var(--muted); }
</style>
</head>
<body>
<main>
<h1>Metrics Dashboard</h1>
<p>Redis serves recent 30-day queries. Older ranges fall back to Postgres through the same API.</p>
<div class="controls">
<div>
<label for="from">From (UTC)</label>
<input id="from" type="datetime-local" />
</div>
<div>
<label for="to">To (UTC)</label>
<input id="to" type="datetime-local" />
</div>
<div style="display:flex;align-items:end;">
<button id="refresh">Refresh</button>
</div>
</div>
<div class="status" id="status">Loading...</div>
<section class="cards" id="cards"></section>
<div class="table-wrap">
<table>
<thead><tr><th>Campaign</th><th>Bid Requests</th><th>Deduped Impressions</th><th>Unknown Impressions</th><th>View Rate</th></tr></thead>
<tbody id="rows"></tbody>
</table>
</div>
</main>
<script>
const now = new Date();
const hourAgo = new Date(now.getTime() - 60 * 60 * 1000);
const fmt = (d) => new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
document.getElementById('from').value = fmt(hourAgo);
document.getElementById('to').value = fmt(now);
function toUtc(value) { return new Date(value).toISOString(); }
function pct(v) { return (v * 100).toFixed(2) + '%'; }
function num(v) { return new Intl.NumberFormat().format(v || 0); }
async function load() {
  const from = toUtc(document.getElementById('from').value);
  const to = toUtc(document.getElementById('to').value);
  const [summaryRes, campaignRes] = await Promise.all([
    fetch('/api/metrics/summary?from=' + encodeURIComponent(from) + '&to=' + encodeURIComponent(to)),
    fetch('/api/metrics/by-campaign?from=' + encodeURIComponent(from) + '&to=' + encodeURIComponent(to))
  ]);
  const summary = await summaryRes.json();
  const campaigns = await campaignRes.json();
  document.getElementById('status').textContent = 'Source: ' + summary.source + ' | Last projected: ' + (summary.last_projected_at || 'n/a');
  document.getElementById('cards').innerHTML = [
    ['View Rate', pct(summary.view_rate || 0)],
    ['Bid Requests', num(summary.bid_requests)],
    ['Deduped Impressions', num(summary.deduped_impressions)],
    ['Unknown Impressions', num(summary.unknown_impressions)]
  ].map(([label, value]) => '<article class="card"><span>' + label + '</span><strong>' + value + '</strong></article>').join('');
  document.getElementById('rows').innerHTML = campaigns.campaigns.map((item) =>
    '<tr><td>' + item.campaign_id + '</td><td>' + num(item.bid_requests) + '</td><td>' + num(item.deduped_impressions) + '</td><td>' + num(item.unknown_impressions) + '</td><td>' + pct(item.view_rate || 0) + '</td></tr>'
  ).join('');
}
document.getElementById('refresh').addEventListener('click', load);
load().catch((err) => { document.getElementById('status').textContent = err.message; });
</script>
</body>
</html>`
	tpl := template.Must(template.New("page").Parse(page))
	if err := tpl.Execute(w, nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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

func respondJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(value)
}
