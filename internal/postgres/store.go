package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"bidsrv/internal/event"
	"bidsrv/internal/metrics"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	OutboxBidSeen        = "bid_seen"
	OutboxImpressionSeen = "impression_seen"
)

type Store struct {
	pool *pgxpool.Pool
}

type OutboxRecord struct {
	ID        int64
	EventType string
	EntityID  string
	EventTS   time.Time
	Payload   []byte
}

type BidRow struct {
	Event event.BidEvent
}

type ImpressionRow struct {
	Event event.ImpressionEvent
}

func New(ctx context.Context, dsn string) (*Store, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	var pool *pgxpool.Pool
	for i := 0; i < 10; i++ {
		pool, err = pgxpool.NewWithConfig(ctx, config)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
		}
		if err == nil {
			return &Store{pool: pool}, nil
		}
		if pool != nil {
			pool.Close()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("connect postgres: %w", err)
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS bids (
  bid_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL,
  campaign_id TEXT NOT NULL,
  user_idfv TEXT,
  placement_id TEXT,
  event_ts TIMESTAMPTZ NOT NULL,
  raw_json JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS bids_event_ts_idx ON bids(event_ts);
CREATE INDEX IF NOT EXISTS bids_campaign_ts_idx ON bids(campaign_id, event_ts);

CREATE TABLE IF NOT EXISTS impressions (
  bid_id TEXT PRIMARY KEY,
  event_ts TIMESTAMPTZ NOT NULL,
  raw_json JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS impressions_event_ts_idx ON impressions(event_ts);

CREATE TABLE IF NOT EXISTS projection_outbox (
  id BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL,
  payload_json JSONB NOT NULL,
  processed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS projection_outbox_unprocessed_idx ON projection_outbox(processed_at, id);
`
	_, err := s.pool.Exec(ctx, ddl)
	return err
}

func (s *Store) IngestBid(ctx context.Context, evt event.BidEvent, raw []byte) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	inserted, err := insertBid(ctx, tx, evt, raw)
	if err != nil {
		return false, err
	}
	if inserted {
		if err := insertOutbox(ctx, tx, OutboxBidSeen, evt.BidID, toTime(evt.Timestamp), raw); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return inserted, nil
}

func (s *Store) IngestImpression(ctx context.Context, evt event.ImpressionEvent, raw []byte) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	inserted, err := insertImpression(ctx, tx, evt, raw)
	if err != nil {
		return false, err
	}
	if inserted {
		if err := insertOutbox(ctx, tx, OutboxImpressionSeen, evt.BidID, toTime(evt.Timestamp), raw); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return inserted, nil
}

func insertBid(ctx context.Context, tx pgx.Tx, evt event.BidEvent, raw []byte) (bool, error) {
	result, err := tx.Exec(ctx, `
INSERT INTO bids (bid_id, request_id, campaign_id, user_idfv, placement_id, event_ts, raw_json)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT DO NOTHING
`, evt.BidID, evt.RequestID, evt.CampaignID, evt.UserIDFV, evt.PlacementID, toTime(evt.Timestamp), raw)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

func insertImpression(ctx context.Context, tx pgx.Tx, evt event.ImpressionEvent, raw []byte) (bool, error) {
	result, err := tx.Exec(ctx, `
INSERT INTO impressions (bid_id, event_ts, raw_json)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING
`, evt.BidID, toTime(evt.Timestamp), raw)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() == 1, nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, eventType, entityID string, eventTS time.Time, raw []byte) error {
	_, err := tx.Exec(ctx, `
INSERT INTO projection_outbox (event_type, entity_id, event_ts, payload_json)
VALUES ($1, $2, $3, $4)
`, eventType, entityID, eventTS.UTC(), raw)
	return err
}

func (s *Store) ProcessOutboxBatch(ctx context.Context, limit int, apply func(context.Context, []OutboxRecord) error) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
SELECT id, event_type, entity_id, event_ts, payload_json
FROM projection_outbox
WHERE processed_at IS NULL
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT $1
`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	batch := make([]OutboxRecord, 0, limit)
	for rows.Next() {
		var row OutboxRecord
		if err := rows.Scan(&row.ID, &row.EventType, &row.EntityID, &row.EventTS, &row.Payload); err != nil {
			return 0, err
		}
		batch = append(batch, row)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, tx.Rollback(ctx)
	}

	if err := apply(ctx, batch); err != nil {
		return 0, err
	}

	ids := make([]int64, 0, len(batch))
	for _, item := range batch {
		ids = append(ids, item.ID)
	}
	if _, err := tx.Exec(ctx, `UPDATE projection_outbox SET processed_at = $1 WHERE id = ANY($2)`, time.Now().UTC(), ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(batch), nil
}

func (s *Store) Summary(ctx context.Context, from, to time.Time) (metrics.Summary, error) {
	summary := metrics.Summary{From: from.UTC(), To: to.UTC()}
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM bids WHERE event_ts >= $1 AND event_ts < $2`, from.UTC(), to.UTC()).Scan(&summary.BidRequests); err != nil {
		return summary, err
	}
	if err := s.pool.QueryRow(ctx, `
SELECT COUNT(*)
FROM impressions i
JOIN bids b ON b.bid_id = i.bid_id
WHERE i.event_ts >= $1 AND i.event_ts < $2
`, from.UTC(), to.UTC()).Scan(&summary.DedupedImpressions); err != nil {
		return summary, err
	}
	if err := s.pool.QueryRow(ctx, `
SELECT COUNT(*)
FROM impressions i
LEFT JOIN bids b ON b.bid_id = i.bid_id
WHERE i.event_ts >= $1 AND i.event_ts < $2 AND b.bid_id IS NULL
`, from.UTC(), to.UTC()).Scan(&summary.UnknownImpressions); err != nil {
		return summary, err
	}
	summary.Finalize(time.Now().UTC())
	return summary, nil
}

func (s *Store) ByCampaign(ctx context.Context, from, to time.Time) ([]metrics.CampaignMetrics, error) {
	merged := make(map[string]metrics.CampaignMetrics)

	rows, err := s.pool.Query(ctx, `
SELECT campaign_id, COUNT(*)
FROM bids
WHERE event_ts >= $1 AND event_ts < $2
GROUP BY campaign_id
`, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var campaignID string
		var count int64
		if err := rows.Scan(&campaignID, &count); err != nil {
			rows.Close()
			return nil, err
		}
		merged[campaignID] = metrics.CampaignMetrics{CampaignID: campaignID, BidRequests: count}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	rows, err = s.pool.Query(ctx, `
SELECT b.campaign_id, COUNT(*)
FROM impressions i
JOIN bids b ON b.bid_id = i.bid_id
WHERE i.event_ts >= $1 AND i.event_ts < $2
GROUP BY b.campaign_id
`, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var campaignID string
		var count int64
		if err := rows.Scan(&campaignID, &count); err != nil {
			rows.Close()
			return nil, err
		}
		item := merged[campaignID]
		item.CampaignID = campaignID
		item.DedupedImpressions = count
		merged[campaignID] = item
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	var unknown int64
	if err := s.pool.QueryRow(ctx, `
SELECT COUNT(*)
FROM impressions i
LEFT JOIN bids b ON b.bid_id = i.bid_id
WHERE i.event_ts >= $1 AND i.event_ts < $2 AND b.bid_id IS NULL
`, from.UTC(), to.UTC()).Scan(&unknown); err != nil {
		return nil, err
	}
	if unknown > 0 {
		item := merged["UNKNOWN"]
		item.CampaignID = "UNKNOWN"
		item.UnknownImpressions = unknown
		merged["UNKNOWN"] = item
	}

	items := make([]metrics.CampaignMetrics, 0, len(merged))
	for _, item := range merged {
		items = append(items, item)
	}
	return metrics.NormalizeCampaigns(items), nil
}

func (s *Store) TimeSeries(ctx context.Context, from, to time.Time, resolution string) (metrics.TimeSeries, error) {
	resolution = normalizeResolution(resolution)
	series := metrics.TimeSeries{
		From:       from.UTC(),
		To:         to.UTC(),
		Resolution: resolution,
	}

	points := make(map[time.Time]metrics.TimeSeriesPoint)
	for _, bucket := range seriesBuckets(from, to, resolution) {
		points[bucket] = metrics.TimeSeriesPoint{TS: bucket}
	}

	bidCounts, err := s.bucketedCounts(ctx, "bids", "", from, to, resolution)
	if err != nil {
		return series, err
	}
	for ts, count := range bidCounts {
		point := points[ts]
		point.TS = ts
		point.BidRequests = count
		points[ts] = point
	}

	dedupCounts, err := s.bucketedCounts(ctx, "impressions i", "JOIN bids b ON b.bid_id = i.bid_id", from, to, resolution)
	if err != nil {
		return series, err
	}
	for ts, count := range dedupCounts {
		point := points[ts]
		point.TS = ts
		point.DedupedImpressions = count
		points[ts] = point
	}

	unknownCounts, err := s.bucketedCounts(ctx, "impressions i", "LEFT JOIN bids b ON b.bid_id = i.bid_id", from, to, resolution, "b.bid_id IS NULL")
	if err != nil {
		return series, err
	}
	for ts, count := range unknownCounts {
		point := points[ts]
		point.TS = ts
		point.UnknownImpressions = count
		points[ts] = point
	}

	series.Points = make([]metrics.TimeSeriesPoint, 0, len(points))
	for _, bucket := range seriesBuckets(from, to, resolution) {
		series.Points = append(series.Points, points[bucket])
	}
	series.Points = metrics.NormalizeTimeSeries(series.Points)
	return series, nil
}

func (s *Store) RecentBids(ctx context.Context, since time.Time) ([]BidRow, error) {
	rows, err := s.pool.Query(ctx, `
SELECT raw_json
FROM bids
WHERE event_ts >= $1
ORDER BY event_ts ASC
`, since.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]BidRow, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var evt event.BidEvent
		if err := json.Unmarshal(raw, &evt); err != nil {
			return nil, err
		}
		result = append(result, BidRow{Event: evt})
	}
	return result, rows.Err()
}

func (s *Store) RecentImpressions(ctx context.Context, since time.Time) ([]ImpressionRow, error) {
	rows, err := s.pool.Query(ctx, `
SELECT raw_json
FROM impressions
WHERE event_ts >= $1
ORDER BY event_ts ASC
`, since.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]ImpressionRow, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var evt event.ImpressionEvent
		if err := json.Unmarshal(raw, &evt); err != nil {
			return nil, err
		}
		result = append(result, ImpressionRow{Event: evt})
	}
	return result, rows.Err()
}

func toTime(ts int64) time.Time {
	if ts == 0 {
		return time.Now().UTC()
	}
	return time.Unix(ts, 0).UTC()
}

func (s *Store) bucketedCounts(ctx context.Context, fromExpr, joinExpr string, from, to time.Time, resolution string, extraWhere ...string) (map[time.Time]int64, error) {
	timeColumn := "event_ts"
	where := "event_ts >= $1 AND event_ts < $2"
	if fromExpr != "bids" {
		timeColumn = "i.event_ts"
		where = "i.event_ts >= $1 AND i.event_ts < $2"
	}
	bucketExpr := bucketEpochExpr(resolution, timeColumn)
	for _, clause := range extraWhere {
		where += " AND " + clause
	}

	query := fmt.Sprintf(`
SELECT %s AS bucket_epoch, COUNT(*)
FROM %s
%s
WHERE %s
GROUP BY bucket_epoch
ORDER BY bucket_epoch
`, bucketExpr, fromExpr, joinExpr, where)

	rows, err := s.pool.Query(ctx, query, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[time.Time]int64)
	for rows.Next() {
		var epoch int64
		var count int64
		if err := rows.Scan(&epoch, &count); err != nil {
			return nil, err
		}
		out[time.Unix(epoch, 0).UTC()] = count
	}
	return out, rows.Err()
}

func bucketEpochExpr(resolution, timeColumn string) string {
	resolution = normalizeResolution(resolution)
	return fmt.Sprintf("EXTRACT(EPOCH FROM date_trunc('%s', %s AT TIME ZONE 'UTC'))::bigint", resolution, timeColumn)
}

func normalizeResolution(resolution string) string {
	switch resolution {
	case "hour", "day":
		return resolution
	default:
		return "minute"
	}
}

func seriesBuckets(from, to time.Time, resolution string) []time.Time {
	start := truncateResolution(from.UTC(), resolution)
	end := truncateResolution(to.UTC(), resolution)
	if !end.Equal(to.UTC()) {
		end = advanceResolution(end, resolution)
	}
	if !start.Before(end) {
		return []time.Time{start}
	}
	buckets := make([]time.Time, 0)
	for current := start; current.Before(end); current = advanceResolution(current, resolution) {
		buckets = append(buckets, current)
	}
	return buckets
}

func truncateResolution(ts time.Time, resolution string) time.Time {
	ts = ts.UTC()
	switch normalizeResolution(resolution) {
	case "day":
		return time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
	case "hour":
		return ts.Truncate(time.Hour)
	default:
		return ts.Truncate(time.Minute)
	}
}

func advanceResolution(ts time.Time, resolution string) time.Time {
	switch normalizeResolution(resolution) {
	case "day":
		return ts.AddDate(0, 0, 1)
	case "hour":
		return ts.Add(time.Hour)
	default:
		return ts.Add(time.Minute)
	}
}
