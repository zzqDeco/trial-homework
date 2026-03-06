package readmodel

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"bidsrv/internal/event"
	"bidsrv/internal/metrics"

	"github.com/go-redis/redis/v8"
)

const (
	globalKey         = "rm:global"
	campaignSetKey    = "rm:campaigns"
	unknownCampaignID = "UNKNOWN"
	readModelTTL      = 31 * 24 * time.Hour
	minuteLayout      = "200601021504"
)

type Model struct {
	client *redis.Client
}

func New(ctx context.Context, addr string) (*Model, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	var err error
	for i := 0; i < 10; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = client.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			return &Model{client: client}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, err
}

func (m *Model) Close() error {
	return m.client.Close()
}

func (m *Model) ProjectBid(ctx context.Context, evt event.BidEvent) error {
	bucket := bucketFor(toTime(evt.Timestamp))
	unknownKey := unknownKeyFor(evt.BidID)
	unknown, err := m.client.HGetAll(ctx, unknownKey).Result()
	if err != nil {
		return err
	}

	_, err = m.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		bidKey := bidMetaKey(evt.BidID)
		pipe.HSet(ctx, bidKey, map[string]interface{}{
			"campaign_id":  evt.CampaignID,
			"placement_id": evt.PlacementID,
			"user_idfv":    evt.UserIDFV,
			"event_ts":     strconv.FormatInt(evt.Timestamp, 10),
		})
		pipe.Expire(ctx, bidKey, readModelTTL)

		incrementCounts(ctx, pipe, globalKey, 0, 0, 1)
		incrementCounts(ctx, pipe, summaryMinuteKey(bucket), 0, 0, 1)
		pipe.Expire(ctx, summaryMinuteKey(bucket), readModelTTL)

		pipe.SAdd(ctx, campaignSetKey, evt.CampaignID)
		incrementCounts(ctx, pipe, campaignKey(evt.CampaignID), 0, 0, 1)
		incrementCounts(ctx, pipe, campaignMinuteKey(evt.CampaignID, bucket), 0, 0, 1)
		pipe.Expire(ctx, campaignMinuteKey(evt.CampaignID, bucket), readModelTTL)

		if len(unknown) > 0 {
			impressionBucket := unknown["impression_bucket"]
			incrementCounts(ctx, pipe, globalKey, -1, 1, 0)
			incrementCounts(ctx, pipe, summaryMinuteKey(impressionBucket), -1, 1, 0)
			pipe.Expire(ctx, summaryMinuteKey(impressionBucket), readModelTTL)

			pipe.SAdd(ctx, campaignSetKey, unknownCampaignID)
			incrementCounts(ctx, pipe, campaignKey(unknownCampaignID), -1, 0, 0)
			incrementCounts(ctx, pipe, campaignMinuteKey(unknownCampaignID, impressionBucket), -1, 0, 0)
			pipe.Expire(ctx, campaignMinuteKey(unknownCampaignID, impressionBucket), readModelTTL)

			incrementCounts(ctx, pipe, campaignKey(evt.CampaignID), 0, 1, 0)
			incrementCounts(ctx, pipe, campaignMinuteKey(evt.CampaignID, impressionBucket), 0, 1, 0)
			pipe.Expire(ctx, campaignMinuteKey(evt.CampaignID, impressionBucket), readModelTTL)
			pipe.Del(ctx, unknownKey)
		}

		now := time.Now().UTC().Format(time.RFC3339)
		pipe.HSet(ctx, globalKey, "last_projected_at", now)
		return nil
	})
	return err
}

func (m *Model) ProjectImpression(ctx context.Context, evt event.ImpressionEvent) error {
	bucket := bucketFor(toTime(evt.Timestamp))
	bidKey := bidMetaKey(evt.BidID)
	campaignID, err := m.client.HGet(ctx, bidKey, "campaign_id").Result()
	if err != nil && err != redis.Nil {
		return err
	}

	_, err = m.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		if campaignID != "" {
			incrementCounts(ctx, pipe, globalKey, 0, 1, 0)
			incrementCounts(ctx, pipe, summaryMinuteKey(bucket), 0, 1, 0)
			pipe.Expire(ctx, summaryMinuteKey(bucket), readModelTTL)
			pipe.SAdd(ctx, campaignSetKey, campaignID)
			incrementCounts(ctx, pipe, campaignKey(campaignID), 0, 1, 0)
			incrementCounts(ctx, pipe, campaignMinuteKey(campaignID, bucket), 0, 1, 0)
			pipe.Expire(ctx, campaignMinuteKey(campaignID, bucket), readModelTTL)
		} else {
			incrementCounts(ctx, pipe, globalKey, 1, 0, 0)
			incrementCounts(ctx, pipe, summaryMinuteKey(bucket), 1, 0, 0)
			pipe.Expire(ctx, summaryMinuteKey(bucket), readModelTTL)
			pipe.SAdd(ctx, campaignSetKey, unknownCampaignID)
			incrementCounts(ctx, pipe, campaignKey(unknownCampaignID), 1, 0, 0)
			incrementCounts(ctx, pipe, campaignMinuteKey(unknownCampaignID, bucket), 1, 0, 0)
			pipe.Expire(ctx, campaignMinuteKey(unknownCampaignID, bucket), readModelTTL)
			pipe.HSet(ctx, unknownKeyFor(evt.BidID), map[string]interface{}{
				"impression_bucket": bucket,
				"impression_ts":     strconv.FormatInt(evt.Timestamp, 10),
			})
			pipe.Expire(ctx, unknownKeyFor(evt.BidID), readModelTTL)
		}

		now := time.Now().UTC().Format(time.RFC3339)
		pipe.HSet(ctx, globalKey, "last_projected_at", now)
		return nil
	})
	return err
}

func (m *Model) Summary(ctx context.Context, from, to time.Time) (metrics.Summary, error) {
	summary := metrics.Summary{From: from.UTC(), To: to.UTC()}
	buckets := minuteBuckets(from, to)
	pipe := m.client.Pipeline()
	cmds := make([]*redis.StringStringMapCmd, 0, len(buckets))
	for _, bucket := range buckets {
		cmds = append(cmds, pipe.HGetAll(ctx, summaryMinuteKey(bucket)))
	}
	lastProjected := pipe.HGet(ctx, globalKey, "last_projected_at")
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return summary, err
	}
	for _, cmd := range cmds {
		values, err := cmd.Result()
		if err != nil && err != redis.Nil {
			return summary, err
		}
		applyHashCounts(values, &summary.BidRequests, &summary.DedupedImpressions, &summary.UnknownImpressions)
	}
	if ts, err := lastProjected.Result(); err == nil && ts != "" {
		if parsed, parseErr := time.Parse(time.RFC3339, ts); parseErr == nil {
			parsed = parsed.UTC()
			summary.LastProjectedAt = &parsed
		}
	}
	summary.Finalize(time.Now().UTC())
	return summary, nil
}

func (m *Model) ByCampaign(ctx context.Context, from, to time.Time) ([]metrics.CampaignMetrics, error) {
	campaignIDs, err := m.client.SMembers(ctx, campaignSetKey).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	sort.Strings(campaignIDs)
	buckets := minuteBuckets(from, to)
	items := make([]metrics.CampaignMetrics, 0, len(campaignIDs))

	for _, campaignID := range campaignIDs {
		item := metrics.CampaignMetrics{CampaignID: campaignID}
		pipe := m.client.Pipeline()
		cmds := make([]*redis.StringStringMapCmd, 0, len(buckets))
		for _, bucket := range buckets {
			cmds = append(cmds, pipe.HGetAll(ctx, campaignMinuteKey(campaignID, bucket)))
		}
		_, err := pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			return nil, err
		}
		for _, cmd := range cmds {
			values, err := cmd.Result()
			if err != nil && err != redis.Nil {
				return nil, err
			}
			applyHashCounts(values, &item.BidRequests, &item.DedupedImpressions, &item.UnknownImpressions)
		}
		if item.BidRequests == 0 && item.DedupedImpressions == 0 && item.UnknownImpressions == 0 {
			continue
		}
		items = append(items, item)
	}

	return metrics.NormalizeCampaigns(items), nil
}

func (m *Model) Reset(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := m.client.Scan(ctx, cursor, "rm:*", 500).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := m.client.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func incrementCounts(ctx context.Context, pipe redis.Pipeliner, key string, unknownDelta, dedupDelta, bidDelta int64) {
	if unknownDelta != 0 {
		pipe.HIncrBy(ctx, key, "unknown_impressions", unknownDelta)
	}
	if dedupDelta != 0 {
		pipe.HIncrBy(ctx, key, "deduped_impressions", dedupDelta)
	}
	if bidDelta != 0 {
		pipe.HIncrBy(ctx, key, "bid_requests", bidDelta)
	}
}

func applyHashCounts(values map[string]string, bids, dedup, unknown *int64) {
	*bids += parseInt(values["bid_requests"])
	*dedup += parseInt(values["deduped_impressions"])
	*unknown += parseInt(values["unknown_impressions"])
}

func parseInt(raw string) int64 {
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func bidMetaKey(bidID string) string {
	return fmt.Sprintf("rm:bid:%s", bidID)
}

func unknownKeyFor(bidID string) string {
	return fmt.Sprintf("rm:unknown:%s", bidID)
}

func campaignKey(campaignID string) string {
	return fmt.Sprintf("rm:campaign:%s", campaignID)
}

func summaryMinuteKey(bucket string) string {
	return fmt.Sprintf("rm:summary:min:%s", bucket)
}

func campaignMinuteKey(campaignID, bucket string) string {
	return fmt.Sprintf("rm:campaign:%s:min:%s", campaignID, bucket)
}

func bucketFor(ts time.Time) string {
	return ts.UTC().Truncate(time.Minute).Format(minuteLayout)
}

func minuteBuckets(from, to time.Time) []string {
	start := from.UTC().Truncate(time.Minute)
	end := to.UTC().Truncate(time.Minute)
	if !end.Equal(to.UTC()) {
		end = end.Add(time.Minute)
	}
	if !start.Before(end) {
		return []string{start.Format(minuteLayout)}
	}
	buckets := make([]string, 0, int(end.Sub(start)/time.Minute)+1)
	for current := start; current.Before(end); current = current.Add(time.Minute) {
		buckets = append(buckets, current.Format(minuteLayout))
	}
	return buckets
}

func toTime(ts int64) time.Time {
	if ts == 0 {
		return time.Now().UTC()
	}
	return time.Unix(ts, 0).UTC()
}
