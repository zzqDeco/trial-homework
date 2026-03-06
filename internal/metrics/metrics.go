package metrics

import (
	"sort"
	"time"
)

type Summary struct {
	From                time.Time  `json:"from"`
	To                  time.Time  `json:"to"`
	Source              string     `json:"source"`
	BidRequests         int64      `json:"bid_requests"`
	DedupedImpressions  int64      `json:"deduped_impressions"`
	UnknownImpressions  int64      `json:"unknown_impressions"`
	ViewRate            float64    `json:"view_rate"`
	LastProjectedAt     *time.Time `json:"last_projected_at,omitempty"`
	ProjectionLagSecond float64    `json:"projection_lag_seconds,omitempty"`
}

type CampaignMetrics struct {
	CampaignID         string  `json:"campaign_id"`
	BidRequests        int64   `json:"bid_requests"`
	DedupedImpressions int64   `json:"deduped_impressions"`
	UnknownImpressions int64   `json:"unknown_impressions"`
	ViewRate           float64 `json:"view_rate"`
}

type TimeSeriesPoint struct {
	TS                 time.Time `json:"ts"`
	BidRequests        int64     `json:"bid_requests"`
	DedupedImpressions int64     `json:"deduped_impressions"`
	UnknownImpressions int64     `json:"unknown_impressions"`
	ViewRate           float64   `json:"view_rate"`
}

type TimeSeries struct {
	From       time.Time         `json:"from"`
	To         time.Time         `json:"to"`
	Source     string            `json:"source"`
	Resolution string            `json:"resolution"`
	Points     []TimeSeriesPoint `json:"points"`
}

func (s *Summary) Finalize(now time.Time) {
	s.ViewRate = ComputeViewRate(s.DedupedImpressions, s.BidRequests)
	if s.LastProjectedAt != nil {
		s.ProjectionLagSecond = now.Sub(*s.LastProjectedAt).Seconds()
		if s.ProjectionLagSecond < 0 {
			s.ProjectionLagSecond = 0
		}
	}
}

func ComputeViewRate(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func NormalizeCampaigns(items []CampaignMetrics) []CampaignMetrics {
	for i := range items {
		items[i].ViewRate = ComputeViewRate(items[i].DedupedImpressions, items[i].BidRequests)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].BidRequests == items[j].BidRequests {
			return items[i].CampaignID < items[j].CampaignID
		}
		return items[i].BidRequests > items[j].BidRequests
	})
	return items
}

func MergeSummary(a, b Summary) Summary {
	out := Summary{
		From:               a.From,
		To:                 b.To,
		BidRequests:        a.BidRequests + b.BidRequests,
		DedupedImpressions: a.DedupedImpressions + b.DedupedImpressions,
		UnknownImpressions: a.UnknownImpressions + b.UnknownImpressions,
	}
	if a.LastProjectedAt != nil {
		out.LastProjectedAt = a.LastProjectedAt
	}
	if b.LastProjectedAt != nil && (out.LastProjectedAt == nil || b.LastProjectedAt.After(*out.LastProjectedAt)) {
		out.LastProjectedAt = b.LastProjectedAt
	}
	return out
}

func MergeCampaigns(left, right []CampaignMetrics) []CampaignMetrics {
	merged := make(map[string]CampaignMetrics, len(left)+len(right))
	for _, item := range left {
		merged[item.CampaignID] = item
	}
	for _, item := range right {
		current := merged[item.CampaignID]
		current.CampaignID = item.CampaignID
		current.BidRequests += item.BidRequests
		current.DedupedImpressions += item.DedupedImpressions
		current.UnknownImpressions += item.UnknownImpressions
		merged[item.CampaignID] = current
	}
	out := make([]CampaignMetrics, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	return NormalizeCampaigns(out)
}

func NormalizeTimeSeries(points []TimeSeriesPoint) []TimeSeriesPoint {
	for i := range points {
		points[i].ViewRate = ComputeViewRate(points[i].DedupedImpressions, points[i].BidRequests)
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].TS.Before(points[j].TS)
	})
	return points
}

func MergeTimeSeries(left, right TimeSeries) TimeSeries {
	merged := make(map[time.Time]TimeSeriesPoint, len(left.Points)+len(right.Points))
	for _, point := range left.Points {
		merged[point.TS.UTC()] = point
	}
	for _, point := range right.Points {
		key := point.TS.UTC()
		current := merged[key]
		current.TS = key
		current.BidRequests += point.BidRequests
		current.DedupedImpressions += point.DedupedImpressions
		current.UnknownImpressions += point.UnknownImpressions
		merged[key] = current
	}

	out := TimeSeries{
		From:       left.From,
		To:         right.To,
		Resolution: left.Resolution,
	}
	out.Points = make([]TimeSeriesPoint, 0, len(merged))
	for _, point := range merged {
		out.Points = append(out.Points, point)
	}
	out.Points = NormalizeTimeSeries(out.Points)
	return out
}
