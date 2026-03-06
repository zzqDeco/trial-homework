package loadgen

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

type OperationType string

const (
	OpBid              OperationType = "bid"
	OpBillingMatched   OperationType = "billing_matched"
	OpBillingUnknown   OperationType = "billing_unknown"
	OpBillingDuplicate OperationType = "billing_duplicate"
	OpBillingInvalid   OperationType = "billing_invalid"
)

type Config struct {
	BaseURL string

	HTTPTimeout time.Duration
	MaxRetries  int
	Seed        int64

	TargetSuccessBids      int
	TargetMatchedBilling   int
	TargetUnknownBilling   int
	TargetDuplicateBilling int
	TargetNoFillRequests   int
	TargetInvalidBilling   int

	SteadyWorkers   int
	SteadySeconds   int
	Burst1Workers   int
	Burst1Seconds   int
	Burst2Workers   int
	Burst2Seconds   int
	RecoverySeconds int

	ReportPath string
}

type StageReport struct {
	Name                  string  `json:"name"`
	StartedAt             string  `json:"started_at"`
	EndedAt               string  `json:"ended_at"`
	DurationMS            int64   `json:"duration_ms"`
	Attempts              int64   `json:"attempts"`
	HTTP2xx               int64   `json:"http_2xx"`
	HTTP4xx               int64   `json:"http_4xx"`
	HTTP5xx               int64   `json:"http_5xx"`
	NetworkErrors         int64   `json:"network_errors"`
	TimeoutErrors         int64   `json:"timeout_errors"`
	RetriesTotal          int64   `json:"retries_total"`
	BidAttempts           int64   `json:"bid_attempts"`
	BidSuccess            int64   `json:"bid_success"`
	NoFillCount           int64   `json:"nofill_count"`
	BillingAttempts       int64   `json:"billing_attempts"`
	BillingSuccess2xx     int64   `json:"billing_success_2xx"`
	DuplicateAttempts     int64   `json:"duplicate_billing_attempts"`
	InvalidAttempts       int64   `json:"invalid_billing_attempts"`
	SuccessRate           float64 `json:"success_rate"`
	ErrorRate             float64 `json:"error_rate"`
	NetworkErrorRate      float64 `json:"network_error_rate"`
	FirstMinuteErrorRate  float64 `json:"first_minute_error_rate"`
	MaxNoResponseGapMS    int64   `json:"max_no_response_gap_ms"`
	LongNoResponseGT10Sec bool    `json:"long_no_response_gt_10s"`
}

type TargetSummary struct {
	TargetSuccessBids      int `json:"target_success_bids"`
	TargetMatchedBilling   int `json:"target_matched_billing"`
	TargetUnknownBilling   int `json:"target_unknown_billing"`
	TargetDuplicateBilling int `json:"target_duplicate_billing"`
	TargetNoFillRequests   int `json:"target_nofill_requests"`
	TargetInvalidBilling   int `json:"target_invalid_billing"`
	MinTopicBids           int `json:"min_topic_bids"`
	MinTopicImpressions    int `json:"min_topic_impressions"`
}

type Report struct {
	RunID      string `json:"run_id"`
	StartedAt  string `json:"started_at"`
	EndedAt    string `json:"ended_at"`
	DurationMS int64  `json:"duration_ms"`

	BaseURL string `json:"base_url"`
	Seed    int64  `json:"seed"`

	BidAttempts          int64 `json:"bid_attempts"`
	BidSuccess           int64 `json:"bid_success"`
	BillingAttemptsTotal int64 `json:"billing_attempts_total"`
	BillingSuccess2xx    int64 `json:"billing_success_2xx"`
	HTTP4xx              int64 `json:"http_4xx"`
	HTTP5xx              int64 `json:"http_5xx"`
	NetworkErrors        int64 `json:"network_errors"`
	RetriesTotal         int64 `json:"retries_total"`

	NoFillCount              int64 `json:"nofill_count"`
	MatchedBillingUnique     int64 `json:"matched_billing_unique"`
	UnknownBillingUnique     int64 `json:"unknown_billing_unique"`
	DuplicateBillingAttempts int64 `json:"duplicate_billing_attempts"`
	InvalidBillingAttempts   int64 `json:"invalid_billing_attempts"`
	DelayedBillingCount      int64 `json:"delayed_billing_count"`

	Steady   StageReport `json:"steady"`
	Burst1   StageReport `json:"burst1"`
	Burst2   StageReport `json:"burst2"`
	Recovery StageReport `json:"recovery"`

	ExtraStages []StageReport `json:"extra_stages,omitempty"`

	Targets     TargetSummary `json:"targets"`
	TargetsMet  bool          `json:"targets_met"`
	BurstSLOMet bool          `json:"burst_slo_met"`
	ExitCode    int           `json:"exit_code"`
	Notes       []string      `json:"notes,omitempty"`
}

type stageRuntime struct {
	report       StageReport
	start        time.Time
	end          time.Time
	lastResponse time.Time
	maxGap       time.Duration
	longGap      bool
	firstMinute  stageCounter
}

type stageCounter struct {
	attempts      int64
	http4xx       int64
	http5xx       int64
	networkErrors int64
}

type Collector struct {
	mu sync.Mutex

	report Report

	stages map[string]*stageRuntime

	bidIDs      []string
	bidIDSet    map[string]struct{}
	matchedSet  map[string]struct{}
	unknownSet  map[string]struct{}
	notes       []string
	startedTime time.Time
}

func NewCollector(cfg Config) *Collector {
	now := time.Now().UTC()
	return &Collector{
		report: Report{
			RunID:     fmt.Sprintf("deliverable-a-%d", now.UnixNano()),
			StartedAt: now.Format(time.RFC3339),
			BaseURL:   cfg.BaseURL,
			Seed:      cfg.Seed,
			Targets: TargetSummary{
				TargetSuccessBids:      cfg.TargetSuccessBids,
				TargetMatchedBilling:   cfg.TargetMatchedBilling,
				TargetUnknownBilling:   cfg.TargetUnknownBilling,
				TargetDuplicateBilling: cfg.TargetDuplicateBilling,
				TargetNoFillRequests:   cfg.TargetNoFillRequests,
				TargetInvalidBilling:   cfg.TargetInvalidBilling,
				MinTopicBids:           10001,
				MinTopicImpressions:    10001,
			},
		},
		stages:      make(map[string]*stageRuntime),
		bidIDSet:    make(map[string]struct{}),
		matchedSet:  make(map[string]struct{}),
		unknownSet:  make(map[string]struct{}),
		startedTime: now,
	}
}

func (c *Collector) AddNote(note string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notes = append(c.notes, note)
}

func (c *Collector) StartStage(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	r := c.getOrCreateStageLocked(name)
	r.start = now
	r.lastResponse = now
	r.report.Name = name
	r.report.StartedAt = now.Format(time.RFC3339)
}

func (c *Collector) EndStage(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	r := c.getOrCreateStageLocked(name)
	r.end = now
	r.report.EndedAt = now.Format(time.RFC3339)
	if !r.start.IsZero() {
		r.report.DurationMS = now.Sub(r.start).Milliseconds()
	}
	r.report.MaxNoResponseGapMS = r.maxGap.Milliseconds()
	r.report.LongNoResponseGT10Sec = r.longGap
	c.computeStageRatesLocked(r)
}

func (c *Collector) UpdateStageGap(name string, gap time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.getOrCreateStageLocked(name)
	if gap > r.maxGap {
		r.maxGap = gap
	}
	if gap > 10*time.Second {
		r.longGap = true
	}
}

func (c *Collector) StageLastResponse(name string) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.getOrCreateStageLocked(name)
	return r.lastResponse
}

func (c *Collector) RecordRequest(stage string, op OperationType, result RequestResult) {
	c.mu.Lock()
	defer c.mu.Unlock()

	r := c.getOrCreateStageLocked(stage)
	r.report.Attempts++
	c.report.RetriesTotal += int64(result.Retries)
	r.report.RetriesTotal += int64(result.Retries)

	switch op {
	case OpBid:
		c.report.BidAttempts++
		r.report.BidAttempts++
	case OpBillingMatched, OpBillingUnknown, OpBillingDuplicate, OpBillingInvalid:
		c.report.BillingAttemptsTotal++
		r.report.BillingAttempts++
	}

	if result.NetworkError {
		c.report.NetworkErrors++
		r.report.NetworkErrors++
		if result.Timeout {
			r.report.TimeoutErrors++
		}
	} else if result.StatusCode >= 200 && result.StatusCode < 300 {
		r.report.HTTP2xx++
		if op == OpBillingMatched || op == OpBillingUnknown || op == OpBillingDuplicate || op == OpBillingInvalid {
			c.report.BillingSuccess2xx++
			r.report.BillingSuccess2xx++
		}
		r.lastResponse = time.Now().UTC()
	} else if result.StatusCode >= 400 && result.StatusCode < 500 {
		c.report.HTTP4xx++
		r.report.HTTP4xx++
		r.lastResponse = time.Now().UTC()
	} else if result.StatusCode >= 500 {
		c.report.HTTP5xx++
		r.report.HTTP5xx++
		r.lastResponse = time.Now().UTC()
	}

	if stage == "recovery" && !r.start.IsZero() {
		elapsed := time.Since(r.start)
		if elapsed <= 60*time.Second {
			r.firstMinute.attempts++
			if result.NetworkError {
				r.firstMinute.networkErrors++
			} else if result.StatusCode >= 400 && result.StatusCode < 500 {
				r.firstMinute.http4xx++
			} else if result.StatusCode >= 500 {
				r.firstMinute.http5xx++
			}
		}
	}
}

func (c *Collector) MarkBidSuccess(stage, bidID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bidID == "" {
		return
	}
	r := c.getOrCreateStageLocked(stage)
	c.report.BidSuccess++
	r.report.BidSuccess++
	if _, ok := c.bidIDSet[bidID]; !ok {
		c.bidIDSet[bidID] = struct{}{}
		c.bidIDs = append(c.bidIDs, bidID)
	}
}

func (c *Collector) MarkNoFill(stage string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.getOrCreateStageLocked(stage)
	c.report.NoFillCount++
	r.report.NoFillCount++
}

func (c *Collector) MarkMatchedBillingSuccess(stage, bidID string, delayed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bidID == "" {
		return
	}
	if delayed {
		c.report.DelayedBillingCount++
	}
	if _, ok := c.matchedSet[bidID]; !ok {
		c.matchedSet[bidID] = struct{}{}
		c.report.MatchedBillingUnique++
	}
}

func (c *Collector) MarkUnknownBillingSuccess(bidID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if bidID == "" {
		return
	}
	if _, ok := c.unknownSet[bidID]; !ok {
		c.unknownSet[bidID] = struct{}{}
		c.report.UnknownBillingUnique++
	}
}

func (c *Collector) MarkDuplicateBillingAttempt(stage string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.getOrCreateStageLocked(stage)
	c.report.DuplicateBillingAttempts++
	r.report.DuplicateAttempts++
}

func (c *Collector) MarkInvalidBillingAttempt(stage string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.getOrCreateStageLocked(stage)
	c.report.InvalidBillingAttempts++
	r.report.InvalidAttempts++
}

func (c *Collector) BidSuccessCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.report.BidSuccess
}

func (c *Collector) NoFillCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.report.NoFillCount
}

func (c *Collector) MatchedUniqueCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.report.MatchedBillingUnique
}

func (c *Collector) UnknownUniqueCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.report.UnknownBillingUnique
}

func (c *Collector) DuplicateAttemptsCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.report.DuplicateBillingAttempts
}

func (c *Collector) InvalidAttemptsCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.report.InvalidBillingAttempts
}

func (c *Collector) UnmatchedBidIDs(limit int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if limit <= 0 {
		return nil
	}
	out := make([]string, 0, limit)
	for _, bidID := range c.bidIDs {
		if _, ok := c.matchedSet[bidID]; ok {
			continue
		}
		out = append(out, bidID)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (c *Collector) MatchedBidIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.matchedSet))
	for k := range c.matchedSet {
		out = append(out, k)
	}
	return out
}

func (c *Collector) Finalize(cfg Config) Report {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UTC()
	c.report.EndedAt = now.Format(time.RFC3339)
	c.report.DurationMS = now.Sub(c.startedTime).Milliseconds()

	c.report.Steady = c.toStageReportLocked("steady")
	c.report.Burst1 = c.toStageReportLocked("burst1")
	c.report.Burst2 = c.toStageReportLocked("burst2")
	c.report.Recovery = c.toStageReportLocked("recovery")

	for _, name := range c.sortedStageNamesLocked() {
		if name == "steady" || name == "burst1" || name == "burst2" || name == "recovery" {
			continue
		}
		c.report.ExtraStages = append(c.report.ExtraStages, c.toStageReportLocked(name))
	}

	c.report.TargetsMet =
		c.report.BidSuccess >= int64(cfg.TargetSuccessBids) &&
			c.report.MatchedBillingUnique >= int64(cfg.TargetMatchedBilling) &&
			c.report.UnknownBillingUnique >= int64(cfg.TargetUnknownBilling) &&
			c.report.DuplicateBillingAttempts >= int64(cfg.TargetDuplicateBilling) &&
			c.report.NoFillCount >= int64(cfg.TargetNoFillRequests) &&
			c.report.InvalidBillingAttempts >= int64(cfg.TargetInvalidBilling)

	steadyErr := c.report.Steady.ErrorRate
	recoveryErr := c.report.Recovery.FirstMinuteErrorRate
	if recoveryErr == 0 {
		recoveryErr = c.report.Recovery.ErrorRate
	}

	burst1Pass := c.report.Burst1.SuccessRate >= 0.98 && c.report.Burst1.NetworkErrorRate < 0.01 && !c.report.Burst1.LongNoResponseGT10Sec
	burst2Pass := c.report.Burst2.SuccessRate >= 0.98 && c.report.Burst2.NetworkErrorRate < 0.01 && !c.report.Burst2.LongNoResponseGT10Sec
	recoveryPass := math.Abs(recoveryErr-steadyErr) <= 0.005
	c.report.BurstSLOMet = burst1Pass && burst2Pass && recoveryPass

	if len(c.notes) > 0 {
		c.report.Notes = append(c.report.Notes, c.notes...)
	}

	if !c.report.TargetsMet {
		c.report.Notes = append(c.report.Notes, "target counters not fully met")
	}
	if !c.report.BurstSLOMet {
		c.report.Notes = append(c.report.Notes, "burst SLO checks did not fully pass")
	}

	return c.report
}

func (c *Collector) WriteReport(path string, report Report) error {
	if path == "" {
		path = "deliverable_a_report.json"
	}
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

func (c *Collector) getOrCreateStageLocked(name string) *stageRuntime {
	if name == "" {
		name = "default"
	}
	if st, ok := c.stages[name]; ok {
		return st
	}
	st := &stageRuntime{}
	st.report.Name = name
	c.stages[name] = st
	return st
}

func (c *Collector) toStageReportLocked(name string) StageReport {
	st := c.getOrCreateStageLocked(name)
	st.report.MaxNoResponseGapMS = st.maxGap.Milliseconds()
	st.report.LongNoResponseGT10Sec = st.longGap
	if !st.start.IsZero() && st.report.StartedAt == "" {
		st.report.StartedAt = st.start.Format(time.RFC3339)
	}
	if !st.end.IsZero() && st.report.EndedAt == "" {
		st.report.EndedAt = st.end.Format(time.RFC3339)
	}
	if !st.start.IsZero() && !st.end.IsZero() {
		st.report.DurationMS = st.end.Sub(st.start).Milliseconds()
	}
	c.computeStageRatesLocked(st)
	return st.report
}

func (c *Collector) sortedStageNamesLocked() []string {
	names := make([]string, 0, len(c.stages))
	for n := range c.stages {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (c *Collector) computeStageRatesLocked(st *stageRuntime) {
	if st.report.Attempts > 0 {
		st.report.SuccessRate = float64(st.report.HTTP2xx) / float64(st.report.Attempts)
		errCount := st.report.HTTP4xx + st.report.HTTP5xx + st.report.NetworkErrors
		st.report.ErrorRate = float64(errCount) / float64(st.report.Attempts)
		st.report.NetworkErrorRate = float64(st.report.NetworkErrors) / float64(st.report.Attempts)
	}
	if st.firstMinute.attempts > 0 {
		errCount := st.firstMinute.http4xx + st.firstMinute.http5xx + st.firstMinute.networkErrors
		st.report.FirstMinuteErrorRate = float64(errCount) / float64(st.firstMinute.attempts)
	}
}
