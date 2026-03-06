package loadgen

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type stageMode string

const (
	stageMixed   stageMode = "mixed"
	stageBidOnly stageMode = "bid_only"
)

type timedStage struct {
	Name               string
	Mode               stageMode
	Workers            int
	Duration           time.Duration
	NoFillRatio        float64
	DuplicateRatio     float64
	DelayedBillingRate float64
}

func DefaultConfig() Config {
	return Config{
		BaseURL: "http://localhost:8080",

		HTTPTimeout: 4 * time.Second,
		MaxRetries:  2,
		Seed:        20260306,

		TargetSuccessBids:      12500,
		TargetMatchedBilling:   11000,
		TargetUnknownBilling:   1500,
		TargetDuplicateBilling: 3000,
		TargetNoFillRequests:   2000,
		TargetInvalidBilling:   1000,

		SteadyWorkers:   50,
		SteadySeconds:   120,
		Burst1Workers:   200,
		Burst1Seconds:   30,
		Burst2Workers:   300,
		Burst2Seconds:   20,
		RecoverySeconds: 120,

		ReportPath: "deliverable_a_report.json",
	}
}

func Run(ctx context.Context, cfg Config) (Report, error) {
	collector := NewCollector(cfg)
	client := NewClient(cfg)

	if err := healthCheckWithRetry(ctx, client); err != nil {
		report := collector.Finalize(cfg)
		report.ExitCode = 1
		report.Notes = append(report.Notes, fmt.Sprintf("health check failed: %v", err))
		_ = collector.WriteReport(cfg.ReportPath, report)
		return report, err
	}

	stages := []timedStage{
		{
			Name:               "steady",
			Mode:               stageMixed,
			Workers:            cfg.SteadyWorkers,
			Duration:           time.Duration(cfg.SteadySeconds) * time.Second,
			NoFillRatio:        0.10,
			DuplicateRatio:     0.01,
			DelayedBillingRate: 0.02,
		},
		{
			Name:               "burst1",
			Mode:               stageMixed,
			Workers:            cfg.Burst1Workers,
			Duration:           time.Duration(cfg.Burst1Seconds) * time.Second,
			NoFillRatio:        0.10,
			DuplicateRatio:     0.05,
			DelayedBillingRate: 0.05,
		},
		{
			Name:        "burst2",
			Mode:        stageBidOnly,
			Workers:     cfg.Burst2Workers,
			Duration:    time.Duration(cfg.Burst2Seconds) * time.Second,
			NoFillRatio: 0.10,
		},
		{
			Name:               "recovery",
			Mode:               stageMixed,
			Workers:            cfg.SteadyWorkers,
			Duration:           time.Duration(cfg.RecoverySeconds) * time.Second,
			NoFillRatio:        0.10,
			DuplicateRatio:     0.01,
			DelayedBillingRate: 0.02,
		},
	}

	seed := cfg.Seed
	for _, st := range stages {
		if err := runTimedStage(ctx, client, collector, st, seed); err != nil {
			report := collector.Finalize(cfg)
			report.ExitCode = 1
			report.Notes = append(report.Notes, fmt.Sprintf("stage %s failed: %v", st.Name, err))
			_ = collector.WriteReport(cfg.ReportPath, report)
			return report, err
		}
		seed += 137
	}

	if err := topUpNoFill(ctx, client, collector, cfg); err != nil {
		return writeErrorReport(cfg, collector, err)
	}
	if err := topUpSuccessBids(ctx, client, collector, cfg); err != nil {
		return writeErrorReport(cfg, collector, err)
	}
	if err := topUpMatchedBilling(ctx, client, collector, cfg); err != nil {
		return writeErrorReport(cfg, collector, err)
	}
	if err := topUpUnknownBilling(ctx, client, collector, cfg); err != nil {
		return writeErrorReport(cfg, collector, err)
	}
	if err := topUpDuplicateBilling(ctx, client, collector, cfg); err != nil {
		return writeErrorReport(cfg, collector, err)
	}
	if err := injectInvalidBilling(ctx, client, collector, cfg); err != nil {
		return writeErrorReport(cfg, collector, err)
	}

	report := collector.Finalize(cfg)
	if report.TargetsMet && report.BurstSLOMet {
		report.ExitCode = 0
	} else {
		report.ExitCode = 1
	}

	if err := collector.WriteReport(cfg.ReportPath, report); err != nil {
		return report, err
	}

	if report.ExitCode != 0 {
		return report, fmt.Errorf("deliverable A checks did not pass; see %s", cfg.ReportPath)
	}
	return report, nil
}

func writeErrorReport(cfg Config, collector *Collector, runErr error) (Report, error) {
	report := collector.Finalize(cfg)
	report.ExitCode = 1
	report.Notes = append(report.Notes, runErr.Error())
	_ = collector.WriteReport(cfg.ReportPath, report)
	return report, runErr
}

func healthCheckWithRetry(ctx context.Context, client *Client) error {
	var lastErr error
	for i := 0; i < 5; i++ {
		if err := client.Healthz(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if err := sleepWithContext(ctx, time.Duration(i+1)*300*time.Millisecond); err != nil {
			break
		}
	}
	return lastErr
}

func runTimedStage(ctx context.Context, client *Client, collector *Collector, stage timedStage, seed int64) error {
	if stage.Workers <= 0 {
		return fmt.Errorf("stage %s workers must be > 0", stage.Name)
	}
	if stage.Duration <= 0 {
		return fmt.Errorf("stage %s duration must be > 0", stage.Name)
	}

	collector.StartStage(stage.Name)
	defer collector.EndStage(stage.Name)

	stageCtx, cancel := context.WithTimeout(ctx, stage.Duration)
	defer cancel()

	var wg sync.WaitGroup

	monitorDone := make(chan struct{})
	go monitorNoResponseGap(stageCtx, collector, stage.Name, monitorDone)

	for workerID := 0; workerID < stage.Workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(workerID) + 1))
			for {
				select {
				case <-stageCtx.Done():
					return
				default:
					runSingleStageIteration(stageCtx, client, collector, stage, r)
				}
			}
		}(workerID)
	}

	wg.Wait()
	close(monitorDone)
	return nil
}

func monitorNoResponseGap(ctx context.Context, collector *Collector, stageName string, done <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			last := collector.StageLastResponse(stageName)
			gap := time.Since(last)
			collector.UpdateStageGap(stageName, gap)
		}
	}
}

func runSingleStageIteration(ctx context.Context, client *Client, collector *Collector, stage timedStage, r *rand.Rand) {
	now := time.Now().Unix()
	nofill := r.Float64() < stage.NoFillRatio
	userID := pickUserID(r, nofill)
	appBundle := pickAppBundle(r)
	placement := pickPlacement(r)

	bidRes, bidID, isNoFill := client.Bid(ctx, BidRequestPayload{
		UserIDFV:    userID,
		AppBundle:   appBundle,
		PlacementID: placement,
		Timestamp:   now,
	})
	collector.RecordRequest(stage.Name, OpBid, bidRes)

	if isNoFill || bidRes.StatusCode == 204 {
		collector.MarkNoFill(stage.Name)
		return
	}
	if bidRes.StatusCode != 200 || bidID == "" {
		return
	}
	collector.MarkBidSuccess(stage.Name, bidID)

	if stage.Mode == stageBidOnly {
		return
	}

	delayed := false
	if stage.DelayedBillingRate > 0 && r.Float64() < stage.DelayedBillingRate {
		delayed = true
		if err := sleepWithContext(ctx, time.Duration(1+r.Intn(3))*time.Second); err != nil {
			return
		}
	}

	billingRes := client.Billing(ctx, BillingRequestPayload{BidID: bidID, Timestamp: time.Now().Unix()})
	collector.RecordRequest(stage.Name, OpBillingMatched, billingRes)
	if is2xx(billingRes.StatusCode) {
		collector.MarkMatchedBillingSuccess(stage.Name, bidID, delayed)
	}

	if stage.DuplicateRatio > 0 && r.Float64() < stage.DuplicateRatio {
		collector.MarkDuplicateBillingAttempt(stage.Name)
		dupRes := client.Billing(ctx, BillingRequestPayload{BidID: bidID, Timestamp: time.Now().Unix()})
		collector.RecordRequest(stage.Name, OpBillingDuplicate, dupRes)
	}
}

func topUpNoFill(ctx context.Context, client *Client, collector *Collector, cfg Config) error {
	collector.StartStage("topup_nofill")
	defer collector.EndStage("topup_nofill")

	remaining := cfg.TargetNoFillRequests - int(collector.NoFillCount())
	if remaining <= 0 {
		return nil
	}

	return runParallelCount(ctx, cfg.SteadyWorkers, remaining, cfg.Seed+5000, func(task int, r *rand.Rand) {
		bidRes, _, isNoFill := client.Bid(ctx, BidRequestPayload{
			UserIDFV:    "789",
			AppBundle:   pickAppBundle(r),
			PlacementID: pickPlacement(r),
			Timestamp:   time.Now().Unix(),
		})
		collector.RecordRequest("topup_nofill", OpBid, bidRes)
		if isNoFill || bidRes.StatusCode == 204 {
			collector.MarkNoFill("topup_nofill")
		}
	})
}

func topUpSuccessBids(ctx context.Context, client *Client, collector *Collector, cfg Config) error {
	collector.StartStage("topup_success_bids")
	defer collector.EndStage("topup_success_bids")

	loops := 0
	for {
		remaining := cfg.TargetSuccessBids - int(collector.BidSuccessCount())
		if remaining <= 0 {
			return nil
		}
		if loops > 40 {
			return fmt.Errorf("unable to reach target_success_bids after repeated top-ups")
		}
		batch := remaining
		if batch > 2000 {
			batch = 2000
		}
		err := runParallelCount(ctx, cfg.SteadyWorkers, batch, cfg.Seed+int64(6000+loops), func(task int, r *rand.Rand) {
			bidRes, bidID, isNoFill := client.Bid(ctx, BidRequestPayload{
				UserIDFV:    pickUserID(r, false),
				AppBundle:   pickAppBundle(r),
				PlacementID: pickPlacement(r),
				Timestamp:   time.Now().Unix(),
			})
			collector.RecordRequest("topup_success_bids", OpBid, bidRes)
			if isNoFill || bidRes.StatusCode == 204 {
				collector.MarkNoFill("topup_success_bids")
				return
			}
			if bidRes.StatusCode == 200 && bidID != "" {
				collector.MarkBidSuccess("topup_success_bids", bidID)
			}
		})
		if err != nil {
			return err
		}
		loops++
	}
}

func topUpMatchedBilling(ctx context.Context, client *Client, collector *Collector, cfg Config) error {
	collector.StartStage("topup_matched_billing")
	defer collector.EndStage("topup_matched_billing")

	loops := 0
	for {
		remaining := cfg.TargetMatchedBilling - int(collector.MatchedUniqueCount())
		if remaining <= 0 {
			return nil
		}
		if loops > 60 {
			return fmt.Errorf("unable to reach target_matched_billing after repeated top-ups")
		}

		ids := collector.UnmatchedBidIDs(remaining)
		if len(ids) == 0 {
			if err := topUpSuccessBids(ctx, client, collector, Config{
				BaseURL:                cfg.BaseURL,
				HTTPTimeout:            cfg.HTTPTimeout,
				MaxRetries:             cfg.MaxRetries,
				Seed:                   cfg.Seed + int64(7000+loops),
				TargetSuccessBids:      int(collector.BidSuccessCount()) + remaining + 500,
				TargetMatchedBilling:   cfg.TargetMatchedBilling,
				TargetUnknownBilling:   cfg.TargetUnknownBilling,
				TargetDuplicateBilling: cfg.TargetDuplicateBilling,
				TargetNoFillRequests:   cfg.TargetNoFillRequests,
				TargetInvalidBilling:   cfg.TargetInvalidBilling,
				SteadyWorkers:          cfg.SteadyWorkers,
			}); err != nil {
				return err
			}
			ids = collector.UnmatchedBidIDs(remaining)
			if len(ids) == 0 {
				return fmt.Errorf("no unmatched bid ids available for matched billing")
			}
		}

		err := runParallelSlice(ctx, cfg.SteadyWorkers, ids, cfg.Seed+int64(8000+loops), func(bidID string, r *rand.Rand) {
			delayed := r.Float64() < 0.10
			if delayed {
				if err := sleepWithContext(ctx, time.Duration(1+r.Intn(3))*time.Second); err != nil {
					return
				}
			}
			res := client.Billing(ctx, BillingRequestPayload{BidID: bidID, Timestamp: time.Now().Unix()})
			collector.RecordRequest("topup_matched_billing", OpBillingMatched, res)
			if is2xx(res.StatusCode) {
				collector.MarkMatchedBillingSuccess("topup_matched_billing", bidID, delayed)
			}
		})
		if err != nil {
			return err
		}
		loops++
	}
}

func topUpUnknownBilling(ctx context.Context, client *Client, collector *Collector, cfg Config) error {
	collector.StartStage("topup_unknown_billing")
	defer collector.EndStage("topup_unknown_billing")

	remaining := cfg.TargetUnknownBilling - int(collector.UnknownUniqueCount())
	if remaining <= 0 {
		return nil
	}

	return runParallelCount(ctx, cfg.SteadyWorkers, remaining, cfg.Seed+9000, func(task int, r *rand.Rand) {
		id := uuid.NewString()
		res := client.Billing(ctx, BillingRequestPayload{BidID: id, Timestamp: time.Now().Unix()})
		collector.RecordRequest("topup_unknown_billing", OpBillingUnknown, res)
		if is2xx(res.StatusCode) {
			collector.MarkUnknownBillingSuccess(id)
		}
	})
}

func topUpDuplicateBilling(ctx context.Context, client *Client, collector *Collector, cfg Config) error {
	collector.StartStage("topup_duplicate_billing")
	defer collector.EndStage("topup_duplicate_billing")

	remaining := cfg.TargetDuplicateBilling - int(collector.DuplicateAttemptsCount())
	if remaining <= 0 {
		return nil
	}

	matchedIDs := collector.MatchedBidIDs()
	if len(matchedIDs) == 0 {
		return fmt.Errorf("cannot run duplicate billing top-up with no matched bid ids")
	}

	return runParallelCount(ctx, cfg.SteadyWorkers, remaining, cfg.Seed+10000, func(task int, r *rand.Rand) {
		bidID := matchedIDs[r.Intn(len(matchedIDs))]
		collector.MarkDuplicateBillingAttempt("topup_duplicate_billing")
		res := client.Billing(ctx, BillingRequestPayload{BidID: bidID, Timestamp: time.Now().Unix()})
		collector.RecordRequest("topup_duplicate_billing", OpBillingDuplicate, res)
	})
}

func injectInvalidBilling(ctx context.Context, client *Client, collector *Collector, cfg Config) error {
	collector.StartStage("invalid_injection")
	defer collector.EndStage("invalid_injection")

	remaining := cfg.TargetInvalidBilling - int(collector.InvalidAttemptsCount())
	if remaining <= 0 {
		return nil
	}

	return runParallelCount(ctx, cfg.SteadyWorkers, remaining, cfg.Seed+11000, func(task int, r *rand.Rand) {
		collector.MarkInvalidBillingAttempt("invalid_injection")
		var body []byte
		if task%2 == 0 {
			body = []byte(fmt.Sprintf(`{"timestamp": %d}`, time.Now().Unix()))
		} else {
			body = []byte(`{"bid_id":"broken"`)
		}
		res := client.RawBilling(ctx, body)
		collector.RecordRequest("invalid_injection", OpBillingInvalid, res)
	})
}

func runParallelCount(ctx context.Context, workers, total int, seed int64, fn func(task int, r *rand.Rand)) error {
	if total <= 0 {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}

	var idx atomic.Int64
	var wg sync.WaitGroup

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(workerID)*101 + 7))
			for {
				n := int(idx.Add(1) - 1)
				if n >= total {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
					fn(n, r)
				}
			}
		}(workerID)
	}

	wg.Wait()
	return ctx.Err()
}

func runParallelSlice(ctx context.Context, workers int, items []string, seed int64, fn func(item string, r *rand.Rand)) error {
	if len(items) == 0 {
		return nil
	}
	if workers <= 0 {
		workers = 1
	}

	var idx atomic.Int64
	var wg sync.WaitGroup

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed + int64(workerID)*97 + 17))
			for {
				n := int(idx.Add(1) - 1)
				if n >= len(items) {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
					fn(items[n], r)
				}
			}
		}(workerID)
	}

	wg.Wait()
	return ctx.Err()
}

func pickUserID(r *rand.Rand, forceNoFill bool) string {
	if forceNoFill {
		return "789"
	}
	v := r.Float64()
	switch {
	case v < 0.45:
		return "123"
	case v < 0.80:
		return "456"
	default:
		return fmt.Sprintf("u-%06d", r.Intn(1_000_000))
	}
}

func pickAppBundle(r *rand.Rand) string {
	apps := []string{
		"com.game.alpha",
		"com.video.beta",
		"com.news.gamma",
		"com.music.delta",
	}
	return apps[r.Intn(len(apps))]
}

func pickPlacement(r *rand.Rand) string {
	placements := []string{
		"feed_top",
		"reward_video",
		"interstitial",
		"home_banner",
	}
	return placements[r.Intn(len(placements))]
}

func is2xx(code int) bool {
	return code >= 200 && code < 300
}
