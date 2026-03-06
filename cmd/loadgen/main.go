package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"bidsrv/internal/loadgen"
)

func main() {
	cfg := loadgen.DefaultConfig()

	// Flags override the default full profile for smoke runs or VM-specific tuning.
	flag.StringVar(&cfg.BaseURL, "base-url", cfg.BaseURL, "base URL for bidding server")
	httpTimeoutMS := int(cfg.HTTPTimeout / time.Millisecond)
	flag.IntVar(&httpTimeoutMS, "http-timeout-ms", httpTimeoutMS, "HTTP request timeout in milliseconds")
	flag.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries, "max retries for network / 5xx errors")
	flag.Int64Var(&cfg.Seed, "seed", cfg.Seed, "deterministic random seed")

	flag.IntVar(&cfg.TargetSuccessBids, "target-success-bids", cfg.TargetSuccessBids, "target successful bid count")
	flag.IntVar(&cfg.TargetMatchedBilling, "target-matched-billing", cfg.TargetMatchedBilling, "target unique matched billing count")
	flag.IntVar(&cfg.TargetUnknownBilling, "target-unknown-billing", cfg.TargetUnknownBilling, "target unique unknown billing count")
	flag.IntVar(&cfg.TargetDuplicateBilling, "target-duplicate-billing", cfg.TargetDuplicateBilling, "target duplicate billing attempts")
	flag.IntVar(&cfg.TargetNoFillRequests, "target-nofill-requests", cfg.TargetNoFillRequests, "target no-fill bid requests")
	flag.IntVar(&cfg.TargetInvalidBilling, "target-invalid-billing", cfg.TargetInvalidBilling, "target invalid billing attempts")

	flag.IntVar(&cfg.SteadyWorkers, "steady-workers", cfg.SteadyWorkers, "steady and recovery workers")
	flag.IntVar(&cfg.SteadySeconds, "steady-seconds", cfg.SteadySeconds, "steady baseline duration (seconds)")
	flag.IntVar(&cfg.Burst1Workers, "burst1-workers", cfg.Burst1Workers, "burst1 workers")
	flag.IntVar(&cfg.Burst1Seconds, "burst1-seconds", cfg.Burst1Seconds, "burst1 duration (seconds)")
	flag.IntVar(&cfg.Burst2Workers, "burst2-workers", cfg.Burst2Workers, "burst2 workers")
	flag.IntVar(&cfg.Burst2Seconds, "burst2-seconds", cfg.Burst2Seconds, "burst2 duration (seconds)")
	flag.IntVar(&cfg.RecoverySeconds, "recovery-seconds", cfg.RecoverySeconds, "recovery duration (seconds)")
	flag.StringVar(&cfg.ReportPath, "report-path", cfg.ReportPath, "output report path")
	flag.Parse()

	cfg.HTTPTimeout = time.Duration(httpTimeoutMS) * time.Millisecond

	ctx := context.Background()
	report, err := loadgen.Run(ctx, cfg)
	if err != nil {
		log.Printf("loadgen finished with failures: %v", err)
	}

	printSummary(report, cfg.ReportPath)

	if report.ExitCode != 0 {
		os.Exit(report.ExitCode)
	}
}

func printSummary(report loadgen.Report, reportPath string) {
	// Print a compact run summary while the full JSON report stays on disk.
	type summary struct {
		RunID                    string  `json:"run_id"`
		ReportPath               string  `json:"report_path"`
		BidAttempts              int64   `json:"bid_attempts"`
		BidSuccess               int64   `json:"bid_success"`
		BillingAttemptsTotal     int64   `json:"billing_attempts_total"`
		BillingSuccess2xx        int64   `json:"billing_success_2xx"`
		NoFillCount              int64   `json:"nofill_count"`
		MatchedBillingUnique     int64   `json:"matched_billing_unique"`
		UnknownBillingUnique     int64   `json:"unknown_billing_unique"`
		DuplicateBillingAttempts int64   `json:"duplicate_billing_attempts"`
		InvalidBillingAttempts   int64   `json:"invalid_billing_attempts"`
		TargetsMet               bool    `json:"targets_met"`
		BurstSLOMet              bool    `json:"burst_slo_met"`
		SteadyErrorRate          float64 `json:"steady_error_rate"`
		RecoveryFirstMinuteError float64 `json:"recovery_first_minute_error_rate"`
		ExitCode                 int     `json:"exit_code"`
	}

	recoveryErr := report.Recovery.FirstMinuteErrorRate
	if recoveryErr == 0 {
		recoveryErr = report.Recovery.ErrorRate
	}

	out := summary{
		RunID:                    report.RunID,
		ReportPath:               reportPath,
		BidAttempts:              report.BidAttempts,
		BidSuccess:               report.BidSuccess,
		BillingAttemptsTotal:     report.BillingAttemptsTotal,
		BillingSuccess2xx:        report.BillingSuccess2xx,
		NoFillCount:              report.NoFillCount,
		MatchedBillingUnique:     report.MatchedBillingUnique,
		UnknownBillingUnique:     report.UnknownBillingUnique,
		DuplicateBillingAttempts: report.DuplicateBillingAttempts,
		InvalidBillingAttempts:   report.InvalidBillingAttempts,
		TargetsMet:               report.TargetsMet,
		BurstSLOMet:              report.BurstSLOMet,
		SteadyErrorRate:          report.Steady.ErrorRate,
		RecoveryFirstMinuteError: recoveryErr,
		ExitCode:                 report.ExitCode,
	}

	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}
