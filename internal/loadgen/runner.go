package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RequestResult struct {
	StatusCode   int
	Body         []byte
	NetworkError bool
	Timeout      bool
	Retries      int
	Err          string
}

type BidRequestPayload struct {
	UserIDFV    string `json:"user_idfv"`
	AppBundle   string `json:"app_bundle"`
	PlacementID string `json:"placement_id"`
	Timestamp   int64  `json:"timestamp"`
}

type BillingRequestPayload struct {
	BidID     string `json:"bid_id"`
	Timestamp int64  `json:"timestamp"`
}

type bidResponseBody struct {
	BidID string `json:"bid_id"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	maxRetries int
	timeout    time.Duration
}

func NewClient(cfg Config) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(cfg.BaseURL, "/"),
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
		maxRetries: cfg.MaxRetries,
		timeout:    cfg.HTTPTimeout,
	}
}

func (c *Client) Healthz(ctx context.Context) error {
	res := c.doRequest(ctx, http.MethodGet, c.baseURL+"/healthz", nil, "")
	if res.NetworkError {
		return fmt.Errorf("healthz network error: %s", res.Err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz status: %d", res.StatusCode)
	}
	return nil
}

func (c *Client) Bid(ctx context.Context, payload BidRequestPayload) (RequestResult, string, bool) {
	body, err := json.Marshal(payload)
	if err != nil {
		return RequestResult{NetworkError: true, Err: err.Error()}, "", false
	}

	res := c.doRequest(ctx, http.MethodPost, c.baseURL+"/v1/bid", body, "application/json")
	if res.StatusCode == http.StatusNoContent {
		return res, "", true
	}
	if res.StatusCode != http.StatusOK {
		return res, "", false
	}

	var bidResp bidResponseBody
	if err := json.Unmarshal(res.Body, &bidResp); err != nil {
		res.Err = fmt.Sprintf("decode bid response: %v", err)
		return res, "", false
	}
	return res, bidResp.BidID, false
}

func (c *Client) Billing(ctx context.Context, payload BillingRequestPayload) RequestResult {
	body, err := json.Marshal(payload)
	if err != nil {
		return RequestResult{NetworkError: true, Err: err.Error()}
	}
	return c.doRequest(ctx, http.MethodPost, c.baseURL+"/v1/billing", body, "application/json")
}

func (c *Client) RawBilling(ctx context.Context, body []byte) RequestResult {
	return c.doRequest(ctx, http.MethodPost, c.baseURL+"/v1/billing", body, "application/json")
}

func (c *Client) doRequest(ctx context.Context, method, reqURL string, body []byte, contentType string) RequestResult {
	var retries int
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, c.timeout)
		res := c.doRequestOnce(attemptCtx, method, reqURL, body, contentType)
		cancel()

		if !shouldRetry(res) || attempt == c.maxRetries {
			res.Retries = retries
			return res
		}

		retries++
		if err := sleepWithContext(ctx, backoffDuration(attempt)); err != nil {
			res.Retries = retries
			return res
		}
	}

	return RequestResult{NetworkError: true, Err: "unreachable retry loop"}
}

func (c *Client) doRequestOnce(ctx context.Context, method, reqURL string, body []byte, contentType string) RequestResult {
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, reader)
	if err != nil {
		return RequestResult{NetworkError: true, Err: err.Error()}
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return RequestResult{
			NetworkError: true,
			Timeout:      isTimeoutError(err),
			Err:          err.Error(),
		}
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return RequestResult{
			StatusCode: resp.StatusCode,
			Body:       nil,
			Err:        readErr.Error(),
		}
	}

	return RequestResult{
		StatusCode: resp.StatusCode,
		Body:       respBody,
	}
}

func shouldRetry(res RequestResult) bool {
	if res.NetworkError {
		return true
	}
	if res.StatusCode >= http.StatusInternalServerError {
		return true
	}
	return false
}

func backoffDuration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	base := 100 * time.Millisecond
	max := 2 * time.Second
	d := base * time.Duration(1<<attempt)
	if d > max {
		return max
	}
	return d
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if errors.Is(urlErr.Err, context.DeadlineExceeded) {
			return true
		}
		if nerr, ok := urlErr.Err.(net.Error); ok && nerr.Timeout() {
			return true
		}
	}
	return false
}
