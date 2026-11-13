package remotedelivery

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/openclaw/crawlkit/remote"
)

var (
	ErrDeliveryOptions = errors.New("delivery requires authentication and positive time and part limits")
	ErrDeliveryState   = errors.New("delivery outbox operation failed; reconcile the retained capture")
	ErrDeliveryClient  = errors.New("delivery client configuration failed")
	ErrDeliverySend    = errors.New("delivery request or receipt failed; resume the exact persisted part")
)

// DeliveryOptions cannot override the destination held by the registration.
// The caller owns these in-memory adapters and their cancellation behavior.
type DeliveryOptions struct {
	HTTPClient    *http.Client
	TokenProvider remote.TokenProvider
}

type DeliveryLimits struct {
	MaxParts    int
	MaxDuration time.Duration
}

type DeliveryStatus string

const (
	DeliveryIdle      DeliveryStatus = "idle"
	DeliveryPending   DeliveryStatus = "pending"
	DeliveryComplete  DeliveryStatus = "complete"
	DeliveryPartLimit DeliveryStatus = "part_limit"
)

type DeliveryResult struct {
	CaptureID string
	// This counts successful local receipt records, not distinct remote effects.
	// Another dispatcher may have recorded the same receipt concurrently.
	ReceiptsRecorded int
	Status           DeliveryStatus
}

// DeliverPending sends only the capture pending at entry. No source capture,
// implicit retry, successor delivery or database transaction spans HTTP.
func (o *Outbox) DeliverPending(ctx context.Context, options DeliveryOptions, limits DeliveryLimits) (DeliveryResult, error) {
	var result DeliveryResult
	if options.TokenProvider == nil || limits.MaxParts < 1 || limits.MaxParts > remote.MaxChangeParts || limits.MaxDuration <= 0 {
		return result, ErrDeliveryOptions
	}
	ctx, cancel := context.WithTimeout(ctx, limits.MaxDuration)
	defer cancel()
	state, err := o.State(ctx)
	if err != nil {
		return result, deliveryError(ctx, err, ErrDeliveryState)
	}
	if state.PendingID == "" {
		result.Status = DeliveryIdle
		return result, nil
	}
	result.CaptureID, result.Status = state.PendingID, DeliveryPending
	r := state.Registration
	client, err := remote.NewClient(remote.Options{Endpoint: r.Origin, HTTPClient: options.HTTPClient, TokenProvider: options.TokenProvider})
	if err != nil {
		return result, deliveryError(ctx, err, ErrDeliveryClient)
	}
	for result.ReceiptsRecorded < limits.MaxParts {
		part, err := o.NextPart(ctx)
		if err != nil {
			return result, deliveryError(ctx, err, ErrDeliveryState)
		}
		if part == nil || part.Ref.CaptureID != result.CaptureID {
			result.Status = DeliveryComplete
			return result, nil
		}
		receipt, err := client.ApplyChanges(ctx, "discrawl", r.Archive, part.Body)
		if err != nil {
			return result, deliveryError(ctx, err, ErrDeliverySend)
		}
		if err := o.RecordReceipt(ctx, part.Ref, receipt); err != nil {
			return result, deliveryError(ctx, err, ErrDeliveryState)
		}
		result.ReceiptsRecorded++
	}
	state, err = o.State(ctx)
	if err != nil {
		return result, deliveryError(ctx, err, ErrDeliveryState)
	}
	result.Status = DeliveryPartLimit
	if state.PendingID != result.CaptureID {
		result.Status = DeliveryComplete
	}
	return result, nil
}

// Transport and SQLite errors can contain URLs, credentials or source data.
// Expose a stable operation error while retaining useful cancellation identity.
func deliveryError(ctx context.Context, cause, safe error) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(safe, err)
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(cause, err) {
			return errors.Join(safe, err)
		}
	}
	return safe
}
