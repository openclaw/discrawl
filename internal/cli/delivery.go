package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/crawlkit/remote"
	"github.com/openclaw/discrawl/internal/remotedelivery"
	"github.com/openclaw/discrawl/internal/store"
)

const deliveryUsage = `Usage:
  discrawl delivery prepare-seed --source-db PATH --output DIR --guilds ID[,ID...] [--timeout 30m]
  discrawl delivery init --outbox PATH --registration FILE --ownership FILE [--timeout 5m]
  discrawl delivery capture --outbox PATH --source-db PATH [--timeout 30s] [--max-events 10000] [--max-bytes 8388608]
  discrawl delivery drain --outbox PATH --token-env NAME [--timeout 30s] [--max-parts 64]

Use explicit absolute database paths. Init requires an externally qualified
registration and a sorted JSON-lines ownership stream; it never replaces a file.
Capture reads the native database without migrating it. Drain sends only the
persisted pending capture; repeat it after a lost response. Destination and scope
come from the immutable registration. No collection or schedule is started.
See docs/commands/delivery.md for input formats and qualification requirements.
`

type deliveryRegistration struct {
	Format       string                      `json:"format"`
	Registration remotedelivery.Registration `json:"registration"`
}

type deliveryOptions struct {
	outboxPath, registrationPath, ownershipPath, sourcePath, tokenEnv string
	outputPath, guilds                                                string
	maxEvents, maxParts                                               int
	maxBytes                                                          int64
}

type deliveryResult struct {
	Status              string `json:"status"`
	CaptureID           string `json:"capture_id,omitempty"`
	NativeSequence      string `json:"native_sequence,omitempty"`
	ReceiptsRecorded    int    `json:"receipts_recorded,omitempty"`
	ManifestSHA256      string `json:"manifest_sha256,omitempty"`
	QualificationSHA256 string `json:"qualification_sha256,omitempty"`
	OwnedKeysCount      int64  `json:"owned_keys_count,omitempty"`
}

func runDelivery(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		_, err := io.WriteString(stdout, deliveryUsage)
		return err
	}
	op := args[0]
	if op != "prepare-seed" && op != "init" && op != "capture" && op != "drain" {
		return usageErr(errors.New("delivery requires prepare-seed, init, capture or drain"))
	}
	fs := flag.NewFlagSet("delivery "+op, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var options deliveryOptions
	if op != "prepare-seed" {
		fs.StringVar(&options.outboxPath, "outbox", "", "")
	}
	timeoutDefault := 30 * time.Second
	if op == "init" {
		timeoutDefault = 5 * time.Minute
	}
	if op == "prepare-seed" {
		timeoutDefault = 30 * time.Minute
	}
	timeout := fs.Duration("timeout", timeoutDefault, "")
	switch op {
	case "prepare-seed":
		fs.StringVar(&options.sourcePath, "source-db", "", "")
		fs.StringVar(&options.outputPath, "output", "", "")
		fs.StringVar(&options.guilds, "guilds", "", "")
	case "init":
		fs.StringVar(&options.registrationPath, "registration", "", "")
		fs.StringVar(&options.ownershipPath, "ownership", "", "")
	case "capture":
		fs.StringVar(&options.sourcePath, "source-db", "", "")
		fs.IntVar(&options.maxEvents, "max-events", 10000, "")
		fs.Int64Var(&options.maxBytes, "max-bytes", 8<<20, "")
	case "drain":
		fs.StringVar(&options.tokenEnv, "token-env", "", "")
		fs.IntVar(&options.maxParts, "max-parts", remote.MaxChangeParts, "")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return usageErr(errors.New("invalid delivery flags; see discrawl delivery --help"))
	}
	if fs.NArg() != 0 || (op != "prepare-seed" && options.outboxPath == "") || *timeout <= 0 {
		return usageErr(errors.New("delivery requires explicit paths, positive timeout and flags only"))
	}
	if (op == "prepare-seed" && (options.sourcePath == "" || options.outputPath == "" || options.guilds == "")) ||
		(op == "init" && (options.registrationPath == "" || options.ownershipPath == "")) ||
		(op == "capture" && (options.sourcePath == "" || options.maxEvents <= 0 || options.maxBytes <= 0)) ||
		(op == "drain" && (options.tokenEnv == "" || options.maxParts < 1 || options.maxParts > remote.MaxChangeParts)) {
		return usageErr(errors.New("missing or invalid delivery arguments; see discrawl delivery --help"))
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	result, err := executeDelivery(ctx, op, options)
	// A failed drain can have durable remote/local effects. Report the bounded
	// partial result before returning the error; never imply rollback or retry.
	if result.Status != "" {
		if writeErr := json.NewEncoder(stdout).Encode(result); writeErr != nil {
			return writeErr
		}
	}
	if err != nil {
		// SQLite, filesystem and HTTP errors may contain paths or private values.
		safe := errors.New("delivery " + op + " failed; preserve output and reconcile before resuming")
		if ctx.Err() != nil {
			return errors.Join(safe, ctx.Err())
		}
		return safe
	}
	return nil
}

func executeDelivery(ctx context.Context, op string, options deliveryOptions) (deliveryResult, error) {
	if op == "prepare-seed" {
		prepared, qualification, err := remotedelivery.PrepareSeed(ctx, options.sourcePath, options.outputPath,
			remotedelivery.Scope{GuildIDs: strings.Split(options.guilds, ",")})
		if err != nil {
			return deliveryResult{}, err
		}
		return deliveryResult{
			Status: "prepared", NativeSequence: strconv.FormatInt(prepared.Baseline.Cursor.Sequence, 10),
			ManifestSHA256: prepared.SeedManifestSHA256, QualificationSHA256: qualification, OwnedKeysCount: prepared.OwnedKeysCount,
		}, nil
	}
	if op == "init" {
		return initializeDelivery(ctx, options.outboxPath, options.registrationPath, options.ownershipPath)
	}
	// Read the token before opening the outbox: missing credentials cannot
	// trigger local writes or an unauthenticated request.
	var token string
	if op == "drain" {
		token = os.Getenv(options.tokenEnv)
		if token == "" {
			return deliveryResult{}, errors.New("missing publisher credential")
		}
	}
	outbox, err := remotedelivery.OpenOutbox(ctx, options.outboxPath)
	if err != nil {
		return deliveryResult{}, err
	}
	defer outbox.Close()
	if op == "drain" {
		deadline, _ := ctx.Deadline() // runDelivery always supplies a deadline.
		result, err := outbox.DeliverPending(ctx, remotedelivery.DeliveryOptions{TokenProvider: remote.StaticToken(token)}, remotedelivery.DeliveryLimits{MaxParts: options.maxParts, MaxDuration: time.Until(deadline)})
		return deliveryResult{Status: string(result.Status), CaptureID: result.CaptureID, ReceiptsRecorded: result.ReceiptsRecorded}, err
	}
	state, err := outbox.State(ctx)
	if err != nil {
		return deliveryResult{}, err
	}
	if state.PendingID != "" {
		return deliveryResult{Status: "pending", CaptureID: state.PendingID}, remotedelivery.ErrPendingCapture
	}
	capture, err := store.CaptureNativeChanges(ctx, options.sourcePath, state.Baseline, store.NativeCaptureLimits{MaxEvents: options.maxEvents, MaxBytes: options.maxBytes})
	if err != nil {
		return deliveryResult{}, err
	}
	result := deliveryResult{Status: "idle", NativeSequence: strconv.FormatInt(capture.Through.Cursor.Sequence, 10)}
	if reflect.DeepEqual(capture.From, capture.Through) {
		return result, nil
	}
	result.CaptureID, err = outbox.Stage(ctx, capture, options.maxBytes)
	if err == nil {
		result.Status = "staged"
	} else {
		result.Status = ""
	}
	return result, err
}

func initializeDelivery(ctx context.Context, path, registrationPath, ownershipPath string) (deliveryResult, error) {
	f, err := openDeliveryInput(registrationPath)
	if err != nil {
		return deliveryResult{}, err
	}
	body, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	closeErr := f.Close()
	if err != nil || closeErr != nil || len(body) > 1<<20 {
		return deliveryResult{}, errors.New("invalid registration file")
	}
	var input deliveryRegistration
	if err := decodeDeliveryJSON(body, &input); err != nil || input.Format != "discrawl-delivery-registration/v1" {
		return deliveryResult{}, errors.New("invalid registration document")
	}
	keys, err := openDeliveryInput(ownershipPath)
	if err != nil {
		return deliveryResult{}, err
	}
	defer keys.Close()
	scanner := bufio.NewScanner(keys)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	outbox, err := remotedelivery.InitOutbox(ctx, path, input.Registration, func(yield func(remotedelivery.SeedKey) error) error {
		var count int64
		for scanner.Scan() {
			if err := ctx.Err(); err != nil {
				return err
			}
			count++
			if count > input.Registration.OwnedKeysCount {
				return remotedelivery.ErrOwnership
			}
			var key remotedelivery.SeedKey
			if err := decodeDeliveryJSON(scanner.Bytes(), &key); err != nil {
				return err
			}
			if err := yield(key); err != nil {
				return err
			}
		}
		return scanner.Err()
	})
	if err != nil {
		return deliveryResult{}, err
	}
	if err := outbox.Close(); err != nil {
		return deliveryResult{}, err
	}
	return deliveryResult{Status: "initialized", NativeSequence: strconv.FormatInt(input.Registration.Baseline.Cursor.Sequence, 10)}, nil
}

func decodeDeliveryJSON(body []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}

// Only local regular files are supported; a FIFO must not block an operator
// command beyond its deadline while waiting for an unowned producer.
func openDeliveryInput(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("delivery input must be a regular file")
	}
	return os.Open(path)
}
