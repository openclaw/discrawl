package share

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/openclaw/crawlkit/mirror"
)

const producerName = "producer.json"

var producerRevision = regexp.MustCompile(`^[0-9a-f]{40}$`)

// PublicationProducer is an unsigned publisher assertion, not authentication.
type PublicationProducer struct {
	Repository string `json:"repository"`
	Revision   string `json:"revision"`
	RunID      int64  `json:"run_id"`
	RunAttempt int64  `json:"run_attempt"`
}

type publicationReceipt struct {
	Schema         int                 `json:"schema"`
	ManifestPath   string              `json:"manifest_path"`
	ManifestSHA256 string              `json:"manifest_sha256"`
	Producer       PublicationProducer `json:"producer"`
}

func PublicationProducerFromEnv(lookup func(string) (string, bool)) (*PublicationProducer, error) {
	names := []string{
		"DISCRAWL_PRODUCER_REPOSITORY", "DISCRAWL_PRODUCER_REVISION",
		"DISCRAWL_PRODUCER_RUN_ID", "DISCRAWL_PRODUCER_RUN_ATTEMPT",
	}
	values := make([]string, len(names))
	present := 0
	for i, name := range names {
		value, ok := lookup(name)
		if ok {
			present++
		}
		if len(value) > 128 {
			return nil, fmt.Errorf("invalid %s", name)
		}
		values[i] = value
	}
	if present == 0 {
		return nil, nil
	}
	if present != len(names) {
		return nil, errors.New("publication producer requires all four DISCRAWL_PRODUCER inputs")
	}
	runID, idErr := strconv.ParseInt(values[2], 10, 64)
	attempt, attemptErr := strconv.ParseInt(values[3], 10, 64)
	if idErr != nil || attemptErr != nil ||
		strconv.FormatInt(runID, 10) != values[2] || strconv.FormatInt(attempt, 10) != values[3] {
		return nil, errors.New("publication producer run ID and attempt must be positive decimal integers")
	}
	producer := &PublicationProducer{Repository: values[0], Revision: values[1], RunID: runID, RunAttempt: attempt}
	if err := producer.validate(); err != nil {
		return nil, err
	}
	return producer, nil
}

func (p *PublicationProducer) validate() error {
	if p == nil {
		return nil
	}
	if p.Repository != "openclaw/discrawl" || !producerRevision.MatchString(p.Revision) ||
		p.RunID <= 0 || p.RunAttempt <= 0 {
		return errors.New("invalid publication producer: require openclaw/discrawl, full lowercase source SHA, and positive run ID and attempt")
	}
	return nil
}

func encodePublicationReceipt(body []byte, producer PublicationProducer) ([]byte, error) {
	if err := producer.validate(); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	receipt := publicationReceipt{
		Schema: 1, ManifestPath: ManifestName,
		ManifestSHA256: hex.EncodeToString(sum[:]), Producer: producer,
	}
	out, err := json.MarshalIndent(receipt, "", "  ")
	return append(out, '\n'), err
}

func validatePublicationReceipt(manifest, body []byte) (PublicationProducer, error) {
	var receipt publicationReceipt
	if len(body) > 4096 {
		return PublicationProducer{}, errors.New("publication receipt exceeds 4096 bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return PublicationProducer{}, errors.New("invalid publication receipt JSON")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return PublicationProducer{}, errors.New("publication receipt has trailing JSON")
	}
	if receipt.Schema != 1 || receipt.ManifestPath != ManifestName {
		return PublicationProducer{}, errors.New("unsupported publication receipt")
	}
	if err := receipt.Producer.validate(); err != nil {
		return PublicationProducer{}, err
	}
	sum := sha256.Sum256(manifest)
	if receipt.ManifestSHA256 != hex.EncodeToString(sum[:]) {
		return PublicationProducer{}, errors.New("publication receipt does not bind the exact manifest bytes")
	}
	return receipt.Producer, nil
}

func validateProducerDestination(repo string, previous map[string]bool, producer *PublicationProducer) error {
	if producer == nil || previous[producerName] {
		return nil
	}
	_, err := os.Lstat(filepath.Join(repo, producerName))
	if !errors.Is(err, os.ErrNotExist) {
		return errors.New("producer.json already exists without declared publication ownership")
	}
	return nil
}

func workingPublicationBinding(repo string, expected *PublicationProducer) ([]byte, []byte, error) {
	manifest, err := ReadManifest(repo)
	if errors.Is(err, ErrNoManifest) && expected == nil {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if _, err := publicationPaths(manifest); err != nil {
		return nil, nil, err
	}
	if manifest.Files["producer"] == "" {
		if expected != nil {
			return nil, nil, errors.New("publication producer receipt is missing")
		}
		return nil, nil, nil
	}
	for _, name := range []string{ManifestName, producerName} {
		info, err := regularFileInRoot(repo, filepath.Join(repo, name), name, "publication")
		if err != nil {
			return nil, nil, err
		}
		if name == producerName && info.Size() > 4096 {
			return nil, nil, errors.New("publication receipt exceeds 4096 bytes")
		}
	}
	manifestBody, err := os.ReadFile(filepath.Join(repo, ManifestName))
	if err != nil {
		return nil, nil, err
	}
	receiptBody, err := os.ReadFile(filepath.Join(repo, producerName))
	if err != nil {
		return nil, nil, err
	}
	producer, err := validatePublicationReceipt(manifestBody, receiptBody)
	if err != nil {
		return nil, nil, err
	}
	if expected != nil && producer != *expected {
		return nil, nil, errors.New("publication receipt does not match the requested producer")
	}
	return manifestBody, receiptBody, nil
}

// Keep the existing one-retry transport policy, but validate the exact committed
// pair after rebasing and before either push. Never restamp remote changes.
func pushBoundPublication(ctx context.Context, opts Options, manifest, receipt []byte) error {
	check := func() error {
		for name, want := range map[string][]byte{ManifestName: manifest, producerName: receipt} {
			got, err := publicationGit(ctx, opts.RepoPath, nil, "show", "HEAD:"+name)
			if err != nil {
				return err
			}
			if !bytes.Equal(got, want) {
				return errors.New("committed publication binding changed; refusing push without a new verified export")
			}
		}
		_, _, err := workingPublicationBinding(opts.RepoPath, opts.Producer)
		return err
	}
	branch := strings.TrimSpace(opts.Branch)
	if branch == "" {
		branch = "main"
	}
	refs := []string{"HEAD:refs/heads/" + branch}
	if opts.Tag != "" {
		if err := mirror.ValidateTag(ctx, mirrorOptions(opts), opts.Tag); err != nil {
			return err
		}
		refs = append(refs, "refs/tags/"+opts.Tag)
	}
	if err := check(); err != nil {
		return err
	}
	err := mirror.PushAtomic(ctx, mirrorOptions(opts), refs...)
	if err == nil {
		return nil
	}
	lower := strings.ToLower(err.Error())
	if !strings.Contains(lower, "non-fast-forward") && !strings.Contains(lower, "fetch first") &&
		!strings.Contains(lower, "failed to push some refs") {
		return err
	}
	if err := mirror.SyncForWrite(ctx, mirrorOptions(opts)); err != nil {
		return fmt.Errorf("sync before publication retry: %w", err)
	}
	if err := check(); err != nil {
		return err
	}
	return mirror.PushAtomic(ctx, mirrorOptions(opts), refs...)
}
