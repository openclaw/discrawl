#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git -C "$(dirname "${BASH_SOURCE[0]}")/.." rev-parse --show-toplevel)"
image="${DISCRAWL_DOCKER_IMAGE:-golang:1.27.0-bookworm@sha256:ded31c68586d2e49e760acc2e65a884b23d032e9bbbed0ae0c55abd3fcaf4452}"
tmp="$(mktemp -d /tmp/discrawl-docker-smoke.XXXXXX)"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

fixture="$tmp/backup"
mkdir -p "$fixture/tables/guilds" "$fixture/tables/channels" "$fixture/tables/members" "$fixture/tables/messages"
mkdir -p "$fixture/embeddings/openai/text-embedding-3-small/message_normalized_v1"

write_gz() {
  local rel="$1"
  local body="$2"
  printf '%s\n' "$body" | gzip -n -c > "$fixture/$rel"
}

now="2026-04-21T12:00:00Z"
write_gz "tables/guilds/000000.jsonl.gz" \
  "{\"id\":\"g1\",\"name\":\"Docker Guild\",\"icon\":null,\"raw_json\":\"{}\",\"updated_at\":\"$now\"}"
write_gz "tables/channels/000000.jsonl.gz" \
  "{\"id\":\"c1\",\"guild_id\":\"g1\",\"parent_id\":null,\"kind\":\"text\",\"name\":\"general\",\"topic\":null,\"position\":0,\"is_nsfw\":0,\"is_archived\":0,\"is_locked\":0,\"is_private_thread\":0,\"thread_parent_id\":null,\"archive_timestamp\":null,\"raw_json\":\"{}\",\"updated_at\":\"$now\"}"
write_gz "tables/members/000000.jsonl.gz" \
  "{\"guild_id\":\"g1\",\"user_id\":\"u1\",\"username\":\"peter\",\"global_name\":null,\"display_name\":\"Docker Peter\",\"nick\":null,\"discriminator\":null,\"avatar\":null,\"bot\":0,\"joined_at\":null,\"role_ids_json\":\"[]\",\"raw_json\":\"{}\",\"updated_at\":\"$now\"}"
write_gz "tables/messages/000000.jsonl.gz" \
  "{\"id\":\"m1\",\"guild_id\":\"g1\",\"channel_id\":\"c1\",\"author_id\":\"u1\",\"message_type\":0,\"created_at\":\"$now\",\"edited_at\":null,\"deleted_at\":null,\"content\":\"docker smoke archive is queryable\",\"normalized_content\":\"docker smoke archive is queryable\",\"reply_to_message_id\":null,\"pinned\":0,\"has_attachments\":0,\"raw_json\":\"{}\",\"updated_at\":\"$now\"}"
write_gz "embeddings/openai/text-embedding-3-small/message_normalized_v1/000000.jsonl.gz" \
  "{\"message_id\":\"m1\",\"provider\":\"openai\",\"model\":\"text-embedding-3-small\",\"input_version\":\"message_normalized_v1\",\"dimensions\":2,\"embedding_blob\":\"AACAPwAAAAA=\",\"embedded_at\":\"$now\"}"

cat > "$fixture/manifest.json" <<JSON
{
  "version": 1,
  "generated_at": "$now",
  "tables": [
    {"name": "guilds", "files": ["tables/guilds/000000.jsonl.gz"], "columns": ["id", "name", "icon", "raw_json", "updated_at"], "rows": 1},
    {"name": "channels", "files": ["tables/channels/000000.jsonl.gz"], "columns": ["id", "guild_id", "parent_id", "kind", "name", "topic", "position", "is_nsfw", "is_archived", "is_locked", "is_private_thread", "thread_parent_id", "archive_timestamp", "raw_json", "updated_at"], "rows": 1},
    {"name": "members", "files": ["tables/members/000000.jsonl.gz"], "columns": ["guild_id", "user_id", "username", "global_name", "display_name", "nick", "discriminator", "avatar", "bot", "joined_at", "role_ids_json", "raw_json", "updated_at"], "rows": 1},
    {"name": "messages", "files": ["tables/messages/000000.jsonl.gz"], "columns": ["id", "guild_id", "channel_id", "author_id", "message_type", "created_at", "edited_at", "deleted_at", "content", "normalized_content", "reply_to_message_id", "pinned", "has_attachments", "raw_json", "updated_at"], "rows": 1}
  ],
  "embeddings": [
    {"provider": "openai", "model": "text-embedding-3-small", "input_version": "message_normalized_v1", "files": ["embeddings/openai/text-embedding-3-small/message_normalized_v1/000000.jsonl.gz"], "columns": ["message_id", "provider", "model", "input_version", "dimensions", "embedding_blob", "embedded_at"], "rows": 1}
  ],
  "files": {"manifest": "manifest.json"}
}
JSON

git -C "$fixture" init
git -C "$fixture" checkout -B main
git -C "$fixture" config user.name "discrawl docker smoke"
git -C "$fixture" config user.email "discrawl@example.com"
git -C "$fixture" add .
git -C "$fixture" commit -m "test: fixture snapshot"

docker run --rm \
  --mount "type=bind,source=$repo_root,target=/src,readonly" \
  --mount "type=bind,source=$fixture,target=/backup,readonly" \
  "$image" \
  bash -lc '
    set -euo pipefail
    mkdir -p /work/bin /work/gocache /work/gomodcache
    export PATH=/usr/local/go/bin:$PATH
    export GOBIN=/work/bin GOCACHE=/work/gocache GOMODCACHE=/work/gomodcache
    cd /src
    go install ./cmd/discrawl
    discrawl=/work/bin/discrawl
    test "$("$discrawl" --version)" = devel
    "$discrawl" --config /work/config.toml subscribe --repo /work/share --with-embeddings file:///backup > /work/subscribe.out
    grep -q "embeddings=\\[" /work/subscribe.out
    "$discrawl" --config /work/config.toml --plain sql "select provider, model, count(*) as total from message_embeddings group by provider, model" | tee /work/embeddings.out
    grep -q "openai" /work/embeddings.out
    grep -q "text-embedding-3-small" /work/embeddings.out
    grep -Eq "(^|[^0-9])1([^0-9]|$)" /work/embeddings.out
    "$discrawl" --config /work/config.toml search "docker smoke archive" | tee /work/search.out
    grep -q "docker smoke archive is queryable" /work/search.out
    "$discrawl" --config /work/config.toml messages --channel general --all | tee /work/messages.out
    grep -q "docker smoke archive is queryable" /work/messages.out
    "$discrawl" --config /work/config.toml --plain sql "select count(*) as total from messages" | tee /work/sql.out
    grep -Eq "(^|[^0-9])1([^0-9]|$)" /work/sql.out
    "$discrawl" --config /work/config.toml report | tee /work/report.out
    grep -q "Archive size: 1 messages" /work/report.out
  '
