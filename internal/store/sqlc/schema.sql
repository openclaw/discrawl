create table guilds (
	id text primary key,
	name text not null,
	icon text,
	raw_json text not null,
	updated_at text not null
);

create table channels (
	id text primary key,
	guild_id text not null,
	parent_id text,
	kind text not null,
	name text not null,
	topic text,
	position integer,
	is_nsfw integer not null default 0,
	is_archived integer not null default 0,
	is_locked integer not null default 0,
	is_private_thread integer not null default 0,
	thread_parent_id text,
	archive_timestamp text,
	raw_json text not null,
	updated_at text not null
);

create table members (
	guild_id text not null,
	user_id text not null,
	username text not null,
	global_name text,
	display_name text,
	nick text,
	discriminator text,
	avatar text,
	bot integer not null default 0,
	joined_at text,
	role_ids_json text not null,
	raw_json text not null,
	updated_at text not null,
	primary key (guild_id, user_id)
);

create table messages (
	id text primary key,
	guild_id text not null,
	channel_id text not null,
	author_id text,
	message_type integer not null,
	created_at text not null,
	edited_at text,
	deleted_at text,
	content text not null,
	normalized_content text not null,
	reply_to_message_id text,
	pinned integer not null default 0,
	has_attachments integer not null default 0,
	raw_json text not null,
	updated_at text not null
);

create table message_events (
	event_id integer primary key autoincrement,
	guild_id text not null,
	channel_id text not null,
	message_id text not null,
	event_type text not null,
	event_at text not null,
	payload_json text not null
);

create table message_attachments (
	attachment_id text primary key,
	message_id text not null,
	guild_id text not null,
	channel_id text not null,
	author_id text,
	filename text not null,
	content_type text,
	size integer not null default 0,
	url text,
	proxy_url text,
	text_content text not null default '',
	media_path text,
	content_sha256 text,
	content_size integer not null default 0,
	fetched_at text,
	fetch_status text not null default '',
	fetch_error text not null default '',
	updated_at text not null
);

create table mention_events (
	event_id integer primary key autoincrement,
	message_id text not null,
	guild_id text not null,
	channel_id text not null,
	author_id text,
	target_type text not null,
	target_id text not null,
	target_name text not null default '',
	event_at text not null
);

create table sync_state (
	scope text primary key,
	cursor text,
	updated_at text not null
);

create table embedding_jobs (
	message_id text primary key,
	state text not null,
	attempts integer not null default 0,
	provider text not null default '',
	model text not null default '',
	input_version text not null default '',
	last_error text not null default '',
	locked_at text,
	updated_at text not null
);

create table message_embeddings (
	message_id text not null,
	provider text not null,
	model text not null,
	input_version text not null,
	dimensions integer not null,
	embedding_blob blob not null,
	embedded_at text not null,
	primary key (message_id, provider, model, input_version)
);

create table failure_ledger (
	failure_id integer primary key autoincrement,
	operation text not null,
	source text not null,
	guild_id text not null default '',
	channel_id text not null default '',
	message_id text not null default '',
	related_kind text not null default '',
	related_id text not null default '',
	error_class text not null,
	error_message text not null,
	first_seen_at text not null,
	last_seen_at text not null,
	retry_count integer not null default 0,
	resolved_at text,
	unique(operation, source, guild_id, channel_id, message_id, related_kind, related_id)
);

-- sqlc only needs parseable table shapes. Runtime migrations create real FTS5
-- virtual tables and maintain rowids.
create table message_fts (
	rowid integer primary key,
	message_id text,
	guild_id text,
	channel_id text,
	author_id text,
	author_name text,
	channel_name text,
	content text
);

create table member_fts (
	rowid integer primary key,
	member_key text,
	guild_id text,
	user_id text,
	username text,
	display_name text,
	profile_text text
);
