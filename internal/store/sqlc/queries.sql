-- name: GetSyncState :one
select coalesce(cursor, '') as cursor
from sync_state
where scope = ?;

-- name: SetSyncState :exec
insert into sync_state(scope, cursor, updated_at)
values(?, ?, ?)
on conflict(scope) do update set
	cursor = excluded.cursor,
	updated_at = excluded.updated_at;

-- name: DeleteSyncState :exec
delete from sync_state
where scope = ?;

-- name: ChannelMessageBounds :one
select cast(coalesce(min(id), '') as text) as oldest_id,
       cast(coalesce(max(id), '') as text) as newest_id
from messages
where channel_id = ?;

-- name: UpdateAttachmentMedia :exec
update message_attachments
set media_path = ?,
    content_sha256 = ?,
    content_size = ?,
    fetched_at = ?,
    fetch_status = ?,
    fetch_error = ?,
    updated_at = ?
where attachment_id = ?;

-- name: UpdateAttachmentFetchStatus :exec
update message_attachments
set fetched_at = ?,
    fetch_status = ?,
    fetch_error = ?,
    updated_at = ?
where attachment_id = ?;

-- name: CountEmbeddingBacklog :one
select count(*) as count
from embedding_jobs j
join messages m on m.id = j.message_id
where j.state = 'pending'
  and m.deleted_at is null;

-- name: HasMessageEmbeddings :one
select exists(
	select 1
	from message_embeddings
	where provider = ?
	  and model = ?
	  and input_version = ?
) as present;

-- name: CountGuilds :one
select count(*) as count from guilds;

-- name: CountChannels :one
select count(*) as count from channels;

-- name: CountMessages :one
select count(*) as count from messages;

-- name: CountMembers :one
select count(*) as count from members;

-- name: CountThreads :one
select count(*) as count
from channels
where kind like 'thread_%';

-- name: GetSyncUpdatedAt :one
select coalesce(updated_at, '') as updated_at
from sync_state
where scope = ?;

-- name: GetGuildName :one
select name
from guilds
where id = ?;

-- name: ListGuildIDs :many
select id
from guilds
order by id;

-- name: CountChannelsByGuild :one
select count(*) as count
from channels
where guild_id = ?;

-- name: CountMembersByGuild :one
select count(*) as count
from members
where guild_id = ?;

-- name: ListMembers :many
select guild_id, user_id, username, coalesce(global_name, '') as global_name,
       coalesce(display_name, '') as display_name, coalesce(nick, '') as nick,
       coalesce(discriminator, '') as discriminator, coalesce(avatar, '') as avatar,
       role_ids_json, bot, coalesce(joined_at, '') as joined_at, raw_json
from members
order by coalesce(nullif(display_name, ''), nullif(nick, ''), nullif(global_name, ''), username), username
limit ?;

-- name: ListMembersByGuild :many
select guild_id, user_id, username, coalesce(global_name, '') as global_name,
       coalesce(display_name, '') as display_name, coalesce(nick, '') as nick,
       coalesce(discriminator, '') as discriminator, coalesce(avatar, '') as avatar,
       role_ids_json, bot, coalesce(joined_at, '') as joined_at, raw_json
from members
where guild_id = ?
order by coalesce(nullif(display_name, ''), nullif(nick, ''), nullif(global_name, ''), username), username
limit ?;

-- name: ListMembersByUserID :many
select guild_id, user_id, username, coalesce(global_name, '') as global_name,
       coalesce(display_name, '') as display_name, coalesce(nick, '') as nick,
       coalesce(discriminator, '') as discriminator, coalesce(avatar, '') as avatar,
       role_ids_json, bot, coalesce(joined_at, '') as joined_at, raw_json
from members
where user_id = ?
order by guild_id, username;

-- name: MemberMessageStats :one
select count(*) as message_count,
       cast(coalesce(min(created_at), '') as text) as first_message_at,
       cast(coalesce(max(created_at), '') as text) as last_message_at
from messages
where guild_id = ? and author_id = ?;

-- name: ListRecentMemberMessages :many
select
	m.id as message_id,
	m.guild_id,
	m.channel_id,
	coalesce(c.name, '') as channel_name,
	coalesce(m.author_id, '') as author_id,
	cast(coalesce(
		nullif(mem.display_name, ''),
		nullif(mem.nick, ''),
		nullif(mem.global_name, ''),
		nullif(mem.username, ''),
		nullif(json_extract(m.raw_json, '$.author.global_name'), ''),
		nullif(json_extract(m.raw_json, '$.author.username'), ''),
		''
	) as text) as author_name,
	cast(case
		when trim(coalesce(m.content, '')) <> '' then m.content
		else m.normalized_content
	end as text) as content,
	m.created_at,
	coalesce(m.reply_to_message_id, '') as reply_to_message,
	m.has_attachments,
	m.pinned
from messages m
left join channels c on c.id = m.channel_id
left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
where m.guild_id = ? and m.author_id = ?
order by m.created_at desc, m.id desc
limit ?;

-- name: ListChannels :many
select id, guild_id, coalesce(parent_id, '') as parent_id, kind, name,
       coalesce(topic, '') as topic, position, is_nsfw, is_archived,
       is_locked, is_private_thread, coalesce(thread_parent_id, '') as thread_parent_id,
       coalesce(archive_timestamp, '') as archive_timestamp
from channels
order by guild_id, position, name;

-- name: ListChannelsByGuild :many
select id, guild_id, coalesce(parent_id, '') as parent_id, kind, name,
       coalesce(topic, '') as topic, position, is_nsfw, is_archived,
       is_locked, is_private_thread, coalesce(thread_parent_id, '') as thread_parent_id,
       coalesce(archive_timestamp, '') as archive_timestamp
from channels
where guild_id = ?
order by guild_id, position, name;

-- name: ListIncompleteMessageChannelIDs :many
select c.id
from channels c
where c.kind in ('text', 'news', 'announcement', 'thread_public', 'thread_private', 'thread_news', 'thread_announcement')
  and not exists (
	select 1
	from sync_state s
	where s.scope = 'channel:' || c.id || ':history_complete'
  )
  and not exists (
	select 1
	from sync_state s
	where s.scope = 'channel:' || c.id || ':unavailable'
  )
order by c.id;

-- name: ListIncompleteMessageChannelIDsByGuild :many
select c.id
from channels c
where c.kind in ('text', 'news', 'announcement', 'thread_public', 'thread_private', 'thread_news', 'thread_announcement')
  and c.guild_id = ?
  and not exists (
	select 1
	from sync_state s
	where s.scope = 'channel:' || c.id || ':history_complete'
  )
  and not exists (
	select 1
	from sync_state s
	where s.scope = 'channel:' || c.id || ':unavailable'
  )
order by c.id;

-- name: UpsertGuild :exec
insert into guilds(id, name, icon, raw_json, updated_at)
values(?, ?, ?, ?, ?)
on conflict(id) do update set
	name = excluded.name,
	icon = excluded.icon,
	raw_json = excluded.raw_json,
	updated_at = excluded.updated_at;

-- name: UpsertChannel :exec
insert into channels(
	id, guild_id, parent_id, kind, name, topic, position, is_nsfw,
	is_archived, is_locked, is_private_thread, thread_parent_id,
	archive_timestamp, raw_json, updated_at
) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(id) do update set
	guild_id = excluded.guild_id,
	parent_id = excluded.parent_id,
	kind = excluded.kind,
	name = excluded.name,
	topic = excluded.topic,
	position = excluded.position,
	is_nsfw = excluded.is_nsfw,
	is_archived = excluded.is_archived,
	is_locked = excluded.is_locked,
	is_private_thread = excluded.is_private_thread,
	thread_parent_id = excluded.thread_parent_id,
	archive_timestamp = excluded.archive_timestamp,
	raw_json = excluded.raw_json,
	updated_at = excluded.updated_at;

-- name: DeleteMembersByGuild :exec
delete from members
where guild_id = ?;

-- name: InsertMember :exec
insert into members(
	guild_id, user_id, username, global_name, display_name, nick, discriminator,
	avatar, bot, joined_at, role_ids_json, raw_json, updated_at
) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: UpsertMember :exec
insert into members(
	guild_id, user_id, username, global_name, display_name, nick, discriminator,
	avatar, bot, joined_at, role_ids_json, raw_json, updated_at
) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(guild_id, user_id) do update set
	username = excluded.username,
	global_name = excluded.global_name,
	display_name = excluded.display_name,
	nick = excluded.nick,
	discriminator = excluded.discriminator,
	avatar = excluded.avatar,
	bot = excluded.bot,
	joined_at = excluded.joined_at,
	role_ids_json = excluded.role_ids_json,
	raw_json = excluded.raw_json,
	updated_at = excluded.updated_at;

-- name: DeleteMember :exec
delete from members
where guild_id = ? and user_id = ?;

-- name: DeleteOrphanChannels :exec
delete from channels
where channels.guild_id = ?
  and not exists (
	select 1
	from messages
	where messages.channel_id = channels.id
  );

-- name: DeleteEmbeddingJobsByGuild :exec
delete from embedding_jobs
where message_id in (select id from messages where guild_id = ?);

-- name: DeleteMessageEmbeddingsByGuild :exec
delete from message_embeddings
where message_id in (select id from messages where guild_id = ?);

-- name: DeleteMessageEventsByGuild :exec
delete from message_events
where guild_id = ?;

-- name: DeleteAttachmentsByGuild :exec
delete from message_attachments
where guild_id = ?;

-- name: DeleteMentionEventsByGuild :exec
delete from mention_events
where guild_id = ?;

-- name: DeleteMessagesByGuild :exec
delete from messages
where guild_id = ?;

-- name: DeleteChannelsByGuild :exec
delete from channels
where guild_id = ?;

-- name: DeleteGuild :exec
delete from guilds
where id = ?;

-- name: GetMessageNormalizedContent :one
select normalized_content
from messages
where id = ?;

-- name: CountEmbeddingJobsByMessage :one
select count(*) as count
from embedding_jobs
where message_id = ?;

-- name: UpsertMessage :exec
insert into messages(
	id, guild_id, channel_id, author_id, message_type, created_at, edited_at, deleted_at,
	content, normalized_content, reply_to_message_id, pinned, has_attachments, raw_json, updated_at
) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(id) do update set
	guild_id = excluded.guild_id,
	channel_id = excluded.channel_id,
	author_id = excluded.author_id,
	message_type = excluded.message_type,
	created_at = excluded.created_at,
	edited_at = excluded.edited_at,
	deleted_at = coalesce(excluded.deleted_at, messages.deleted_at),
	content = excluded.content,
	normalized_content = excluded.normalized_content,
	reply_to_message_id = excluded.reply_to_message_id,
	pinned = excluded.pinned,
	has_attachments = excluded.has_attachments,
	raw_json = excluded.raw_json,
	updated_at = excluded.updated_at;

-- name: UpsertEmbeddingJobPending :exec
insert into embedding_jobs(message_id, state, attempts, updated_at)
values(?, 'pending', 0, ?)
on conflict(message_id) do update set
	state = 'pending',
	attempts = 0,
	last_error = '',
	locked_at = null,
	updated_at = excluded.updated_at;

-- name: MarkMessageDeleted :exec
update messages
set deleted_at = ?, updated_at = ?
where id = ?;

-- name: InsertMessageEvent :exec
insert into message_events(guild_id, channel_id, message_id, event_type, event_at, payload_json)
values(?, ?, ?, ?, ?, ?);

-- name: ListExistingAttachmentMedia :many
select attachment_id, coalesce(media_path, '') as media_path,
       coalesce(content_sha256, '') as content_sha256, content_size,
       coalesce(fetched_at, '') as fetched_at, coalesce(fetch_status, '') as fetch_status,
       coalesce(fetch_error, '') as fetch_error
from message_attachments
where message_id = ?;

-- name: DeleteAttachmentsByMessage :exec
delete from message_attachments
where message_id = ?;

-- name: InsertMessageAttachment :exec
insert into message_attachments(
	attachment_id, message_id, guild_id, channel_id, author_id, filename,
	content_type, size, url, proxy_url, text_content, media_path, content_sha256,
	content_size, fetched_at, fetch_status, fetch_error, updated_at
) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
on conflict(attachment_id) do update set
	message_id = excluded.message_id,
	guild_id = excluded.guild_id,
	channel_id = excluded.channel_id,
	author_id = excluded.author_id,
	filename = excluded.filename,
	content_type = excluded.content_type,
	size = excluded.size,
	url = excluded.url,
	proxy_url = excluded.proxy_url,
	text_content = excluded.text_content,
	media_path = coalesce(excluded.media_path, message_attachments.media_path),
	content_sha256 = coalesce(excluded.content_sha256, message_attachments.content_sha256),
	content_size = case
		when excluded.content_size > 0 then excluded.content_size
		else message_attachments.content_size
	end,
	fetched_at = coalesce(excluded.fetched_at, message_attachments.fetched_at),
	fetch_status = case
		when excluded.fetch_status <> '' then excluded.fetch_status
		else message_attachments.fetch_status
	end,
	fetch_error = case
		when excluded.fetch_status <> '' or excluded.fetch_error <> '' then excluded.fetch_error
		else message_attachments.fetch_error
	end,
	updated_at = excluded.updated_at;

-- name: DeleteMentionEventsByMessage :exec
delete from mention_events
where message_id = ?;

-- name: InsertMentionEvent :exec
insert into mention_events(
	message_id, guild_id, channel_id, author_id, target_type, target_id, target_name, event_at
) values(?, ?, ?, ?, ?, ?, ?, ?);

-- name: InsertMissingEmbeddingJobs :exec
insert or ignore into embedding_jobs(
	message_id, state, attempts, provider, model, input_version, last_error, locked_at, updated_at
)
select id, 'pending', 0, ?, ?, ?, '', null, ?
from messages
where deleted_at is null;

-- name: RequeueAllEmbeddingJobs :execrows
update embedding_jobs
set state = 'pending',
	attempts = 0,
	provider = ?,
	model = ?,
	input_version = ?,
	last_error = '',
	locked_at = null,
	updated_at = ?
where message_id in (select id from messages where deleted_at is null);

-- name: ListPendingEmbeddingJobs :many
select
	j.message_id,
	m.normalized_content,
	j.attempts,
	j.provider,
	j.model,
	j.input_version
from embedding_jobs j
join messages m on m.id = j.message_id
where j.state = 'pending'
  and m.deleted_at is null
  and (j.locked_at is null or j.locked_at = '' or j.locked_at < ?)
order by j.updated_at, j.message_id
limit ?;

-- name: ResetEmbeddingJobIdentity :exec
update embedding_jobs
set provider = ?,
	model = ?,
	input_version = ?,
	last_error = '',
	locked_at = null,
	updated_at = ?
where message_id = ?;

-- name: ResetEmbeddingJobIdentityAndAttempts :exec
update embedding_jobs
set provider = ?,
	model = ?,
	input_version = ?,
	attempts = 0,
	last_error = '',
	locked_at = null,
	updated_at = ?
where message_id = ?;

-- name: LockEmbeddingJob :execrows
update embedding_jobs
set locked_at = sqlc.arg(locked_at), updated_at = sqlc.arg(updated_at)
where message_id = sqlc.arg(message_id)
  and state = 'pending'
  and (locked_at is null or locked_at = '' or locked_at < sqlc.arg(stale_before));

-- name: UpsertMessageEmbedding :exec
insert into message_embeddings(
	message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
) values(?, ?, ?, ?, ?, ?, ?)
on conflict(message_id, provider, model, input_version) do update set
	dimensions = excluded.dimensions,
	embedding_blob = excluded.embedding_blob,
	embedded_at = excluded.embedded_at;

-- name: DeleteMessageEmbeddingsByMessage :exec
delete from message_embeddings
where message_id = ?;

-- name: MarkEmbeddingJobDone :exec
update embedding_jobs
set state = 'done',
	attempts = 0,
	provider = ?,
	model = ?,
	input_version = ?,
	last_error = '',
	locked_at = null,
	updated_at = ?
where message_id = ?;

-- name: MarkEmptyEmbeddingJobDone :exec
update embedding_jobs
set state = 'done',
	provider = ?,
	model = ?,
	input_version = ?,
	last_error = '',
	locked_at = null,
	updated_at = ?
where message_id = ?;

-- name: MarkEmbeddingJobRateLimited :exec
update embedding_jobs
set state = 'pending',
	provider = ?,
	model = ?,
	input_version = ?,
	last_error = ?,
	locked_at = null,
	updated_at = ?
where message_id = ?;

-- name: MarkEmbeddingJobFailed :exec
update embedding_jobs
set state = ?,
	attempts = ?,
	provider = ?,
	model = ?,
	input_version = ?,
	last_error = ?,
	locked_at = null,
	updated_at = ?
where message_id = ?;
