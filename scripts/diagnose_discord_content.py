#!/usr/bin/env python3
"""One admitted hosted measurement; never persist or render Discord payloads."""

from datetime import datetime, timezone
import http.client
import json
import os
import re
import signal
import sys

GUILD = "1456350064065904867"
CHANNEL = "1456744319972282449"
MESSAGE = "1564461144779784244"
APPLICATION_PATH = "/api/v10/applications/@me"
MESSAGE_PATH = f"/api/v10/channels/{CHANNEL}/messages/{MESSAGE}"
WORKFLOW = "openclaw/discrawl/.github/workflows/diagnose-discord-content.yml@refs/heads/main"
MAX_BODY = 256 * 1024


class DiagnosticError(Exception):
    pass


def require(value, code):
    if not value:
        raise DiagnosticError(code)


def snowflake(value):
    return isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{16,19}", value) is not None and int(value) < 2**64


def admit(env):
    expected = env.get("EXPECTED_SHA", "")
    require(env.get("GITHUB_REPOSITORY") == "openclaw/discrawl"
            and env.get("GITHUB_WORKFLOW_REF") == WORKFLOW
            and env.get("GITHUB_REF") == "refs/heads/main"
            and env.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and env.get("GITHUB_RUN_ATTEMPT") == "1"
            and env.get("GITHUB_ACTOR") == "vincentkoc"
            and env.get("GITHUB_TRIGGERING_ACTOR") == "vincentkoc"
            and re.fullmatch(r"[0-9a-f]{40}", expected) is not None
            and expected == env.get("GITHUB_SHA") == env.get("CHECKED_OUT_SHA"), "admission")
    require(env.get("ORDINARY_TEXT_CONFIRMED") == "true", "ordinary_confirmation")
    for key in ("EXPECTED_APPLICATION_ID", "EXPECTED_BOT_ID", "EXPECTED_AUTHOR_ID"):
        require(snowflake(env.get(key)), "expected_identity")
    require(env["EXPECTED_AUTHOR_ID"] != env["EXPECTED_BOT_ID"], "expected_identity")
    token = env.get("DISCORD_BOT_TOKEN", "")
    require(token and token.isascii() and not any(c.isspace() or ord(c) < 33 or ord(c) > 126 for c in token), "credential")


def flags(application):
    # Discord's full flags_new is a decimal STRING; legacy flags stops at bit 30.
    legacy = application.get("flags")
    full = application.get("flags_new")
    if "flags" in application:
        require(type(legacy) is int and 0 <= legacy < 2**31, "flags_unverified")
    if "flags_new" in application:
        require(isinstance(full, str) and re.fullmatch(r"0|[1-9][0-9]{0,127}", full) is not None,
                "flags_unverified")
        value = int(full)
        require("flags" not in application or value & (2**31 - 1) == legacy, "flags_inconsistent")
    else:
        require("flags" in application, "flags_unverified")
        value = legacy
    return {"message_content": bool(value & (1 << 18)),
            "message_content_limited": bool(value & (1 << 19))}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "invalid_response")
        result[key] = value
    return result


def get_json(path, token, report):
    # Fixed TLS origin, no proxies, redirects, retries, cookies or generic URL input.
    require(path in (APPLICATION_PATH, MESSAGE_PATH), "endpoint")
    connection = http.client.HTTPSConnection("discord.com", timeout=10)
    try:
        connection.request("GET", path, headers={"Authorization": "Bot " + token,
                           "Accept": "application/json", "User-Agent": "discrawl-read-only-diagnostic"})
        response = connection.getresponse()
        status = response.status
        require(type(status) is int and 100 <= status <= 599, "invalid_response")
        report["application_http_status" if path == APPLICATION_PATH else "message_http_status"] = status
        # Do not read private error bodies or follow Location/Retry-After.
        require(status not in (401, 403), "permission")
        require(status != 429, "rate_limited")
        require(not 300 <= status <= 399, "redirect")
        require(status == 200, "http_status")
        require(response.getheader("Content-Encoding", "identity") == "identity", "invalid_response")
        body = response.read(MAX_BODY + 1)
        require(len(body) <= MAX_BODY, "response_too_large")
        value = json.loads(body, object_pairs_hook=unique_object,
                           parse_constant=lambda _value: require(False, "invalid_response"))
        require(isinstance(value, dict), "invalid_response")
        return value
    finally:
        connection.close()


def validate_message(message, env):
    require(message.get("id") == MESSAGE and message.get("channel_id") == CHANNEL
            and message.get("guild_id", GUILD) == GUILD, "message_identity")
    author = message.get("author")
    require(isinstance(author, dict) and author.get("id") == env["EXPECTED_AUTHOR_ID"]
            and author.get("bot", False) is False and author.get("system", False) is False,
            "message_identity")
    require(type(message.get("type")) is int and message["type"] == 0
            and not any(key in message for key in ("webhook_id", "application_id", "application",
                       "interaction", "interaction_metadata", "message_reference", "message_snapshots")),
            "not_ordinary")
    stamp = datetime.fromisoformat(message.get("timestamp", "").replace("Z", "+00:00"))
    require(stamp.tzinfo is not None and datetime(2026, 10, 26, tzinfo=timezone.utc) <= stamp
            < datetime(2026, 10, 28, tzinfo=timezone.utc), "message_time")
    # The fixed snowflake also binds the returned timestamp (within one ms).
    require(abs(stamp.timestamp() * 1000 - ((int(MESSAGE) >> 22) + 1420070400000)) < 1, "message_time")
    mentions = message.get("mentions")
    require(isinstance(mentions, list) and all(isinstance(item, dict) and snowflake(item.get("id"))
                                             for item in mentions), "invalid_response")
    require(not any(item["id"] in (env["EXPECTED_BOT_ID"], env["EXPECTED_APPLICATION_ID"])
                    for item in mentions), "app_mention_exception")
    content = message.get("content")
    require(isinstance(content, str), "invalid_response")
    require(not any(f"<@{prefix}{identity}>" in content for prefix in ("", "!")
                    for identity in (env["EXPECTED_BOT_ID"], env["EXPECTED_APPLICATION_ID"])),
            "app_mention_exception")
    return len(content)


def measure(env, report):
    admit(env)
    application = get_json(APPLICATION_PATH, env["DISCORD_BOT_TOKEN"], report)
    bot = application.get("bot")
    require(application.get("id") == env["EXPECTED_APPLICATION_ID"]
            and isinstance(bot, dict) and bot.get("id") == env["EXPECTED_BOT_ID"], "application_identity")
    grants = flags(application)
    report.update(application_id=application["id"], bot_id=bot["id"], **grants)
    message = get_json(MESSAGE_PATH, env["DISCORD_BOT_TOKEN"], report)
    length = validate_message(message, env)
    report.update(message_identity_validated=True, content_length=length, content_length_unit="unicode_code_points")


def timeout(_signum, _frame):
    raise TimeoutError()


def hosted_environment():
    env = dict(os.environ)
    # Read dispatch values from the runner event file: do not echo author IDs in
    # Actions' step environment display or interpolate operator input into shell.
    with open(env["GITHUB_EVENT_PATH"], "rb") as event_file:
        raw = event_file.read(64 * 1024 + 1)
    require(len(raw) <= 64 * 1024, "admission")
    event = json.loads(raw, object_pairs_hook=unique_object)
    inputs = event["inputs"]
    require(isinstance(inputs, dict), "admission")
    for key in ("expected_sha", "expected_application_id", "expected_bot_id", "expected_author_id",
                "ordinary_text_confirmed"):
        env[key.upper()] = inputs.get(key)
    return env


def main():
    report = {}
    code = 1
    try:
        signal.signal(signal.SIGALRM, timeout)
        signal.alarm(30)
        measure(hosted_environment(), report)
        report["result"] = "measured"
        code = 0
    except DiagnosticError as error:
        report["error"] = str(error)  # Only fixed in-source error classes reach this type.
    except TimeoutError:
        report["error"] = "timeout"
    except (ValueError, TypeError, AttributeError, OverflowError):
        report["error"] = "invalid_response"
    except Exception:
        report["error"] = "transport_or_internal"
    finally:
        signal.alarm(0)
    print(json.dumps(report, sort_keys=True))
    return code


if __name__ == "__main__":
    sys.exit(main())
