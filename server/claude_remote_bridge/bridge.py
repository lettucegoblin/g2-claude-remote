"""Claude Remote bridge — a small JSON+SSE server the G2 glasses app talks to.

It is a purpose-built sibling of ``claude-rc``'s ``webui.py``: a dependency-free
``http.server`` that wraps :class:`claude_rc.client.RemoteControlClient` and
exposes exactly what the glasses UI needs. It differs from the stock webui in
five deliberate ways, each required by this app:

1. **Active-only.** ``GET /api/sessions`` returns *only* sessions that are
   ``status == "active"`` **and** ``connection_status == "connected"`` — i.e.
   neither archived nor dead (a dead session is one whose ``claude
   remote-control`` worker has disconnected; sends to it would queue forever).
   Every mutating / streaming route re-checks this and refuses with **409** if
   the session is no longer active. This is the app's non-negotiable rule and it
   is enforced here, server-side, as the single source of truth.
2. **Auth.** Every request must carry ``Authorization: Bearer $RC_BRIDGE_TOKEN``.
   The SSE stream is the sole exception -- ``EventSource`` cannot set headers --
   and takes a single-use ``?ticket=`` from ``POST /api/tickets``, or a legacy
   ``?token=``. Neither query credential is honoured on any other route. The bridge
   binds to ``0.0.0.0`` so the phone can reach it over Tailscale/LAN, so unlike
   the loopback-only webui it needs its own shared-secret guard.
3. **CORS.** The WebView bundle is a different origin, so responses carry
   permissive CORS headers (safe: the bearer token, not a cookie, is the guard).
4. **Richer, camelCase events.** ``RcEvent`` is a superset of the webui's
   flattened event that additionally carries ``requestId`` / ``toolUseId`` /
   ``permissionRequest`` (needed to answer a permission prompt) and ``usage``.
   ``permissionRequest`` covers both blocking flavors: tool-permissions
   (``can_use_tool``) and QUESTIONS (with parsed ``questions``/options). NOTE the
   sharp edge, confirmed against a live prompt: the **AskUserQuestion** tool is
   delivered as a ``can_use_tool`` permission whose ``input`` holds the
   ``questions`` — *not* as a ``request_user_dialog`` — so it is answered on the
   PERMISSION path (allow + ``updatedInput.answers``), not with a dialog result.
   The ``request_user_dialog`` / ``side_question`` subtypes exist for rarer,
   still-unconfirmed dialog kinds. Two routes close the loop: ``POST …/permission``
   (allow/deny) and ``POST …/dialog`` (answer a question — routes a ``can_use_tool``
   question back through the permission builder) — neither the webui nor
   ``RemoteControlClient`` can do these on their own.
5. **Effort control with a working fallback.** ``POST …/effort`` calls
   claude-rc-api's ``set_effort`` with wait+fallback: the protocol-correct
   ``apply_flag_settings`` control is refused by current remote-control
   workers, so on refusal the ``/effort <level>`` slash command is injected
   instead — workers execute it as a zero-cost local command. NOTE that the
   command path persists the worker machine's default effort (CLI semantics,
   not session-scoped); the response's ``via`` says which path ran.

Run it — the goal is "one command and the glasses can connect":

    uvx --from "git+https://github.com/ThatCrispyToast/g2-claude-remote#subdirectory=server" claude-remote-bridge
    claude-remote-bridge                       # if pip/uv-installed
    python3 server/rc_bridge.py                # from a repo checkout (dev shim)

On startup it prints every URL the phone can use (localhost / LAN IP /
Tailscale IP) and the bearer token. With no token configured anywhere it
GENERATES one — a short word passphrase like ``coral-anvil-mango-scoop-visor``
(see ``wordlist``), made to be typed by hand, not a random blob — and persists
it to ``~/.config/claude-remote/bridge-token``, so the pairing flow is: run the
command, then copy one URL + the token into the app panel's Settings card.
``--open`` disables auth (dev only).

Config precedence (highest wins): CLI flags → ``RC_BRIDGE_*`` env vars →
``.env.local`` (in the CWD or the repo checkout; ``VITE_BRIDGE_TOKEN`` doubles
as the token) → the persisted token file → generate-and-persist.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import ipaddress
import json
import math
import os
import re
import secrets
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from . import __version__
from .wordlist import WORDS

# In a repo checkout this file lives at server/claude_remote_bridge/bridge.py,
# so parents[2] is the repo root (where .env.local and ../claude-rc-api live).
# In an installed wheel those paths simply don't exist and are skipped.
_REPO_ROOT = Path(__file__).resolve().parents[2]

try:
    from claude_rc.client import APIError, RemoteControlClient
except ModuleNotFoundError:
    # Not installed — fall back to a checkout next to the repo (../claude-rc-api).
    _sibling = _REPO_ROOT.parent / "claude-rc-api"
    if (_sibling / "claude_rc").is_dir():
        sys.path.insert(0, str(_sibling))
    try:
        from claude_rc.client import APIError, RemoteControlClient
    except ModuleNotFoundError as exc:  # pragma: no cover - operator error
        sys.stderr.write(
            f"cannot import claude_rc ({exc}).\n"
            "Install the API client one of these ways:\n"
            "  pip install git+https://github.com/ThatCrispyToast/claude-rc-api\n"
            "  # or clone it next to this repo (auto-detected) and install its\n"
            "  # one dependency:  pip install httpx\n"
        )
        raise SystemExit(2)
from claude_rc.credentials import CredentialsError, load_credentials, load_org_uuid
from claude_rc.events import Event, _to_int


# --- config ----------------------------------------------------------------
def _env_file() -> dict[str, str]:
    """KEY=VALUE pairs from `.env.local` (the app's config file), so one file
    can configure both sides. Looked for in the CWD first, then the repo
    checkout this module sits in. Comments/blanks skipped, quotes stripped."""
    out: dict[str, str] = {}
    for candidate in (Path.cwd() / ".env.local", _REPO_ROOT / ".env.local"):
        try:
            text = candidate.read_text()
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            out[key.strip()] = value.split(" #", 1)[0].strip().strip("'\"")
        break
    return out


_ENV_FILE = _env_file()


def _cfg(key: str, default: str = "") -> str:
    """A config value: the process env wins, then `.env.local`, then `default`."""
    return os.environ.get(key) or _ENV_FILE.get(key) or default


# Set by main() before serve(); module-level so the request handlers see them.
HOST = "0.0.0.0"
PORT = 8790
TOKEN = ""
VERBOSE = False
# Failed-auth throttling. The bearer token is the only thing standing in front
# of an endpoint that steers every session of the logged-in account, and the
# timing-safe comparison only removes the shortcut -- nothing else slows a
# guesser down. 0 failures disables the limiter.
MAX_AUTH_FAILURES = 10
AUTH_BLOCK_S = 300
TRUST_PROXY = False
# Sliding window over which failures accumulate toward MAX_AUTH_FAILURES.
_AUTH_WINDOW_S = 60.0
# Cap on tracked sources, so a public endpoint being sprayed from many
# addresses cannot grow these tables without bound.
_AUTH_TABLE_CAP = 4096

_auth_lock = threading.Lock()
_auth_fails: dict[str, list[float]] = {}
_auth_blocked: dict[str, float] = {}


_SECRET_QS = re.compile(r"((?:token|ticket)=)[^&\s\"']+")


def _redact(value: Any) -> Any:
    """Strip credentials from anything on its way to a log.

    EventSource cannot set an Authorization header, so the SSE stream has to
    carry its credential in the URL -- which BaseHTTPRequestHandler would
    otherwise write to the access log verbatim.
    """
    return _SECRET_QS.sub(r"\1REDACTED", value) if isinstance(value, str) else value


# Short-lived, single-use tickets for the SSE stream. A URL credential leaks by
# design: into this server's log, into any reverse proxy's log, into anything
# that samples URLs. The bearer token is long-lived, so a single captured line
# hands over the account. A ticket is minted by an authenticated POST, spent on
# first use, and dead within seconds -- so the same captured line is worthless.
TICKET_TTL_S = 30
_ticket_lock = threading.Lock()
_tickets: dict[str, float] = {}


def mint_ticket() -> tuple[str, int]:
    """A fresh single-use stream credential and its lifetime in seconds."""
    now = time.monotonic()
    ticket = secrets.token_urlsafe(32)
    with _ticket_lock:
        for k, exp in list(_tickets.items()):
            if exp <= now:
                del _tickets[k]
        _tickets[ticket] = now + TICKET_TTL_S
    return ticket, TICKET_TTL_S


def redeem_ticket(ticket: str) -> bool:
    """Spend a ticket. Always consumes it, so a replay cannot succeed."""
    if not ticket:
        return False
    now = time.monotonic()
    with _ticket_lock:
        exp = _tickets.pop(ticket, None)
    return exp is not None and exp > now


def auth_retry_after(ip: str) -> int:
    """Seconds this source must wait before trying again; 0 if it may proceed."""
    if MAX_AUTH_FAILURES <= 0:
        return 0
    now = time.monotonic()
    with _auth_lock:
        until = _auth_blocked.get(ip)
        if until is None:
            return 0
        if until > now:
            return max(1, int(until - now))
        del _auth_blocked[ip]  # served its time
        _auth_fails.pop(ip, None)
    return 0


def note_auth_failure(ip: str) -> int:
    """Record a rejected attempt. Returns the block length if this tripped it."""
    if MAX_AUTH_FAILURES <= 0:
        return 0
    now = time.monotonic()
    with _auth_lock:
        hits = [t for t in _auth_fails.get(ip, ()) if now - t < _AUTH_WINDOW_S]
        hits.append(now)
        if len(hits) >= MAX_AUTH_FAILURES:
            _auth_fails.pop(ip, None)
            _auth_blocked[ip] = now + AUTH_BLOCK_S
            return AUTH_BLOCK_S
        _auth_fails[ip] = hits
        if len(_auth_fails) > _AUTH_TABLE_CAP:
            for k, v in list(_auth_fails.items()):
                if not v or now - v[-1] >= _AUTH_WINDOW_S:
                    _auth_fails.pop(k, None)
    return 0

VALID_MODES = {"default", "plan", "acceptEdits", "bypassPermissions"}
# Effort levels the worker's flag-settings schema accepts remotely. "max" is
# session-scoped in the CLI and rejected on this path; "auto" maps to None
# (an explicit null effortLevel clears back to the model default).
VALID_EFFORTS = {"low", "medium", "high", "xhigh"}

# --- route patterns (first hit wins) ---------------------------------------
_SID = r"(?P<sid>[^/]+)"
_R_SESSION = re.compile(rf"^/api/sessions/{_SID}$")
_R_EVENTS = re.compile(rf"^/api/sessions/{_SID}/events$")
_R_STREAM = re.compile(rf"^/api/sessions/{_SID}/stream$")
_R_SEND = re.compile(rf"^/api/sessions/{_SID}/send$")
_R_PERMISSION = re.compile(rf"^/api/sessions/{_SID}/permission$")
_R_DIALOG = re.compile(rf"^/api/sessions/{_SID}/dialog$")
_R_INTERRUPT = re.compile(rf"^/api/sessions/{_SID}/interrupt$")
_R_MODEL = re.compile(rf"^/api/sessions/{_SID}/model$")
_R_PERMMODE = re.compile(rf"^/api/sessions/{_SID}/permission_mode$")
_R_EFFORT = re.compile(rf"^/api/sessions/{_SID}/effort$")
_R_ARCHIVE = re.compile(rf"^/api/sessions/{_SID}/archive$")


class SessionInactive(Exception):
    """Raised when a session is archived or dead — never controllable."""


def unwrap_session(s: Any) -> dict:
    """Normalise a session object.

    ``list_sessions`` returns bare session dicts, but ``get_session`` wraps the
    same object under a ``response_shape`` key. Unwrap so downstream code sees one
    consistent shape.
    """
    if isinstance(s, dict) and isinstance(s.get("response_shape"), dict):
        return s["response_shape"]
    return s if isinstance(s, dict) else {}


# --- active predicate (SINGLE SOURCE OF TRUTH) -----------------------------
def is_active(s: dict) -> bool:
    """A session is controllable iff it is active AND its worker is connected.

    - ``status == "archived"``            → excluded (archived).
    - ``status == "active"`` but
      ``connection_status != "connected"`` → excluded (dead: worker gone).
    """
    return s.get("status") == "active" and s.get("connection_status") == "connected"


# --- shaping upstream objects into the app's contract ----------------------
def session_to_json(s: dict) -> dict:
    """Trim a raw upstream session into the ``ActiveSession`` shape (camelCase)."""
    cfg = s.get("config") or {}
    ext = s.get("external_metadata") or {}
    return {
        "id": s.get("id"),
        "title": s.get("title") or "(untitled)",
        "status": s.get("status"),
        "workerStatus": s.get("worker_status"),
        "connectionStatus": s.get("connection_status"),
        "statusBucket": s.get("status_bucket"),
        "model": cfg.get("model") or s.get("model"),
        "permissionMode": cfg.get("permission_mode") or cfg.get("permissionMode"),
        "lastEventAt": s.get("last_event_at"),
        "unread": bool(s.get("unread")),
        "userMessageCount": _to_int(s.get("user_message_count")) or 0,
        "pendingAction": ext.get("pending_action") or None,
    }


def _assistant_model(ev: Event) -> Optional[str]:
    msg = ev.payload.get("message")
    if isinstance(msg, dict) and msg.get("model"):
        return msg.get("model")
    if ev.type == "system":
        return ev.payload.get("model")
    return None


def _dialog_info(req: dict) -> dict:
    """Extract ``dialog_kind`` + normalized questions/options from a QUESTION
    control request (``request_user_dialog`` / ``side_question``).

    The dialog-specific payload is opaque per ``dialog_kind`` and nests under
    different keys depending on the kind, so we probe the common locations and
    normalize whatever we find to the AskUserQuestion shape the UI renders:
    ``questions: [{header, question, multiSelect, options: [{label, description}]}]``.
    A plain confirm dialog with no structured questions surfaces its ``prompt``.
    """
    dialog_kind = req.get("dialog_kind") or req.get("dialogKind")
    # The payload can live directly on the request or nested under one of these.
    payload: Optional[dict] = req
    for key in ("dialog_payload", "payload", "input", "tool_input", "data"):
        v = req.get(key)
        if isinstance(v, dict):
            payload = v
            break

    raw_questions = None
    for src in (payload, req):
        if isinstance(src, dict) and isinstance(src.get("questions"), list):
            raw_questions = src["questions"]
            break

    questions = []
    for q in raw_questions or []:
        if not isinstance(q, dict):
            continue
        options = []
        for o in q.get("options") or []:
            if isinstance(o, dict):
                options.append(
                    {"label": o.get("label") or o.get("value") or "", "description": o.get("description") or ""}
                )
            elif isinstance(o, str):
                options.append({"label": o, "description": ""})
        questions.append(
            {
                "header": q.get("header") or "",
                "question": q.get("question") or q.get("text") or q.get("prompt") or "",
                "multiSelect": bool(q.get("multiSelect") or q.get("multi_select")),
                "options": options,
            }
        )

    prompt = ""
    if not questions:
        for key in ("message", "prompt", "text", "title", "question"):
            v = (payload or {}).get(key) if isinstance(payload, dict) else None
            if isinstance(v, str) and v.strip():
                prompt = v
                break

    return {"dialogKind": dialog_kind, "questions": questions, "prompt": prompt}


def _permission_request(ev: Event) -> Optional[dict]:
    """The details out of a blocking control request — either a tool-permission
    (``can_use_tool``) or a question (``request_user_dialog`` / ``side_question``).

    The inbound control request nests everything under ``payload.request``. We keep
    the whole ``request`` blob (so the UI can show whatever's there) plus pull the
    common fields to well-known names, and — for questions — the parsed
    ``dialogKind`` / ``questions`` / ``prompt``. Field locations were confirmed
    against a live prompt; we stay tolerant of alternates.
    """
    if not ev.is_blocking_control:
        return None
    req = ev.payload.get("request") or {}
    tool_use_id = (
        req.get("tool_use_id")
        or req.get("toolUseId")
        or (req.get("tool") or {}).get("id")
    )
    dialog = _dialog_info(req)
    return {
        "subtype": req.get("subtype"),
        "toolName": req.get("tool_name") or req.get("name") or (req.get("tool") or {}).get("name"),
        "toolUseId": tool_use_id,
        "input": req.get("input") or req.get("tool_input") or (req.get("tool") or {}).get("input"),
        "suggestions": req.get("permission_suggestions") or req.get("suggestions"),
        "dialogKind": dialog["dialogKind"],
        "questions": dialog["questions"],
        "prompt": dialog["prompt"],
        "raw": req,
    }


def _usage(ev: Event) -> Optional[dict]:
    if ev.type != "result":
        return None
    return {
        "costUsd": ev.payload.get("total_cost_usd"),
        "numTurns": ev.payload.get("num_turns"),
        "durationMs": ev.payload.get("duration_ms"),
        "isError": bool(ev.payload.get("is_error")),
    }


def event_to_json(ev: Event) -> dict:
    """Flatten an :class:`Event` into ``RcEvent`` (camelCase, superset of webui)."""
    blocking_subtype = None
    request_id = None
    if ev.type == "control_request":
        req = ev.payload.get("request") or {}
        blocking_subtype = req.get("subtype")
        request_id = ev.payload.get("request_id")
    elif ev.type == "control_response":
        # A control_response's request_id correlates it to the control_request it
        # answers — the app uses it to auto-dismiss a prompt answered elsewhere
        # (another controller / the CLI itself).
        resp = ev.payload.get("response") or {}
        request_id = resp.get("request_id") or ev.payload.get("request_id")
    perm = _permission_request(ev)
    return {
        "type": ev.type,
        "subtype": ev.subtype,
        "role": ev.role,
        "text": ev.text(),
        "toolUses": [
            {"name": t.get("name"), "input": t.get("input"), "id": t.get("id")}
            for t in ev.tool_uses()
        ],
        "sequenceNum": ev.sequence_num,
        "id": ev.id,
        "timestamp": ev.processed_at,
        "model": _assistant_model(ev),
        "isTurnEnd": ev.is_turn_end,
        "isTerminal": ev.is_terminal,
        "isBlockingControl": ev.is_blocking_control,
        "blockingSubtype": blocking_subtype,
        "requestId": request_id,
        "toolUseId": perm.get("toolUseId") if perm else None,
        "permissionRequest": perm,
        "usage": _usage(ev),
    }


def build_permission_answer(
    request_id: str,
    tool_use_id: Optional[str],
    decision: str,
    updated_input: Any = None,
    message: Optional[str] = None,
) -> dict:
    """Construct the ``control_response`` that answers a ``can_use_tool`` prompt.

    Claude Code's stream-json control protocol answers a ``control_request`` with
    a ``control_response`` whose outer ``subtype`` is ``success`` (the request was
    handled) and whose inner ``response`` carries the actual decision
    (``behavior: allow|deny``). ``request_id`` correlates it to the prompt. This
    shape was validated against a live prompt; ``send_raw`` delivers it.
    """
    behavior = "allow" if decision == "allow" else "deny"
    inner: dict[str, Any] = {"behavior": behavior}
    if behavior == "allow":
        # An allow must echo the (possibly edited) tool input back.
        inner["updatedInput"] = updated_input if updated_input is not None else {}
    else:
        inner["message"] = message or "Denied by controller"
    resp: dict[str, Any] = {"subtype": "success", "request_id": request_id, "response": inner}
    if tool_use_id:
        resp["tool_use_id"] = tool_use_id
    return {"type": "control_response", "response": resp}


def _answers_map(answers: Any) -> dict[str, Any]:
    """Fold the app's ``[{header, question, options: [chosen labels]}]`` into the
    AskUserQuestion answer map: ``{<question text>: <label | [labels]>}``.

    The tool keys answers by the *question* string (``answers[question]`` in the
    tool's ``mapToolResultToToolResultBlockParam``), so that's the key. A single
    pick is a bare label; a multi-select is the list. Unanswered questions are
    simply omitted (→ the tool reports them as "(no option selected)").
    """
    out: dict[str, Any] = {}
    for a in answers or []:
        if not isinstance(a, dict):
            continue
        q = a.get("question") or a.get("header")
        picks = [p for p in (a.get("options") or []) if p]
        if not q or not picks:
            continue
        out[q] = picks[0] if len(picks) == 1 else picks
    return out


def build_question_answer(
    request_id: str,
    tool_use_id: Optional[str],
    status: str,
    answers: Any,
    original_input: Any,
) -> dict:
    """Answer an **AskUserQuestion** — which the remote-control API delivers as a
    ``can_use_tool`` permission request, *not* a ``request_user_dialog`` — by
    ALLOWING the tool with the wearer's picks merged into ``updatedInput``.

    Confirmed against a live AskUserQuestion (``pathogen-test``): the chosen
    option(s) ride in ``updatedInput.answers``, a map keyed by *question text* →
    label. The tool then returns ``"<question>"="<label>"`` and the model reads
    ``[User answered AskUserQuestion]: …``. An empty ``answers`` yields
    "The user did not answer the questions." — which is exactly the graceful
    *dismiss*, so a ``cancelled`` status simply allows with an empty map.

    ``updatedInput`` MUST echo ``questions`` — the tool destructures it and
    crashes ("Cannot destructure property 'questions'") if it is missing — so we
    carry the original input through and reconstruct a minimal ``questions`` list
    if it somehow didn't reach us.
    """
    answer_map = _answers_map(answers) if status == "completed" else {}
    base = dict(original_input) if isinstance(original_input, dict) else {}
    if not isinstance(base.get("questions"), list):
        base["questions"] = [
            {
                "question": (a.get("question") or a.get("header") or ""),
                "header": a.get("header") or "",
                "options": [],
                "multiSelect": False,
            }
            for a in (answers or [])
            if isinstance(a, dict)
        ]
    base["answers"] = answer_map
    return build_permission_answer(request_id, tool_use_id, "allow", updated_input=base)


def build_dialog_answer(
    request_id: str,
    status: str,
    answers: Any = None,
) -> dict:
    """Construct the ``control_response`` for a *true* dialog control request
    (``request_user_dialog`` / ``side_question`` — e.g. elicitation, MCP-approval,
    plan dialogs). **AskUserQuestion does NOT use this path** — it arrives as
    ``can_use_tool`` and is answered by :func:`build_question_answer`.

    Same outer envelope as a permission answer (``control_response`` with
    ``subtype: success`` + ``request_id``), but the inner ``response`` is a dialog
    result: ``{status, result}`` where ``status`` is ``completed`` | ``cancelled``
    and ``result`` is the dialog-specific payload (opaque per ``dialog_kind``).

    This shape is still reverse-engineered and UNCONFIRMED (no live sample of a
    non-tool dialog kind yet) — it's the best-effort fallback for those rarer
    kinds; confirm ``result`` against a live one before trusting it.
    """
    inner: dict[str, Any] = {"status": status}
    if status == "completed":
        inner["result"] = {"answers": answers} if answers else {}
    else:
        inner["result"] = {}
    resp = {"subtype": "success", "request_id": request_id, "response": inner}
    return {"type": "control_response", "response": resp}


class _Bridge(ThreadingHTTPServer):
    daemon_threads = True  # long-lived SSE streams die with the process

    def __init__(self, addr, handler, client: RemoteControlClient):
        super().__init__(addr, handler)
        self.rc = client


def _token_eq(supplied: str) -> bool:
    """Constant-time comparison of a presented token against ``TOKEN``.

    ``==`` on str short-circuits at the first differing byte, so how long the
    check runs leaks how much of the prefix a guess got right -- which turns
    recovering the token from an exponential search into a linear, byte-by-byte
    one. Nothing here rate-limits guesses, so that signal is worth denying.

    Both sides are hashed first rather than passed to ``compare_digest``
    directly: it keeps the compared lengths equal (so the token's own length
    does not leak), and it sidesteps ``compare_digest``'s ASCII-only
    restriction on str -- ``--token`` / ``$RC_BRIDGE_TOKEN`` accept any format.
    """
    if not supplied:
        return False
    return hmac.compare_digest(
        hashlib.sha256(supplied.encode("utf-8")).digest(),
        hashlib.sha256(TOKEN.encode("utf-8")).digest(),
    )


class _Handler(BaseHTTPRequestHandler):
    server_version = "claude-remote-bridge"
    protocol_version = "HTTP/1.1"

    @property
    def rc(self) -> RemoteControlClient:
        return self.server.rc  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        if VERBOSE:
            super().log_message(fmt, *(_redact(a) for a in args))

    # -- request helpers ---------------------------------------------------
    def _query(self) -> dict[str, list[str]]:
        return parse_qs(urlparse(self.path).query)

    def _qint(self, q: dict, key: str, default: int) -> int:
        try:
            return int(q.get(key, [default])[0])
        except (ValueError, TypeError):
            return default

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw or b"{}")
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}

    def _authed(self) -> bool:
        if not TOKEN:
            return True  # dev mode: no token configured
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer ") and _token_eq(auth[7:]):
            return True
        # BOTH query credentials are confined to the stream -- the one request a
        # browser cannot put a header on. Accepting the bearer in a URL anywhere
        # else re-opens exactly what the ticket exists to close: any client,
        # script or pasted link spilling the key to every session into a log this
        # server does not control, and redaction only covers our own. Elsewhere
        # the header is the only way in.
        if _R_STREAM.match(urlparse(self.path).path):
            q = self._query()
            if _token_eq(q.get("token", [None])[0] or ""):
                return True
            # Tried last, and only here, because redemption SPENDS the ticket:
            # honouring it on any other route would let one request burn the
            # credential the stream is about to present.
            return redeem_ticket(q.get("ticket", [""])[0])
        return False

    def _client_ip(self) -> str:
        """The source a failed attempt is charged to.

        The peer address by default. X-Forwarded-For is honoured only under
        --trust-proxy: reading it unconditionally would let any caller set the
        header and draw a fresh allowance per request, which is worse than no
        limiter at all. Behind one trusted proxy the RIGHTMOST entry is the one
        that proxy appended, and the only element a client cannot forge.
        """
        if TRUST_PROXY:
            xff = self.headers.get("X-Forwarded-For", "")
            if xff:
                return xff.rsplit(",", 1)[-1].strip()
        return self.client_address[0]

    def _gate(self) -> bool:
        """Authenticate, throttling repeat failures. Answers on refusal.

        A valid token is honoured even while its source is blocked. Checking
        the block first would let an attacker lock the owner OUT: sources are
        keyed by IP, and behind a reverse proxy without --trust-proxy every
        caller shares the proxy's address. Since an attacker never holds a
        valid token, they still fall through to the block on every attempt --
        the limiter loses nothing, and stops being a denial-of-service lever.
        """
        if self._authed():
            return True
        ip = self._client_ip()
        retry = auth_retry_after(ip)
        if retry:
            self._too_many(retry)
            return False
        blocked = note_auth_failure(ip)
        if VERBOSE:
            note = f" - blocked {blocked}s" if blocked else ""
            print(f"[auth] rejected {ip}{note}", flush=True)
        self._unauth()
        return False

    # -- response helpers --------------------------------------------------
    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    def _json(self, obj: Any, status: int = 200) -> None:
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _unauth(self) -> None:
        self._json({"error": "unauthorized", "status": 401}, status=401)

    def _too_many(self, retry_after: int) -> None:
        body = json.dumps({"error": "too many failed attempts", "status": 429}).encode()
        self.send_response(429)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Retry-After", str(retry_after))
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _error(self, exc: Exception) -> None:
        if isinstance(exc, SessionInactive):
            self._json({"error": "session not active", "status": 409}, status=409)
        elif isinstance(exc, APIError):
            status = exc.status if 400 <= exc.status < 600 else 502
            self._json({"error": exc.body[:1000], "status": status}, status=status)
        elif isinstance(exc, CredentialsError):
            self._json({"error": str(exc), "status": 401}, status=401)
        else:
            self._json({"error": repr(exc), "status": 500}, status=500)

    # -- the active gate ---------------------------------------------------
    def _require_active(self, sid: str) -> dict:
        """Fetch the session and refuse (409) if it is not active+connected."""
        s = unwrap_session(self.rc.get_session(sid))
        if not is_active(s):
            raise SessionInactive(sid)
        return s

    # -- OPTIONS (CORS preflight) ------------------------------------------
    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    # -- GET ---------------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if not self._gate():
            return
        try:
            if path == "/api/whoami":
                return self._json(self._whoami())
            if path in ("/api/sessions", "/api/sessions/"):
                sessions = [session_to_json(s) for s in self.rc.sessions() if is_active(s)]
                return self._json({"sessions": sessions})
            m = _R_STREAM.match(path)
            if m:
                return self._stream(m["sid"])
            m = _R_EVENTS.match(path)
            if m:
                self._require_active(m["sid"])
                limit = self._qint(self._query(), "limit", 200)
                # NEWEST `limit` events (desc + reverse → oldest→newest). Fetching
                # asc would return the OLDEST slice of a long session, leaving the
                # app's resume cursor so far behind that the stream replays the
                # whole backlog (incl. every long-answered permission prompt).
                evs = self.rc.list_events(m["sid"], limit=limit, sort_order="desc")
                evs.reverse()
                return self._json({"events": [event_to_json(e) for e in evs]})
            m = _R_SESSION.match(path)
            if m:
                s = unwrap_session(self.rc.get_session(m["sid"]))
                if not is_active(s):
                    raise SessionInactive(m["sid"])
                return self._json({"session": session_to_json(s)})
            self._json({"error": "not found", "status": 404}, status=404)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:  # noqa: BLE001
            self._error(exc)

    # -- POST --------------------------------------------------------------
    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        body = self._body()  # always drain the body (keep-alive correctness)
        if not self._gate():
            return
        try:
            if path == "/api/tickets":
                ticket, ttl = mint_ticket()
                return self._json({"ticket": ticket, "expires_in": ttl})
            m = _R_SEND.match(path)
            if m:
                self._require_active(m["sid"])
                text = (body.get("text") or "").strip()
                if not text:
                    return self._json({"error": "empty message", "status": 400}, status=400)
                self.rc.send_message(m["sid"], text)
                return self._json({"ok": True})
            m = _R_PERMISSION.match(path)
            if m:
                self._require_active(m["sid"])
                request_id = body.get("request_id")
                decision = body.get("decision")
                if not request_id:
                    return self._json({"error": "missing request_id", "status": 400}, status=400)
                if decision not in ("allow", "deny"):
                    return self._json({"error": "decision must be allow|deny", "status": 400}, status=400)
                payload = build_permission_answer(
                    request_id,
                    body.get("tool_use_id"),
                    decision,
                    body.get("updated_input"),
                    body.get("message"),
                )
                self.rc.send_raw(m["sid"], payload)
                return self._json({"ok": True})
            m = _R_DIALOG.match(path)
            if m:
                self._require_active(m["sid"])
                request_id = body.get("request_id")
                status = body.get("status")
                if not request_id:
                    return self._json({"error": "missing request_id", "status": 400}, status=400)
                if status not in ("completed", "cancelled"):
                    return self._json({"error": "status must be completed|cancelled", "status": 400}, status=400)
                # AskUserQuestion is delivered as a `can_use_tool` permission, so it
                # is answered on the PERMISSION path (allow + updatedInput.answers) —
                # only the rarer true dialog kinds use the {status, result} shape.
                if body.get("subtype") == "can_use_tool":
                    payload = build_question_answer(
                        request_id,
                        body.get("tool_use_id"),
                        status,
                        body.get("answers"),
                        body.get("input"),
                    )
                else:
                    payload = build_dialog_answer(request_id, status, body.get("answers"))
                self.rc.send_raw(m["sid"], payload)
                return self._json({"ok": True})
            m = _R_INTERRUPT.match(path)
            if m:
                self._require_active(m["sid"])
                self.rc.interrupt(m["sid"])
                return self._json({"ok": True})
            m = _R_MODEL.match(path)
            if m:
                self._require_active(m["sid"])
                model = (body.get("model") or "").strip()
                if not model:
                    return self._json({"error": "missing model", "status": 400}, status=400)
                self.rc.set_model(m["sid"], model)
                return self._json({"ok": True})
            m = _R_PERMMODE.match(path)
            if m:
                self._require_active(m["sid"])
                mode = (body.get("mode") or "").strip()
                if mode not in VALID_MODES:
                    return self._json(
                        {"error": f"mode must be one of {sorted(VALID_MODES)}", "status": 400},
                        status=400,
                    )
                self.rc.set_permission_mode(m["sid"], mode)
                return self._json({"ok": True})
            m = _R_EFFORT.match(path)
            if m:
                self._require_active(m["sid"])
                effort = (body.get("effort") or "").strip() or None
                if effort == "auto":
                    effort = None
                if effort is not None and effort not in VALID_EFFORTS:
                    return self._json(
                        {"error": f"effort must be auto or one of {sorted(VALID_EFFORTS)}", "status": 400},
                        status=400,
                    )
                # Current remote-control workers refuse the apply_flag_settings
                # control, so set_effort waits for the worker's verdict and falls
                # back to injecting the /effort slash command (which they DO
                # execute). claude-rc-api < 0.2.0 predates set_effort — the
                # command path alone still works there.
                if hasattr(self.rc, "set_effort"):
                    out = self.rc.set_effort(m["sid"], effort, wait=6.0, command_fallback=True)
                    return self._json({"ok": True, "via": out.get("via", "control")})
                self.rc.send_message(m["sid"], f"/effort {effort or 'auto'}")
                return self._json({"ok": True, "via": "command"})
            m = _R_ARCHIVE.match(path)
            if m:
                # Archiving is the one control that *ends* an active session, so it
                # only requires that the session currently be active.
                self._require_active(m["sid"])
                self.rc.archive_session(m["sid"])
                return self._json({"ok": True})
            self._json({"error": "not found", "status": 404}, status=404)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:  # noqa: BLE001
            self._error(exc)

    # -- handlers ----------------------------------------------------------
    def _whoami(self) -> dict:
        try:
            creds = load_credentials()
        except CredentialsError as exc:
            return {"logged_in": False, "error": str(exc)}
        org = load_org_uuid()
        return {
            "logged_in": True,
            "expired": creds.is_expired(),
            "scopes": creds.scopes,
            "subscription": creds.subscription_type,
            "org_uuid_present": bool(org),
            "token_len": len(creds.access_token),
        }

    def _stream(self, sid: str) -> None:
        # Gate at open: refuse a stream on a non-active session with an error frame.
        try:
            s = unwrap_session(self.rc.get_session(sid))
        except Exception as exc:  # noqa: BLE001
            return self._error(exc)
        if not is_active(s):
            self.close_connection = True
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self._cors()
            self.end_headers()
            self.wfile.write(b"retry: 3000\n: connected\n\n")
            self._sse_error("session not active", 409)
            return

        from_seq = self._qint(self._query(), "from_seq", 0)
        last_event_id = self.headers.get("Last-Event-ID")
        if last_event_id and last_event_id.isdigit():
            from_seq = max(from_seq, int(last_event_id))

        # An event-stream body is delimited only by connection close, so the
        # socket must close when the stream ends. Do NOT send `Connection:
        # keep-alive` — BaseHTTPRequestHandler.send_header special-cases it and
        # resets close_connection back to False, leaving the socket to stall.
        self.close_connection = True
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self._cors()
        self.end_headers()
        self.wfile.write(b"retry: 3000\n: connected\n\n")
        self.wfile.flush()

        try:
            for ev in self.rc.stream_events(sid, from_sequence_num=from_seq):
                data = json.dumps(event_to_json(ev))
                out = ""
                if ev.sequence_num is not None:
                    out += f"id: {ev.sequence_num}\n"
                out += f"data: {data}\n\n"
                self.wfile.write(out.encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return
        except APIError as exc:
            self._sse_error(exc.body[:500], exc.status)
        except Exception as exc:  # noqa: BLE001
            self._sse_error(repr(exc), 500)

    def _sse_error(self, message: str, status: int) -> None:
        try:
            payload = json.dumps({"error": message, "status": status})
            self.wfile.write(f"event: error\ndata: {payload}\n\n".encode())
            self.wfile.flush()
        except OSError:
            pass


# --- startup: token + reachable URLs ----------------------------------------
def _token_store() -> Path:
    """Where a generated token persists across runs (written 0600)."""
    base = Path(os.environ.get("XDG_CONFIG_HOME") or (Path.home() / ".config"))
    return base / "claude-remote" / "bridge-token"


# A generated token is a passphrase — this many words from wordlist.WORDS,
# hyphen-joined — not a random base64 blob, because the end user reads it off
# the startup banner and types it into the panel's Settings card by hand. Five
# words over the 1295-word EFF short list is ~52 bits: comfortably past
# brute-forcing a LAN/tailnet bridge (which does no rate-limiting) yet short
# and unambiguous to type. Override the count with RC_BRIDGE_TOKEN_WORDS;
# it is floored at 3 (~31 bits) so the knob can't foot-gun the token to nothing.
_DEFAULT_TOKEN_WORDS = 5
_MIN_TOKEN_WORDS = 3


def _token_word_count() -> int:
    try:
        n = int(_cfg("RC_BRIDGE_TOKEN_WORDS", str(_DEFAULT_TOKEN_WORDS)))
    except ValueError:
        n = _DEFAULT_TOKEN_WORDS
    return max(_MIN_TOKEN_WORDS, n)


def _generate_passphrase(words: int) -> str:
    """A human-typeable bearer token: `words` words drawn uniformly from the EFF
    short list with `secrets.choice`, joined by hyphens (e.g. `coral-anvil-mango`).
    Hyphens are URL-safe, so it rides unencoded on both the `Bearer` header and
    the SSE `?token=` query."""
    return "-".join(secrets.choice(WORDS) for _ in range(words))


# Bind addresses on which running with auth disabled is survivable.
_LOOPBACK = ("127.0.0.1", "::1", "localhost")


def _resolve_token(cli_token: Optional[str], open_mode: bool) -> tuple[str, str]:
    """The bearer token plus a human note about where it came from.

    Precedence: --token → $RC_BRIDGE_TOKEN → .env.local (RC_BRIDGE_TOKEN /
    VITE_BRIDGE_TOKEN) → the persisted store → freshly generated + persisted.
    Generating by default (rather than running open) is deliberate: the bridge
    binds beyond loopback and can steer every session of the logged-in account.
    """
    if open_mode:
        return "", "auth DISABLED (--open)"
    if cli_token:
        return cli_token, "from --token"
    if os.environ.get("RC_BRIDGE_TOKEN"):
        return os.environ["RC_BRIDGE_TOKEN"], "from $RC_BRIDGE_TOKEN"
    file_token = _ENV_FILE.get("RC_BRIDGE_TOKEN") or _ENV_FILE.get("VITE_BRIDGE_TOKEN")
    if file_token:
        return file_token, "from .env.local"
    store = _token_store()
    try:
        saved = store.read_text().strip()
        if saved:
            return saved, f"saved in {store}"
    except OSError:
        pass
    words = _token_word_count()
    token = _generate_passphrase(words)
    strength = f"GENERATED {words}-word passphrase, ~{round(words * math.log2(len(WORDS)))}-bit"
    try:
        store.parent.mkdir(parents=True, exist_ok=True)
        store.write_text(token + "\n")
        os.chmod(store, 0o600)
        return token, f"{strength} — saved to {store}"
    except OSError:
        return token, f"{strength} — could not persist, so it will CHANGE next run"


def _source_ip(probe: str) -> Optional[str]:
    """The local address the kernel would route to `probe` from (UDP connect
    picks the source interface without sending any packet)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect((probe, 53))
            return s.getsockname()[0]
    except OSError:
        return None


# Tailscale hands out addresses from the CGNAT range.
_TAILSCALE_NET = ipaddress.ip_network("100.64.0.0/10")


def _reachable_urls(port: int) -> list[tuple[str, str]]:
    """(label, url) pairs the phone can try: localhost, the LAN IP, and — when
    Tailscale is up — the Tailscale IP (probing Tailscale's own resolver
    address selects the tailnet interface)."""
    urls: list[tuple[str, str]] = [("local", f"http://127.0.0.1:{port}")]
    seen = {"127.0.0.1"}
    for label, probe in (("LAN", "8.8.8.8"), ("tailscale", "100.100.100.100")):
        ip = _source_ip(probe)
        if not ip or ip in seen:
            continue
        seen.add(ip)
        if ipaddress.ip_address(ip) in _TAILSCALE_NET:
            label = "tailscale"
        urls.append((label, f"http://{ip}:{port}"))
    return urls


def _login_line() -> str:
    try:
        creds = load_credentials()
    except CredentialsError as exc:
        return f"⚠ NOT logged in to Claude Code — {exc}"
    return f"Claude Code login: ok ({creds.subscription_type or 'unknown plan'})"


def serve(token_note: str) -> None:
    try:
        rc = RemoteControlClient()
    except CredentialsError as exc:
        sys.stderr.write(
            f"Not logged in to Claude Code: {exc}\n"
            "On this machine run `claude` and `/login` with a claude.ai account\n"
            "(not an API key), then start the bridge again.\n"
        )
        raise SystemExit(1)
    httpd = _Bridge((HOST, PORT), _Handler, rc)

    print(f"Claude Remote bridge v{__version__}  —  listening on {HOST}:{PORT}   (Ctrl-C to stop)")
    print(f"  {_login_line()}")
    print()
    print("  In the Claude Remote app, open the panel's Settings card and enter:")
    rows = _reachable_urls(PORT) if HOST in ("0.0.0.0", "::", "") else [("bound", f"http://{HOST}:{PORT}")]
    for label, url in rows:
        print(f"    Bridge URL ({label + ')':<11} {url}")
    if TOKEN:
        # Shown ONLY to a terminal. Under systemd or `| tee` this banner IS a log
        # file, and a pairing secret written there outlives every process that
        # could have used it -- journald keeps its own copy besides, so one
        # restart can persist it twice. The note names the source either way, and
        # for a generated token that is the 0600 file to read, so interactive
        # pairing is unchanged and a supervised run says where to look instead.
        if sys.stdout.isatty():
            print(f"    Bridge token:           {TOKEN}")
        else:
            print(f"    Bridge token:           not shown (stdout is not a terminal)")
        print(f"                            ({token_note})")
    else:
        print(
            "  ⚠ UNAUTHENTICATED (--open): anyone who can reach this port can read\n"
            "    and steer your Claude sessions. Dev only."
        )
    print()
    print(f"  You might need to open port {PORT} in this host's firewall.")
    print()
    # The pairing banner must reach the terminal even when stdout is a pipe or
    # a log file (uvx | tee, systemd) — block buffering would sit on it forever.
    sys.stdout.flush()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down…")
    finally:
        httpd.shutdown()
        httpd.server_close()
        rc.close()


def main(argv: Optional[list[str]] = None) -> None:
    global HOST, PORT, TOKEN, VERBOSE, MAX_AUTH_FAILURES, AUTH_BLOCK_S, TRUST_PROXY
    ap = argparse.ArgumentParser(
        prog="claude-remote-bridge",
        description=(
            "Bridge between the Claude Remote glasses app and this machine's "
            "Claude Code Remote Control sessions. Run it where you are logged "
            "in to Claude Code; it prints the URL + token to pair the app."
        ),
    )
    ap.add_argument("--host", default=None, help="bind address (default 0.0.0.0)")
    ap.add_argument("--port", type=int, default=None, help="port (default 8790)")
    ap.add_argument("--token", default=None, help="bearer token the app must present (default: env / .env.local / persisted / a generated word passphrase)")
    ap.add_argument("--open", action="store_true", help="run WITHOUT authentication (dev only)")
    ap.add_argument("--max-auth-failures", type=int, default=None, help="rejected attempts from one source before it is temporarily blocked (default 10; 0 disables)")
    ap.add_argument("--auth-block-seconds", type=int, default=None, help="how long a blocked source stays blocked (default 300)")
    ap.add_argument("--trust-proxy", action="store_true", help="take the client IP from X-Forwarded-For; ONLY behind a trusted reverse proxy")
    ap.add_argument("--verbose", action="store_true", help="log every request")
    ap.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    args = ap.parse_args(argv)

    HOST = args.host or _cfg("RC_BRIDGE_HOST", "0.0.0.0")
    try:
        PORT = args.port or int(_cfg("RC_BRIDGE_PORT", "8790"))
    except ValueError:
        PORT = 8790
    TOKEN, token_note = _resolve_token(args.token, args.open)
    # Running unauthenticated is a dev convenience; binding beyond loopback is
    # the default. Together they publish an endpoint that steers every session
    # of the logged-in account and runs tool calls on this machine, to anyone
    # who can reach the port. Refuse the combination outright rather than
    # trusting the operator to never pair them by accident. Keyed on an empty
    # TOKEN, not on --open, so any other route to a blank token is caught too.
    if not TOKEN and HOST not in _LOOPBACK:
        sys.exit(
            f"refusing to run without authentication on a non-loopback bind ({HOST}).\n"
            f"  --open is dev-only: pair it with --host 127.0.0.1, or drop it and "
            f"use the generated token."
        )
    VERBOSE = args.verbose or _cfg("RC_BRIDGE_VERBOSE") not in ("", "0", "false", "no")
    try:
        MAX_AUTH_FAILURES = (
            args.max_auth_failures if args.max_auth_failures is not None
            else int(_cfg("RC_BRIDGE_MAX_AUTH_FAILURES", "10"))
        )
    except ValueError:
        MAX_AUTH_FAILURES = 10
    try:
        AUTH_BLOCK_S = (
            args.auth_block_seconds if args.auth_block_seconds is not None
            else int(_cfg("RC_BRIDGE_AUTH_BLOCK_SECONDS", "300"))
        )
    except ValueError:
        AUTH_BLOCK_S = 300
    TRUST_PROXY = args.trust_proxy or _cfg("RC_BRIDGE_TRUST_PROXY") not in ("", "0", "false", "no")
    serve(token_note)


if __name__ == "__main__":
    main()
