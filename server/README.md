# claude-remote-bridge

The server half of [Claude Remote](https://github.com/ThatCrispyToast/g2-claude-remote):
a small JSON+SSE bridge that the Even Realities G2 glasses app talks to. Run it
on any machine that is **logged in to Claude Code** (`claude` → `/login` with a
claude.ai account) and runs `claude remote-control` sessions.

## Run

```bash
uvx --from "git+https://github.com/ThatCrispyToast/g2-claude-remote#subdirectory=server" claude-remote-bridge
```

That's it. On startup it prints the URLs the phone can reach (localhost, LAN IP,
Tailscale IP) and a bearer token — a short word passphrase like
`coral-anvil-mango-scoop-visor`, generated on first run and persisted to
`~/.config/claude-remote/bridge-token` — which you copy into the Claude Remote
app panel's **Settings** card once. The passphrase form is deliberate: it's made
to be *read off the banner and typed by hand* without error, unlike a random
blob. (Setting your own `--token` / `RC_BRIDGE_TOKEN` overrides it, any format.)
No config files required.

If the phone can't connect, open the bridge's port (default `8790`) in the
host's firewall.

**Updating:** `uvx` caches the built environment, so a plain re-run keeps
whatever versions it first resolved — including the `claude-rc-api` git
dependency. To pick up fixes, run once with `--refresh`:

```bash
uvx --refresh --from "git+https://github.com/ThatCrispyToast/g2-claude-remote#subdirectory=server" claude-remote-bridge
```

In particular, **"messages send (HTTP 200, they appear in the session) but
Claude never responds"** on Claude Code ≥ 2.1.220 is fixed in `claude-rc-api`
0.2.1 — newer workers only start a turn for messages whose ingest identifies as
a human client, and older library versions identified as something the worker
demotes to a peer-agent message. Refresh as above to get it.

Also works with `pip install "git+https://github.com/ThatCrispyToast/g2-claude-remote#subdirectory=server"`
then `claude-remote-bridge`, or from a repo checkout via `python3 server/rc_bridge.py`.

## Options

```
--host HOST     bind address (default 0.0.0.0)
--port PORT     port (default 8790)
--token TOKEN   bearer token the app must present
--open          run WITHOUT authentication (dev only; loopback binds only)
--verbose       log every request

--max-auth-failures N    rejected attempts from one source before it is
                         temporarily blocked (default 10; 0 disables)
--auth-block-seconds N   how long a blocked source stays blocked (default 300)
--trust-proxy            take the client IP from X-Forwarded-For; ONLY behind a
                         trusted reverse proxy
```

`--open` is refused on a non-loopback bind: unauthenticated plus the default
`0.0.0.0` would publish an endpoint that steers every session of the logged-in
account to anyone who can reach the port.

Repeated failed authentications from one source are throttled: 10 within 60s
blocks that source for 5 minutes, answered `429` with `Retry-After`. A **valid**
token is always honoured, even from a blocked source — otherwise anyone could
lock the owner out, since sources are keyed by IP and behind a reverse proxy
without `--trust-proxy` every caller shares the proxy's address. Successful
requests are never counted, so the app's polling and long-lived SSE streams
cannot trip it.

`--trust-proxy` is opt-in for a reason: reading `X-Forwarded-For`
unconditionally would let any caller set the header and draw a fresh allowance
per request. With one trusted proxy in front, the rightmost entry is the one it
appended and the only element a client cannot forge.

Environment variables `RC_BRIDGE_HOST` / `RC_BRIDGE_PORT` / `RC_BRIDGE_TOKEN` /
`RC_BRIDGE_VERBOSE` / `RC_BRIDGE_MAX_AUTH_FAILURES` /
`RC_BRIDGE_AUTH_BLOCK_SECONDS` / `RC_BRIDGE_TRUST_PROXY` are honored, as is a `.env.local` in the working directory
or repo checkout (`VITE_BRIDGE_TOKEN` doubles as the token, so the app repo's
config file configures both sides). `RC_BRIDGE_TOKEN_WORDS` sets how many words
a *generated* passphrase has (default 5 ≈ 52 bits; floored at 3).

## Stream credentials

`EventSource` cannot set an `Authorization` header, so the SSE stream has to
carry its credential in the URL — where it lands in this server's access log, in
any reverse proxy's log, and in anything that samples URLs. The bearer token is
long-lived, so one captured line hands over the account.

Preferred flow: `POST /api/tickets` with the bearer header returns
`{"ticket": "...", "expires_in": 30}`. Pass it as `?ticket=` on
`/api/sessions/<id>/stream`. A ticket is **single-use** (spent on redemption, so
replays fail) and valid only on the stream path, so a copy recovered from a log
is worthless. `?token=` still works for older clients.

Request lines are redacted before logging — both `token=` and `ticket=` are
written as `REDACTED` — but redaction only covers *this* server's log. A reverse
proxy in front keeps its own; configure it not to log query strings.

## What it does

Wraps [`claude-rc-api`](https://github.com/ThatCrispyToast/claude-rc-api)'s
`RemoteControlClient` and exposes exactly what the glasses UI needs: an
active-sessions-only list, per-session SSE event streams, send/interrupt/
model/mode/effort/archive controls, and — beyond what the stock tooling can do —
routes that **answer blocking permission prompts and questions**. It adds no
Anthropic protocol code of its own, keeps no state, and survives OAuth token
rotation (the API client reloads rotated credentials from disk).

Security model: bearer token (a ~52-bit word passphrase, auto-generated by
default), permissive CORS (safe — the token, not a cookie, is the guard), meant
for a private LAN or tailnet only. Anyone with the token can read and steer every
remote-control session of the logged-in account — do not expose it publicly.

## Publishing note

The `claude-rc-api` dependency is a direct git URL so the `uvx --from git+…`
one-liner works without any PyPI release. PyPI rejects direct URL dependencies:
to publish this package there, release `claude-rc-api` to PyPI first and switch
the dependency to a version specifier.

## Credits

`claude_remote_bridge/wordlist.py` embeds the EFF "Short Wordlist #1" (Joseph
Bonneau / the Electronic Frontier Foundation, 2016 — <https://www.eff.org/dice>),
used under [CC BY 3.0 US](https://creativecommons.org/licenses/by/3.0/us/), to
generate the human-typeable token passphrases.
