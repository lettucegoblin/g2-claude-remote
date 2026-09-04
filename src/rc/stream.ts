// SSE stream wrapper around the bridge's GET …/stream endpoint.
//
// `EventSource` can't set an Authorization header, so the stream's credential
// has to ride in the query string. It used to be the long-lived bearer token
// (`?token=`) — the key to every session on the account, deposited into the
// bridge's log, any reverse-proxy access log, and anything else that samples
// URLs. Instead we mint a single-use, seconds-long TICKET per connect
// (`POST /api/tickets` → `?ticket=`), so by the time a URL reaches a log the
// credential in it is already spent. `?token=` stays as the fallback for a
// bridge that predates the endpoint.
//
// That single-use property is also why reconnection is OURS rather than the
// browser's: EventSource's built-in retry replays the exact URL it first
// opened, and a spent ticket answers 401 — a stream that works once and then
// never again. So every transport blip closes the EventSource (which cancels
// that built-in retry) and reopens with a freshly minted ticket, resuming from
// the newest `id:` seen exactly as the browser's own `Last-Event-ID` would.
//
// The bridge frames each event as `id: <seq>\ndata: <RcEvent JSON>` and signals
// mid-stream failure with a named `event: error` frame (which carries `.data`)
// — distinct from a transport blip (a native 'error' Event with no `.data`).

import { RECONNECT_BASE_DELAY_MS, RECONNECT_MAX_ATTEMPTS, RECONNECT_MAX_DELAY_MS } from '../config'
import { BridgeError, type RcEvent } from './types'

export interface StreamHandlers {
  onEvent: (e: RcEvent) => void
  onError: (err: BridgeError) => void
  onOpen?: () => void
}

/** How a stream authenticates itself. `mintTicket` resolves to `null` on a
 *  bridge with no `/api/tickets`, which sends the connect down the `?token=`
 *  fallback; `token` is that fallback's credential (and is empty on a bridge
 *  running without auth at all). */
export interface StreamAuth {
  token: string
  mintTicket: () => Promise<string | null>
}

/** Open a live event stream. Returns a close/unsubscribe function. */
export function openEventStream(
  streamUrl: string,
  auth: StreamAuth,
  fromSeq: number,
  handlers: StreamHandlers,
): () => void {
  let es: EventSource | null = null
  let closed = false
  let timer: ReturnType<typeof setTimeout> | null = null
  // Consecutive failures to get a live stream — a refused mint, a refused
  // connect, or a dropped socket. Reset by a successful open; past the cap we
  // give up and hand the caller a terminal error rather than churn forever.
  let attempts = 0
  // Where a reconnect resumes: the newest `id:` seen, which is precisely what
  // the browser would have sent as `Last-Event-ID`. The bridge folds query and
  // header together with max(), so our reconnect replays what a native one did.
  let resumeSeq = fromSeq

  const close = (): void => {
    if (closed) return
    closed = true
    if (timer) clearTimeout(timer)
    timer = null
    es?.close()
    es = null
  }

  /** Drop the current EventSource — which cancels the built-in retry that would
   *  replay our spent ticket — and reopen after a backoff with a fresh one. */
  const retry = (): void => {
    es?.close()
    es = null
    attempts += 1
    if (attempts > RECONNECT_MAX_ATTEMPTS) {
      close()
      handlers.onError(new BridgeError('stream unreachable', 0))
      return
    }
    const delay = Math.min(RECONNECT_BASE_DELAY_MS * 2 ** (attempts - 1), RECONNECT_MAX_DELAY_MS)
    timer = setTimeout(() => {
      timer = null
      void connect()
    }, delay)
  }

  const connect = async (): Promise<void> => {
    if (closed) return

    let ticket: string | null = null
    try {
      ticket = await auth.mintTicket()
    } catch (e) {
      // A REFUSED mint is the bearer token's verdict on itself, not a blip —
      // retrying re-asks with the same rejected credential, so surface it.
      const err = e instanceof BridgeError ? e : new BridgeError((e as Error).message, 0)
      if (err.status === 401 || err.status === 403) {
        close()
        handlers.onError(err)
        return
      }
      retry() // unreachable/500 — the bridge may just be bouncing
      return
    }
    if (closed) return // closed while the mint was in flight

    const qs = new URLSearchParams({ from_seq: String(resumeSeq) })
    if (ticket) qs.set('ticket', ticket)
    else if (auth.token) qs.set('token', auth.token) // pre-ticket bridge
    const src = new EventSource(`${streamUrl}?${qs.toString()}`)
    es = src

    src.onopen = () => {
      attempts = 0
      handlers.onOpen?.()
    }

    src.onmessage = (ev: MessageEvent) => {
      // Track the resume point even for a frame we can't parse: the bridge has
      // still delivered it, and replaying it on reconnect would only duplicate.
      if (/^\d+$/.test(ev.lastEventId)) resumeSeq = Math.max(resumeSeq, Number(ev.lastEventId))
      if (!ev.data) return
      try {
        handlers.onEvent(JSON.parse(ev.data) as RcEvent)
      } catch {
        /* ignore an unparseable frame rather than tear down the stream */
      }
    }

    // A named `event: error` frame (has .data) is an app error the bridge sent
    // right before closing — surface it and stop, since reconnecting to a dead
    // or inactive session would just collect the same refusal.
    src.addEventListener('error', (ev: Event) => {
      if (closed || src !== es) return // a superseded source's parting shot
      const data = (ev as MessageEvent).data
      if (!data) {
        // Transport-level: the socket dropped, or the bridge refused the
        // connect outright. Either way this URL's ticket is gone, so reopen
        // with a fresh one rather than let EventSource replay the spent one.
        retry()
        return
      }
      let err: BridgeError
      try {
        const o = JSON.parse(data)
        err = new BridgeError(String(o.error ?? 'stream error'), Number(o.status) || 500)
      } catch {
        err = new BridgeError('stream error', 500)
      }
      close()
      handlers.onError(err)
    })
  }

  void connect()
  return close
}
