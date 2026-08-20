// Release notices — the app's only "important notice" channel.
//
// Design constraints (deliberate, keep them):
//   * BAKED IN. The list below is compiled into the build and rides each app
//     update — no server, no fetch, nothing to reach. A pack shows exactly the
//     notices its source shipped with, nothing else, ever.
//   * SHOWN ONCE. A notice renders until the wearer dismisses it, then its id
//     is recorded in the device's settings blob (`seenNotices`) and it never
//     appears again. The blob is mirrored into the Even App-side store
//     (main.ts `persistSettings`), because the WebView's own localStorage is
//     evicted between app launches — without the mirror "once" would mean
//     "once per launch".
//   * UNOBTRUSIVE. Panel-only (a slim dismissible card above the session
//     list); the glasses HUD is never touched. No modal, no toast, no badge.
//
// Adding a notice for a new update: append an entry here (stable id, newest
// first) and bump the app version. Removing stale entries is fine — seen-ids
// are pruned against this list, so the persisted blob stays tiny.

import { loadRuntimeSettings, saveRuntimeSettings } from './config'

export interface Notice {
  /** Stable unique id the seen-state is keyed by. Convention: YYYY-MM-DD-slug. */
  id: string
  /** 'important' gets the accent-wash card; 'info' a plain white one. */
  level: 'info' | 'important'
  title: string
  body: string
  /** Optional copy-ready shell command, rendered as a tap-to-select mono block. */
  command?: string
}

/** Every notice this build knows, newest first. */
export const NOTICES: Notice[] = [
  {
    id: '2026-08-20-bridge-send-fix',
    level: 'important',
    title: 'Fix issued: messages ignored on new Claude Code versions',
    body:
      'Claude Code 2.1.220+ stopped acting on messages sent from the glasses ' +
      'or panel — they reached the session but never got a reply. This is ' +
      'fixed in the bridge stack (claude-rc-api 0.2.1). If the bridge host ' +
      'has updated Claude Code, restart your bridge once with:',
    command:
      'uvx --refresh --from "git+https://github.com/ThatCrispyToast/g2-claude-remote#subdirectory=server" claude-remote-bridge',
  },
]

/** The notices this device hasn't dismissed yet, in shipped (newest-first) order. */
export function unseenNotices(): Notice[] {
  const seen = new Set(loadRuntimeSettings().seenNotices ?? [])
  return NOTICES.filter((n) => !seen.has(n.id))
}

/** Record a dismissal. The caller then re-renders and mirrors the settings blob
 *  to the durable App-side store (see main.ts `dismissNotice`). */
export function markNoticeSeen(id: string): void {
  const s = loadRuntimeSettings()
  const seen = new Set(s.seenNotices ?? [])
  seen.add(id)
  // Prune ids that no longer exist in the baked list: they can never render
  // again, and pruning keeps the persisted blob from growing across updates.
  saveRuntimeSettings({ ...s, seenNotices: [...seen].filter((x) => NOTICES.some((n) => n.id === x)) })
}
