# PostHog event allowlist

Closed list of product events. Sentry stays the crash tool (`log::error!` →
issue). These events are how we answer “are users having a bad time?”
without opening an issue per stall sample.

Implemented in `apps/desktop/src-tauri/src/analytics.rs` (native) and
`web-harness/src/analytics.ts` (browser). Local and CI builds are keyless
and no-op; a desktop release bakes `PETAL_POSTHOG_KEY`, and a production
web-harness build bakes `VITE_PETAL_POSTHOG_KEY` (never commit the token).
The ingest host can be overridden the same way (`PETAL_POSTHOG_HOST` /
`VITE_PETAL_POSTHOG_HOST`); `scripts/publish-blob.mjs` checks the baked
key per slice before publishing.
Do not add events from the backend. Do not add `posthog-js`.

Dedicated project: **Petal** (id `317298`) in org Kiruna Labs, US cloud —
[project home](https://us.posthog.com/project/317298/home). Project token
lives in PostHog project settings, never in git.

Locked project settings (2026-08-17): session replay off, autocapture opt
out, JS exception autocapture off, heatmaps/surveys off, IPs anonymized,
timezone `America/Los_Angeles`, authorized URLs `petal.live` /
`app.petal.live` / `meet.petal.live`. Same PII posture as Sentry:
allowlist-first, no meeting content.

## Tooling

- **MCP (authenticated):** `posthog` in `~/.cursor/mcp.json` →
  `https://mcp.posthog.com/mcp`. Before any Petal query, `switch-project`
  to `317298` (the server defaults to whichever project last used OAuth).
- **CLI (installed, login still needed):** `posthog-cli` 0.11.3. One-time
  `posthog-cli login` (opens [CLI authorize](https://us.posthog.com/cli/authorize)),
  then `POSTHOG_CLI_PROJECT_ID=317298`. Personal API keys stay out of the
  repo.

## Properties allowed on every event

`build_version`, `os`, `os_version`, `arch`, `client` (`native` \| `web`)

Plus only the extra columns named on the event below. Duration values are
buckets (`0_10s`, `10_30s`, `30_120s`, `120s_plus`), never raw milliseconds.

**Exception (#370): `remote_control_latency_summary`'s latency percentiles
are integer milliseconds.** The bucket rule exists for user-behaviour
durations (how long a meeting or a stall lasted), where a raw value could
help fingerprint a user. These are aggregate performance measurements of
Petal itself (p50/p95/max over a 60 s window of remote control), carry no
identity, and are useless as buckets. The exception covers only the
`<stage>_p50/_p95/_max/_n` columns and the counters on that one event.

`dropped_since_last` (integer, native only, added #908): present only when
greater than 0. `analytics.rs`'s `try_send` onto the bounded `QUEUE_CAP`
channel previously discarded an overflow with no counter and no trace; the
accumulated count is now embedded on the next event that actually reaches
the queue, so loss is visible in PostHog itself instead of only in a local
counter nobody looks at. The count is restored to the running total (for a
later event to carry instead) if EITHER the carrying event fails to enqueue
(`try_send` onto a still-full channel) OR it enqueues but then fails HTTP
delivery after retries are exhausted (the worker reads the value back out
of the body it just failed to send) — so it is not lost by either of those
paths, though it can lag by more than one event under sustained overflow.

It IS still lost if the process exits before a carrying event's count is
ever reported — a crash, a force-quit, or an OS-level quit (Cmd-Q, Dock →
Quit) that bypasses `quit_app`'s `analytics::flush()` (only the in-app Quit
menu item currently routes through that command; wiring the OS-level quit
paths is tracked separately, not part of #908). Do not read this property
as a complete loss-free guarantee — it closes the two most likely loss
paths (queue overflow and HTTP failure), not every path.

## Properties never allowed

Room name/slug, participant identity, window title, track sid, IP, DSN,
tokens, file paths, SDP, PCM, pixels, device names/ids/serials, display
ids, key codes/glyphs, clipboard contents, pointer coordinates, wheel
deltas.

## The events

| Event | Extra properties | Fires when |
|---|---|---|
| `meeting_joined` | — | Room connect succeeded |
| `meeting_left` | `duration_bucket`, `reconnect_count_bucket` | Leave or disconnect |
| `join_failed` | `reason`: `network` \| `no_backend` \| `token` \| `timeout` | Join did not connect. `no_backend` is native-only (a build with no baked `PETAL_BACKEND_URL`); the web classifier only yields `timeout` \| `token` \| `network`. |
| `share_started` | `source`: `window` \| `display` \| `picker` | A share published |
| `share_stopped` | `reason`: `user` \| `window_gone` \| `capture_failed` | A share ended |
| `remote_audio_silent` | `duration_bucket` | Remote-track watchdog `EnteredAlarm` (#787 class) |
| `remote_video_stalled` | `source`: `stats` \| `gallery` \| `native`; native also sends `sharer_kind`: `native` \| `web` \| `unknown` | Video stall watchdog, after debounce, not per flap. No `duration_bucket`: the emit site has no real duration, and a hardcoded `0_10s` was a fabricated dimension. |
| `capture_restarted` | `outcome`: `recovered` \| `failed` | In-place capture restart finished. Native only today (ScreenCaptureKit). Web has no in-place restart; the function exists for lockstep and has no production call site. |
| `reconnect` | `outcome`: `recovered` \| `failed` | LiveKit reconnect finished (not each attempt) |
| `permission_denied` | `kind`: `screen` \| `mic` \| `camera` | User denied or lost a capture/mic/camera grant. Web screen-picker dismiss (`getDisplayMedia` `NotAllowedError`) is not this event. |
| `remote_control_input` | `kind`: `click` \| `type` \| `paste` \| `scroll` | Host applied a coalesced remote-control input (see below). Web cannot inject OS input; the coalescer is tested but the host emulator must not emit. |
| `device_changed` | `kind`: `display` \| `camera` \| `mic`, `change`: `switched` \| `failed` \| `reconfigured` \| `sleep` \| `wake` | Display / webcam / mic actually changed (see below) |
| `remote_control_latency_summary` | see [below](#remote_control_latency_summary) | One per participant per 60 s of active remote control, plus a final partial on stop when any stage has at least 5 samples. Capped at 120 per control session. Never per input (#370). |
| `annotation_toggled` | `state`: `on` \| `off` | #872. `draw_active` is the ONLY thing that makes the sharer overlay capture the cursor. Nothing recorded it, so a report of "I cannot click on buttons in my apps" was undiagnosable from telemetry. Emitted on a real state change only. No strokes, coordinates, or window titles. Native only today: the web client's `EVENT_NAMES` has the other twelve and does not emit this one. |

No new event without an explicit add to this table.

### `remote_control_latency_summary`

Native: `rc_latency_telemetry.rs` (fed today by the host replay stages).
Web: `shared/logic/rcLatencyAggregator.ts`, driven by the controller grant in
`web-harness/src/remoteControlUi.ts`. The schema is pinned by
`contracts/petal-contracts.json` → `remoteControlLatencySummary`; tracking
issue #369 says which phase feeds which stage. The same fields go to
`petal.log` as one `remote-control-latency-summary:` line per interval.

| Property | Type | Meaning |
|---|---|---|
| `role` | `host` \| `controller` | Which side measured it |
| `rc_session_id` | 32 hex chars | Random per control session, minted locally. Never derived from room, identity, grant token or `controlSessionId`. |
| `interval_s` | integer | Seconds this summary covers (60, or less for the final partial) |
| `final` | boolean | The partial emitted when control ended |
| `<stage>_p50`, `<stage>_p95`, `<stage>_max`, `<stage>_n` | integer ms / count | Per stage: `input_leg_ms`, `input_rtt_ms`, `input_excess_delay_ms`, `replay_inject_ms`, `replay_complete_ms`, `inject_to_capture_ms`, `capture_interval_ms`, `capture_to_encoded_ms`, `encoded_to_packetized_ms`, `pub_rtt_ms`, `sub_rtt_ms`, `video_net_ms`, `jb_delay_ms`, `jb_target_ms`, `decode_ms`, `processing_ms`, `receive_to_present_ms`, `photon_ms`. A stage with no samples is omitted, never sent as 0. |
| `input_stall_count`, `frames_dropped`, `freeze_count`, `nack_count`, `pli_count` | integer | Interval sums |
| `packets_lost_pct_x100`, `offset_err_ms` | integer | Interval maximum |
| `lk_region` | token | SFU edge region (coarse, not the user's location) |
| `same_edge`, `self_hosted_sfu` | `true` \| `false` \| `unknown` | |
| `ice_candidate_type` | `host` \| `srflx` \| `prflx` \| `relay` \| `unknown` | |
| `ice_protocol` | `udp` \| `tcp` \| `unknown` | |
| `window_codec` | `h264` \| `h265` \| `vp8` \| `vp9` \| `av1` \| `unknown` | |
| `content_change` | `detected` \| `unknown` | |

Counters and dimensions appear only once a producer reports them. Histograms
are fixed log-spaced buckets (about 5%) over 0-5000 ms; percentiles are
nearest-rank. Every send passes the single consent check
(`analytics.rs` `usage_stats_consented`, `analytics.ts`
`usageStatsConsented`), which delegates to #377's `usage_stats_enabled()` /
`usageStatsEnabled()` once that lands.

Sentry gets one closed-tag diagnostic, `remote-control-latency-degraded`
(native only), when an interval has `photon_ms_p95 > 400` or
`input_stall_count >= 5`. Do not map
`log::error!` onto these; emit from the same call sites that today `warn!`
the watchdog, once per transition, with the buckets above.

### `remote_control_input`

Emit on the **host** when input is applied or submitted, not on the
controller send path (avoids double-count). Never pointer-`move`, never
separate `down`/`up`, never key names.

| `kind` | Coalesce | Maps from |
|---|---|---|
| `click` | Once per discrete click (`pointer` `click`, or a completed down+up). Not every mouse-down. | `petal.remote-control` `pointer` |
| `type` | Once per typing burst (idle ≥1s). A held-key repeat is still one burst. | `key` packets |
| `paste` | Once per paste. | `text` paste packets (#375) |
| `scroll` | Once per wheel burst (idle ≥500ms). Not per wheel tick. | `wheel` packets |

### `device_changed`

Once per real transition, in a meeting. Never device names. Display
reconfiguration is already bursty (`CGDisplayRegisterReconfigurationCallback`)
— coalesce like the network-change 1s debounce.

| `kind` | `change` | Call site today |
|---|---|---|
| `display` | `reconfigured` | Display hot-plug / resolution (`resilience` display monitor) |
| `display` | `sleep` / `wake` | `screensDidSleep` / `screensDidWake` |
| `camera` | `switched` | `set_camera_device` applied a different webcam |
| `camera` | `failed` | Selected / default webcam could not start |
| `mic` | `switched` | `MicDeviceChanged` (hot-plug or picker) |
| `mic` | `failed` | `MicDeviceFailed` |

Join-path hard fails (mic/speaker never came up) stay Sentry `error!` —
those are “this code path broke,” not a rate. Panic and ObjC exceptions
stay Sentry. The two structured diagnostic classes
(`capture-layout-invalid`, `camera-health`) stay the rate-limited Sentry
diagnostics they already are.
