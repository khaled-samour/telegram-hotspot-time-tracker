# Telegram Hotspot Time Tracker

A Python tool for monitoring phone-hotspot device reachability, estimating connection time, and viewing time-based cost reports through an Arabic Telegram interface.

Designed around an Android/Termux environment, the script combines local network probes, SQLite session records, and a Telegram control panel. Tracking, storage, and cost calculations run locally on the phone; no Cloudflare Worker or separately hosted backend is required. Telegram remains an external service, so bot interaction and alert delivery need internet access. It is a monitoring and accounting tool, not a router-control or access-enforcement system.

## Real-world use

The author reports that he has tested the project and actively uses it on his phone, including observing stored data survive an unexpected process stop. This is his operational experience on his own setup, not a guarantee of compatibility with every Android device or immunity to storage failure.

A separate review of this supplied source ran all 35 built-in local checks successfully. That review did not independently operate his phone, probe his hotspot, or connect the bot to Telegram.

## What it does

- Registers friendly device names against the final octet of an IPv4 address.
- Tracks observed sessions and records connection/disconnection events.
- Shows individual reports, an all-device report, currently observed devices, and recent session history.
- Calculates an estimated amount from recorded time. The current source uses **four hours per shekel**.
- Supports per-device counter resets without clearing other devices' counters.
- Alerts on unregistered responding addresses and offers an inline registration button.
- Provides normal, silent, and off modes for ordinary alerts. Unregistered-device alerts remain audible in every mode.
- Queues alerts locally when Telegram delivery fails and retries them in order.
- Recovers stored sessions after restart and records certain monitoring gaps.
- Includes a built-in local self-test suite.

## Scope

| Implemented | Not implemented |
| --- | --- |
| Reachability checks using ping and a limited TCP fallback | Guaranteed detection of every connected client |
| Time estimates based on observations | Exact Wi-Fi association/disassociation timestamps |
| Estimated time-based charges | Payments, invoicing, or collection |
| Device labels linked to IPv4 host numbers | MAC-address identity or automatic device authentication |
| Telegram reporting and alert controls | Blocking clients, traffic shaping, or bandwidth quotas |
| Recognition of selected hotspot interfaces | Turning the hotspot on/off or changing its password |

The script does not measure transferred data, inspect browsing activity, or enforce payment before network access.

## Architecture

```text
Hotspot interface detection
          |
          v
Local reachability probes ---> Session tracker ---> SQLite
                                      |                |
                                      v                v
                              Event generation    Report builders
                                      |                |
                                      v                v
                                Alert outbox <--> Telegram interface
```

The source separates the main responsibilities:

- `Store`: SQLite tables, metadata, retained probe audit, and outgoing alert queue.
- `Tracker`: session accounting, missed-probe handling, counter baselines, and restart recovery.
- Network functions: hotspot-interface detection and device probing.
- Report builders: Arabic duration, estimated-cost, online-device, and history views.
- `run_bot()`: Telegram commands, inline buttons, alert delivery, and background workers.
- `selftest()`: simulated accounting, reporting, persistence, and queue checks.

SQLite uses write-ahead logging and an application-level reentrant lock. This supports the script's shared database access; it is not a guarantee against all storage or power-loss failures.

## Requirements

- Python 3 with SQLite support.
- The `pyTelegramBotAPI` package, imported by the source as `telebot`.
- Executable `ifconfig` and `ping` commands with output/options compatible with the script.
- A phone-hotspot interface visible to the Python process.
- Local network access to hotspot clients.
- Internet access for Telegram commands and message delivery.
- A Telegram bot token and the numeric ID of the administrator's **private chat** with that bot.

The intended environment is Android with Termux, but compatibility depends on the device, Android version, interface naming, permissions, and network behavior. The source does not invoke root or privileged tethering-control commands. That does not establish that every supported phone permits the required network inspection without additional configuration.

### Network assumptions

`detect_hotspot()` checks only interfaces named `ap0`, `swlan0`, and `wlan1`, in that order. It expects a particular `ifconfig` layout with an IPv4 address following an interface header.

The scanner uses the first three address octets as a prefix and checks host numbers `1` through `254`, excluding the phone itself. This is a `/24`-style assumption, not subnet-mask-aware discovery. Other interface names, output formats, and subnet layouts are not handled automatically.

Registered devices should retain stable IPv4 host numbers through a reservation or other suitable network configuration. This script does not assign or reserve those addresses.

## Setup

The examples below use the author's operational filename, `tracker4.py`. Save the supplied Python source under that filename, or substitute your chosen filename in the commands. Renaming the file does not change the source.

### 1. Install the Python dependency

```sh
python -m pip install pyTelegramBotAPI
```

This is the only third-party Python package directly imported by the application. A compatible package version has not been pinned or verified through a live Telegram run.

Ensure `ifconfig` and `ping` are also available in the intended environment before starting the monitor.

### 2. Configure the bot

Set these environment variables locally:

| Variable | Purpose |
| --- | --- |
| `BOT_TOKEN` | Telegram bot token |
| `ADMIN_CHAT_ID` | Numeric ID of the administrator's private chat with the bot |

The author keeps his local environment configuration in `~/.hotspot_env`. The Python source does **not** read that file automatically: its values must be exported into the process environment by the launching shell or another local launcher. Keep that file private and out of source control.

The script's defaults are placeholders. It exits if either value still contains its placeholder marker, and `ADMIN_CHAT_ID` must be convertible to an integer.

Keep the real token out of source control, screenshots, shared terminal output, and documentation. Configure it on the device rather than replacing the published placeholder with a real credential.

### 3. Run on the hotspot device

```sh
python tracker4.py
```

The monitor must run where it can inspect the phone's hotspot interface and reach its clients. Running it on an unrelated hosted server does not provide access to the phone's local hotspot network.

Open the bot's private chat and send `/start`.

Starting the application starts its network-monitoring worker and Telegram polling. Use it only on networks and devices you own or are authorized to monitor. Keep the process running for continued observations; time while it is stopped cannot be reconstructed as actual device usage.

## Telegram interface

The existing user-facing text is in Arabic.

| Command | Action |
| --- | --- |
| `/start` or `/menu` | Open the control panel |
| `/report` | Choose a registered device or open the all-device report |
| `/online` | Show devices with open observed sessions, including unregistered addresses |
| `/log` | Show recent sessions and monitoring gaps since the global reporting baseline |
| `/add 205 DeviceName` | Register or update the label for host number `205` |
| `/rename 205 NewName` | Alias of `/add` |
| `/del 205` | Remove the registered label; historical records remain |

The address parser accepts a host number or takes the final dot-separated part of an input. It does not validate a supplied full address against the active network prefix.

Inline buttons provide report refresh, navigation, per-device reset confirmation, alert-mode cycling, and registration of an unregistered responding address.

There is an internal global-reset method, but the current Telegram interface exposes only **per-device** counter resets. A reset advances the reporting baseline; it does not delete old sessions.

## Default configuration

These are source constants, not additional environment variables:

| Setting | Default | Meaning |
| --- | --- | --- |
| `DB_PATH` | `~/hotspot.db` | SQLite database location |
| `SCAN_EVERY` | 20 seconds | Target interval for registered/open-session scans |
| `FULL_EVERY` | 300 seconds | Target interval for full host-range discovery |
| `MISS_LIMIT` | 3 | Consecutive failed observations before closing an open session |
| `PING_TIMEOUT` | 2 seconds | Timeout passed to the ping command |
| `WORKERS` | 8 | Concurrent probe workers |
| `SEC_PER_SHEKEL` | 14,400 seconds | Four observed hours per shekel |
| `AUDIT_DAYS` | 3 days | Retention target for raw probe audit records |
| `MIN_RESTART_GAP` | 15 seconds | Threshold used when recording a restart gap |

The outbox worker attempts delivery every five seconds. Delayed alerts older than one minute receive an event-time annotation. Routine pruning removes queued alerts older than two days. Probe-audit and outbox pruning is checked roughly hourly during normal scanning. Sessions, registered names, metadata, and recorded gaps do not have equivalent automatic retention limits in this source.

The history view shows up to 15 recent sessions and gaps since the global reporting baseline. Report warnings appear when a recorded gap exceeds one minute.

Scanning intervals are targets, not real-time deadlines. Slow probes and a full host-range scan can extend a cycle beyond 20 seconds. The startup schedule initially aims to defer full discovery by about 40 seconds, but a newly detected or changed network prefix overrides that delay and triggers a full scan immediately. Eight thread-pool workers limit concurrent probes; they do not guarantee that Android will keep the process alive.

## How accounting works

1. A successful observation opens a session or updates its `last_seen` timestamp.
2. Short runs of failed probes do not immediately close that session.
3. After three consecutive failed observations, the session closes at its **last successful observation**, not at the later moment disconnection is declared.
4. Reports sum recorded session intervals, clipped to the relevant reporting baseline and report time.
5. A device-specific reset advances only that device's baseline. Existing sessions remain stored.
6. Estimated cost is recorded seconds divided by `SEC_PER_SHEKEL`.

Brief missed-observation intervals are included when a later successful observation continues the same session. New devices may remain undetected until a full scan, and sleeping or filtered devices may be missed. These are estimates, not authoritative billing records. A fixed plus-or-minus-20-second accuracy bound and an always-undercharging guarantee are not established: actual scan duration varies, and a short real disconnection can be included in a continuing session.

After restart, open stored sessions are closed and a sufficiently long heartbeat gap can be recorded. Gap handling describes monitor availability, not proven client connectivity. Some short interruptions and network-detection failures are not represented as reliable gap measurements.

## Storage, privacy, and access

The local database contains device labels, IPv4 host numbers, session times, probe results, reporting baselines, monitoring gaps, and queued notification text. Telegram messages can disclose device names, connection times, estimated charges, and unregistered IP addresses to the configured chat.

Do not publish `hotspot.db`, its `-wal` or `-shm` sidecars, database backups, real configuration values, or operational logs containing private information.

The Telegram command and callback filters compare the **chat ID**, not a separate allowlist of human sender IDs. Use the administrator's private chat. Pointing `ADMIN_CHAT_ID` at a group does not restrict control to one person within that group.

Protect both the bot token and the device running the script. Use monitoring transparently and with appropriate permission from the people whose device activity is recorded.

## Self-test

```sh
python tracker4.py --selftest
```

The self-test uses simulated observations, in-memory SQLite databases, and a temporary on-disk database. It does not start Telegram polling or the hotspot scanner and does not require `pyTelegramBotAPI` to exercise this path.

It covers session duration, missed probes, session closure, unregistered-device events, reporting baselines, restart recovery, gap reports, SQLite persistence, a synthetic multi-device workload, alert-queue retry behavior, per-device resets, alert policy, and duration formatting.

**Review result:** all 35 built-in checks passed in an isolated local run. The author separately reports testing and actively using the application on his phone. The review's local test does not independently verify Android interface detection, real hotspot clients, Telegram integration, delivery during actual outages, or real-world timing accuracy. The built-in on-disk persistence check reopens a database; it is not a forced-crash or power-loss test.

## Known limitations

- IPv4 host numbers are identifiers, not proof of device identity. Address reuse can attribute later activity to the wrong label.
- Unregistered-address discovery relies on ping. A TCP connection/refusal check on port 80 is used only for registered addresses or addresses with an already-open session.
- Reachability is not the same as hotspot association or active internet use. Sleeping clients, firewalls, routing, and isolation can affect observations.
- The application provides no access blocking, bandwidth enforcement, data-volume metering, or payment processing.
- Ordinary alert suppression does not suppress unregistered-device alerts. Queued messages retain the delivery mode selected when they were queued.
- Large reports are not automatically split to fit Telegram message limits.
- Dependency versions and device compatibility are not pinned or established by the local tests.
- It is a single-process application. The author reports active use on his setup; compatibility, background-process survival, concurrency behavior, and end-to-end reliability on other devices require separate testing.

## Dependency reference

- [pyTelegramBotAPI installation guide](https://pytba.readthedocs.io/en/latest/install.html)

This documentation describes the supplied source, the author's reported operational experience, and the separate local checks performed during review. It does not claim universal compatibility or production-readiness certification.
