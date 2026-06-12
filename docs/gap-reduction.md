# Gap-Reduction Plan — D Slippage & Front-Running

**Owner:** 1chimaruGin
**Date:** 2026-06-08
**Status:** Planning — execution items prioritized for ≤$200/mo infra budget

---

## Current State (Baseline)

- **Median entry slippage from D:** +42% over D's price
- **Public mempool coverage:** 64% of D's BUYs (we see in pending pool, land N+1)
- **Private mempool coverage:** 0% of D's remaining 36% (D submits to private relay → we miss entirely)
- **Internal latency (post-optimization):** ~8ms prep + 35ms submit_rtt = ~43ms total
- **Submission target:** `https://bsc.blockrazor.xyz` (single endpoint)
- **Detection source:** local geth via WSS

---

## Full Lever Inventory

### A. Detection latency (see D's pending tx faster)

| # | Lever | Effort | Cost/mo | Saved |
|---|---|---|---|---|
| **A1** | Co-locate VM near BlockRazor PoP | 2 hrs | **~$50** | 20-30ms RTT, more same-block landings |
| **A2** | BlockRazor PRIVATE mempool subscription | ~100 LOC + paid | **$200-500** | Part of the 36% private band |
| A3 | bloXroute private feed | ~100 LOC + paid | $1000+ | Broadest private coverage |
| A4 | 48 Club private feed (BSC validator alliance) | ~100 LOC + paid | Variable / unknown | BSC memecoin community standard |
| A5 | Peer local geth with validators directly | Negotiation + config | Free if contacts | Faster public propagation |
| A6 | Dedicated bare-metal geth (NVMe) | Hardware swap | ~$200 | Marginal — already ~5ms detect_ms |

### B. Decision latency (internal — all free, code only)

| # | Lever | Effort | Saved | Status |
|---|---|---|---|---|
| B1 | Wallet-balance cache (eliminate eth_getBalance) | done | ~5-15ms | ✅ shipped |
| B2 | V2 pair cache (eliminate F4 eth_call) | done | ~5-10ms | ✅ shipped |
| **B3** | Pre-sign skeleton tx (sign at boot, patch value+broadcast at detect) | ~80 LOC | ~2-3ms | open |
| B4 | Compile-time hard-code D whitelist | ~10 LOC | <1ms | marginal |
| **B5** | Parallelize gate + sign with `tokio::join!` | ~30 LOC | ~2-3ms | open |

### C. Submission latency (BlockRazor RTT — all free, code only)

| # | Lever | Effort | Saved |
|---|---|---|---|
| C1 | HTTP keepalive + warmer | done | 20-40ms cold-start ✅ shipped |
| **C2** | Race-submit to BlockRazor + local geth in parallel | ~80 LOC | 5-20ms tail latency |
| C3 | Add 48 Club / bloXroute as 2nd submission endpoint | ~120 LOC + paid | First-to-land wins |
| C4 | HTTP/2 prior knowledge (skip protocol negotiation) | ~5 LOC | ~5ms first request |
| C5 | QUIC/HTTP3 transport (if BlockRazor supports) | ~50 LOC | ~10ms |
| C6 | Direct TCP submission (skip JSON-RPC encoding) | ~150 LOC | ~5-10ms |

### D. Block inclusion priority (when D and we both bid)

| # | Lever | Effort | Cost | Effect |
|---|---|---|---|---|
| **D1** | Dynamic gas matching D's tier (10/30/50/92 gwei mirror) | ~50 LOC | +$2-5/trade | Higher tx_index priority |
| **D2** | Bundle backrun via BlockRazor MEV-Boost — atomic `[D's tx, our tx]` | ~250 LOC | Free (BlockRazor bundle endpoint) | **Eliminates entry slippage entirely on bundled signals** |
| D3 | Pay validator for inclusion (MEV-Share BSC style) | Relationship | Paid | Direct ordering control |
| D4 | Run our own block builder | Weeks | Significant | Full ordering control |

### E. Private-mempool access (the 36% we currently can't see)

| # | Lever | Effort | Cost/mo | Captures |
|---|---|---|---|---|
| **E1** | BlockRazor private mempool API | ~100 LOC | $200-500 | Chunk of 36% |
| E2 | bloXroute private feed | ~100 LOC | $1000+ | Broader |
| E3 | 48 Club private feed (BSC-specific) | ~100 LOC | Variable | BSC standard |
| E4 | Multi-builder bundle subscriptions | ~200 LOC | Variable | Multiple coverage |

*(E1 and A2 are the same product viewed from different angles.)*

### F. Front-running / pre-detection (exotic edge — all code work)

| # | Lever | Effort | Feasibility |
|---|---|---|---|
| **F1** | D's `approve(token)` pre-buy detector → pre-arm execution | ~100 LOC | High signal — D's behavior observable |
| **F2** | D's funding-wallet pattern (BNB transfers to known buying wallets) | ~100 LOC | Medium signal |
| **F3** | D's gas-escalation early-warning (>50 gwei from D = warm execution path) | ~30 LOC | Small but free |
| F4 | D's GMGN proxy finer tracking | ~30 LOC | Marginal — already covered |
| F5 | D's social signals (Twitter/Telegram NLP) | Hard, fragile | Possible but unreliable |
| **F6** | Token-deployer monitoring — pre-D snipe on whitelisted devs | ~150 LOC | Real edge (already paper-traded) |
| F7 | D's wallet behavioral ML model | Weeks | High-risk |
| F8 | Mirror-service back-door access | API needed | Likely impossible |
| F9 | D's RPC endpoint network sniffing | Borderline illegal | Skip |

### G. Skip-the-bad-trades (defensive — all free)

| # | Lever | Effort | Effect |
|---|---|---|---|
| G1 | Pre-trade gap probe — skip when price > D × 1.25 | ~50 LOC | Save gas on guaranteed bad entries |
| G2 | Skip D BUYs <0.1 BNB | done | ✅ shipped |
| G3 | Multi-KOL confirmation gate (2+ KOLs) | ~100 LOC | Reduces volume, improves quality |
| G4 | Token-age gate | partial | ✅ partial in pick_route |

---

## Budget-Constrained Plan (≤$200/mo infra)

### Tier 1 — Ship First (free code work, real impact)

These are pure-code wins with no monthly cost. Cumulative latency savings target: **10-25ms shaved off the hot path; F1/F6 attack the 36% blind band**.

| Priority | Item | Effort | Why first |
|---|---|---|---|
| 1 | **C2** Race-submit (BlockRazor + local geth in parallel) | ~80 LOC | Tail-latency killer; first-to-land takes the slot |
| 2 | **D1** Dynamic gas matching D's tier | ~50 LOC | Higher tx_index when D bids high — better same-block ordering |
| 3 | **F1** D's approve-before-buy detector | ~100 LOC | Pre-arms execution on a known pre-buy signal |
| 4 | **F3** D gas-escalation early-warning | ~30 LOC | Trivial; warms the hot path early |
| 5 | **B3** Pre-sign skeleton tx | ~80 LOC | ~2-3ms saved on every submit |
| 6 | **B5** Parallel gate+sign | ~30 LOC | ~2-3ms saved |
| 7 | **F2** D funding-wallet detector | ~100 LOC | Catches some pre-buy preparation |
| 8 | **F6** Token-deployer pre-D snipe (10 dev whitelist) | ~150 LOC | Already validated in paper; real front-run edge |
| 9 | **G1** Pre-trade gap probe (skip >1.25× D) | ~50 LOC | Saves gas on losers |
| 10 | **C4** HTTP/2 prior knowledge | ~5 LOC | Tiny but trivial |

**Total: ~675 LOC, $0/mo, 10-25ms latency saved + opens front-running attack surface on F1/F2/F3/F6.**

### Tier 2 — Spend the $200 infra budget

Two viable allocations of the $200/mo:

**Option A: Latency-first (recommended for current setup)**

| Item | Cost | Effect |
|---|---|---|
| **A1** Co-locate VM near BlockRazor PoP | ~$50/mo | 20-30ms RTT saved — biggest single latency lever |
| Headroom for trade gas / scale-up | ~$150/mo | Unused, reserve for trade volume |
| **Total** | **$50/mo** | Frees budget for trade size scaling |

**Option B: Coverage-first (if Tier 1 ships and we still have +42% gap)**

| Item | Cost | Effect |
|---|---|---|
| **A1** Co-locate VM | ~$50/mo | 20-30ms RTT |
| **E1/A2** BlockRazor private mempool (basic tier) | $150/mo target | Captures part of the 36% blind band |
| **Total** | **$200/mo** | Latency + partial private visibility |

**Recommendation:** Ship Tier 1 entirely, then start with Option A (just A1, $50/mo). Measure: does median slippage drop below +30%? If yes, we don't need E1 yet. If still +35%+, escalate to Option B and pay for private mempool access.

### Out of budget (parked)

These exceed $200/mo or are negotiation-gated. Track only:

- **A3** bloXroute private feed ($1000+/mo)
- **A4** 48 Club (need to verify pricing — could fit budget if available)
- **A6** Dedicated bare-metal geth (~$200/mo, eats whole budget for marginal gain)
- **D3** Direct validator deals (relationship-based)
- **D4** Run own block builder (weeks of work + infra)
- **F5** Social NLP monitoring (high effort, low reliability)
- **F7** ML-based D-prediction (weeks, generalization risk)

### Hard NO (off the table)

- **F8** Mirror-service back-door
- **F9** Network sniffing of D's RPC endpoint

---

## Measurement Plan

Track after each Tier-1 ship:

1. `submit_rtt_ms` distribution (already instrumented) — verify C2/C4 land savings
2. Median entry slippage vs D (re-run `scripts/analyze-real-trade.py` on next 20 trades)
3. Same-block landing rate (currently N+1 typical) — should rise after C2 + D1
4. Pre-D entries via F1/F2/F3 — count per day
5. F6 paper-mirror vs live — track separately to validate front-run edge before scaling

**Decision gate after 50 trades on Tier 1 + A1:**
- Slippage <+30%: hold here, scale trade size
- Slippage +30-40%: ship F6 if not yet
- Slippage still +40%+: escalate to Option B (E1 private mempool)

---

## Next Action

Start with **C2 (race-submit)** — biggest single Tier-1 win, no dependencies, ~80 LOC. Confirm when you want it scoped.
