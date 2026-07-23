# Changelog

## 2026-07-23 (83)

### Follow-token: GMGN bundler second-group buy trigger

- After **Shared FeePayer Locked**, follow-token normal mode polls GMGN bundler traders every **2s** for up to **60s** from token CREATE (`fetchBundlerTraders`, limit 50).
- Bundlers are grouped by `start_holding_at` with **±2s** tolerance. The first group must include all **4** initial bundlers; a **second** group (1–4 wallets, still within the first minute from CREATE) triggers buy immediately and stops polling.
- Round-group and cumulative-dust buy gates are **disabled** for follow-token (feePayer watch continues).
- Buy exit: **+90% MC**; stop-loss remains **-50% P/L**.

## 2026-07-23 (82)

### Follow-wallet: respect manual pause across token flow resets

- **Pause Follow-Wallet** now sets a persistent in-bot `followWalletPaused` flag.
- Auto-resume after `resetForNewToken` / `completeFlowCycle` (follow-token skip, rug, sell complete, etc.) is skipped while paused.
- **Resume Follow-Wallet** clears the flag; startup auto-resume also skips when paused.

## 2026-07-23 (81)

### Follow-token: dev CREATE count 1–3

- Follow-token migration filter accepts dev wallets with **1, 2, or 3** CREATE txs in Helius history (was exactly **1**). **0** or **4+** still rejected.

## 2026-07-23 (80)

### Follow-token: parallel feePayer-funder watch (≤6h)

- When **follow-token** locks a shared feePayer in **normal mode**, Helius **funded-by** is checked.
- If the feePayer was funded **within 6 hours**, the bot also subscribes to that **funder wallet** in parallel.
- Parallel funder uses the same round-group / dust-race / recipient-watch logic as the primary feePayer (shared state).
- Parallel wallet does **not** trigger **>100 SOL** migration/handoff — only the primary feePayer watch migrates.
- **Shared FeePayer Locked** Telegram includes the parallel funder when active.
- Primary and parallel watches dedupe by address: handoff to the parallel funder absorbs the parallel subscription instead of opening a second connection.

## 2026-07-23 (79)

### Insider: startup handoff when shared feePayer is already at zero SOL

- On **Shared FeePayer Locked**, if live balance is **0**, scan recent feePayer txs (since earliest bundler funding) for the **latest transfer-out &gt; 100 SOL** that drained the wallet.
- Hand off monitoring to that recipient automatically (same migration path as live large-drain), up to **5** chained hops if intermediates are also empty.
- Skips handoff when the drain returned to the **original** feePayer or the recipient is also at zero.

## 2026-07-23 (78)

### Follow-token: reject 0 dev CREATE count again

- Migrated tokens with **0** dev CREATE txs in Helius history are skipped again (no bundler fallback). Filter requires **exactly 1** create.

## 2026-07-23 (77)

### Normal mode: -50% stop-loss; +180% take profit for ~0.1–0.5 SOL rounds

- Position stop-loss widened from **-40%** to **-50%** P/L vs entry MC.
- Round groups **~0.1 SOL through ~0.5 SOL** use **+180% MC** take profit; **~0.02 / ~0.05 SOL** stay at **+90%**.
- **$100 first-buy gate unchanged**: among the **first two unique round-group recipients**, buy only proceeds if at least one recipient’s first token buy exceeds **$100**.

## 2026-07-23 (76)

### Normal mode: expand round SOL sizes; tiered MC exit

- Round group sizes: **~0.02 / ~0.05** (&lt; $10 cap) and **~0.1–0.5 SOL** in **0.05 steps** (USD-exempt, may exceed $10).
- Exit bands: **+90% MC** for ~0.02 / ~0.05 SOL; **+180% MC** for ~0.1–0.5 SOL (see **2026-07-23 (77)** for stop-loss update).

## 2026-07-23 (75)

### Follow-token: resubscribe PumpPortal on skip/reset, not only after rug

- **`startFromFollowTokenMigration`** now returns **`true` only when the bundler-funder flow is still active** after startup (fixes PumpPortal staying suspended when startup skip/reset fired `tokenFlowEnded` before unsubscribe).
- PumpPortal **resubscribes on `tokenFlowEnded`** (`reset` or `cycle_complete`) once **`resetForNewToken` / `completeFlowCycle`** tear down feePayer, recipients, and other token watches — no need to wait for on-chain dev rug while the bot has already skipped/reset.

## 2026-07-23 (74)

### Follow-token: revert early-filter fail logs; accept 0 dev CREATE count with bundlers

- Filters 1–2 failures back to **debug**; **info** only after pump + metadata URI pass.
- Dev CREATE history accepts **0 or 1** mint(s); **>1** still rejected. When count is **0**, token continues only if first-four SWAP bundlers validate.

## 2026-07-23 (73)

### Follow-token: always log filters 1–2 at info (pass or fail)

- **Filter 1** (mint suffix) and **filter 2** (Helius DAS metadata URI) now emit **`info`** logs on both pass and fail; later filters unchanged.

## 2026-07-23 (72)

### Follow-token: metadata URI via Helius DAS getAsset

- Replaced Metaplex metadata PDA + RPC `getAccountInfo` with **Helius DAS `getAsset(mint)`** (`content.json_uri`) for the ipfs.io/baf metadata filter.
- Retry/debug logs now refer to DAS indexing lag instead of metadata account lookup.

## 2026-07-23 (71)

### Normal mode: round/dust buy threshold 17 → 15 txs

- `BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY` lowered from **17** to **15** (same 10s window; cumulative dust skip uses the same threshold).

## 2026-07-23 (70)

### Follow-token: tiered backend logs + unsubscribe PumpPortal during active flow

- Backend **info** logs for a migration start only after **pump suffix + metadata URI** pass; earlier skips stay **debug**.
- **PumpPortal migration feed unsubscribes** (`unsubscribeMigration` + WebSocket disconnect, no reconnect) when follow-token bundler-funder flow starts; **resubscribes** (`connect` + `subscribeMigration`) on `tokenFlowEnded` for that follow-token mint (rug/reset or cycle complete).
- Removed **`INSIDER_FOLLOW_TOKEN_VERBOSE_LOGS`** (behavior is now fixed, not env-gated).

## 2026-07-23 (69)

### Follow-token: Metaplex metadata URI filter (`https://ipfs.io/ipfs/baf…`)

- New core filter after **mint ends in pump**: derive Metaplex metadata PDA, read URI via Helius RPC `getAccountInfo`, normalize to **ipfs.io**, require prefix **`https://ipfs.io/ipfs/baf`** before CREATE-tx / migrate-age checks.
- Retries metadata lookup on indexing lag (4s / 8s) when the metadata account is missing; wrong URI prefix fails immediately.
- Added **`token-metaplex-metadata.ts`** (PDA + on-chain decode — same flow as `findMetadataPda` + `fetchMetadata`, no extra npm deps).

## 2026-07-23 (68)

### Follow-token: retry Helius mint CREATE lookup on indexing lag

- When **`mint create transaction not found`**, retries **`getMintCreateTransaction`** after **4s** and **8s** (same delays as first-four bundler fetch) before skipping the migration.

## 2026-07-23 (67)

### Follow-token: verbose backend logs for every PumpPortal migration

- New **`INSIDER_FOLLOW_TOKEN_VERBOSE_LOGS`** (default **`true`**) — `[FOLLOW-TOKEN]` info logs for every migration received, evaluation start, duplicate skips, core filter failures (with reason), and bundler indexing retries.
- When **`false`**, core/bundler skip details stay at **debug** (prior behavior); passes/failures at info/warn unchanged.

## 2026-07-23 (66)

### Follow-token: revert dev create count to Helius CREATE history

- Restored **`HeliusClient.countDevCreatedTokenMints`** (dev fee-payer CREATE txs, first N results) for the “exactly 1 create” filter.
- Removed Bitquery dependency from follow-token start — no **`BITQUERY_*`** env vars required.
- Deleted **`bitquery-client.ts`**.

## 2026-07-23 (65)

### Follow-token: Bitquery OAuth client-credentials + startup auth check

- **`BITQUERY_CLIENT_ID`** + **`BITQUERY_CLIENT_SECRET`** — auto-fetch/refresh V2 tokens from `https://oauth2.bitquery.io/oauth2/token` (avoids expired 12h IDE tokens on servers).
- Follow-token **startup** runs **`verifyAuth()`** — fails immediately with a clear error if Bitquery returns 403, instead of failing on the first migration.
- Bitquery HTTP GraphQL auth uses **`?token=ory_at_...`** on `https://streaming.bitquery.io/graphql` (same URL-token pattern as WSS); **`Authorization: Bearer`** is fallback only.
- Strips accidental quotes/`Bearer` prefix from `BITQUERY_ACCESS_TOKEN`.

## 2026-07-23 (64)

### Follow-wallet: verbose backend logs for testing

- New **`INSIDER_FOLLOW_WALLET_VERBOSE_LOGS`** (default **`true`**) — emits **`[FOLLOW-WALLET]`** info logs for monitoring start/pause/resume, buy detection, flow start, and every Enhanced WSS wallet tx (not just buys).
- Follow-wallet monitors use the **`FOLLOW-WALLET`** log label when verbose (easier to filter in backend logs). Set **`INSIDER_FOLLOW_WALLET_VERBOSE_LOGS=false`** when done testing.

## 2026-07-23 (63)

### Follow-token: Bitquery V2 auth + creator count query

- Bitquery client uses **`https://streaming.bitquery.io/graphql`** with **`Authorization: Bearer <BITQUERY_ACCESS_TOKEN>`** (V2 OAuth `ory_at_...`, not v1 API key).
- Strips accidental `Bearer ` prefix from env; clearer **403** hint when token/endpoint mismatch.
- Creator count query uses **`dataset: combined`** and Pump.fun program address filter for full historical create count.

## 2026-07-23 (62)

### Follow-token: Bitquery Pump.fun creator count

- Replaced Helius CREATE-tx history scan with **Bitquery** aggregate query (`Program.Name = pump`, `Method in [create, create_v2]`, `Transaction.Signer = dev`) — accurate total create count, not limited to first N txs.
- Requires **`BITQUERY_ACCESS_TOKEN`** in `.env` for follow-token start.
- Removed `HeliusClient.countDevCreatedTokenMints`.

## 2026-07-23 (61)

### Follow-token: PumpPortal migration WebSocket + pause/env controls

- Replaced Helius `parsedTransactionSubscribe` with **PumpPortal** `subscribeMigration` (`wss://pumpportal.fun/api/data?api-key=PUMPPORTAL_API_KEY`).
- New `pump-portal-ws.ts` client with reconnect + migration event parsing.
- **Pause Follow-Wallet** / **Resume Follow-Wallet** Telegram buttons (pause monitors only; active token flows unchanged).
- **Pause Funder-First** button (separate from Start).
- Env auto-start flags: **`INSIDER_FOLLOW_WALLET_ENABLED`**, **`INSIDER_FUNDER_FIRST_ENABLED`**, **`INSIDER_FOLLOW_TOKEN_ENABLED`** (follow-token TG alerts still require the last).
- General wallet monitors now pass shared Enhanced WSS for `transactionSubscribe` when `INSIDER_HELIUS_API_KEY` is set.

## 2026-07-23 (60)

### Follow-token: funded-by requires `Centralized Exchange`

- Dev funder filter now requires Helius funded-by `funderType` **`Centralized Exchange`** (case-insensitive), matching the Wallet API response (e.g. Bybit Hot Wallet). Removed legacy `exchange` / name-heuristic fallback.

## 2026-07-23 (59)

### Follow-token: tiered logging and Telegram gates

- **Backend info logs** include the token only after core migration filters pass (pump suffix, dev single create, migrate age, exchange funder). Earlier skips are **debug-only** without mint spam.
- **Telegram** token alerts (filters passed, handoff delayed/skipped) fire only after **first-four bundlers** confirm and only when **`INSIDER_FOLLOW_TOKEN_ENABLED=true`**. Start/stop listener TG unchanged.

## 2026-07-23 (58)

### Follow-token: Pump.fun migration listener

- New **follow-token** flow listens for Pump.fun **`migrate` / `migrate_v2`** on program `6EF8…` via Helius **`parsedTransactionSubscribe`**.
- Migration filters: mint ends **`pump`**, dev created **exactly 1** token, migrate within **`INSIDER_FOLLOW_TOKEN_MAX_MIGRATION_AGE_SEC`** (default 60s) of create, dev **funded by Centralized Exchange** (Helius `/v1/wallet/{wallet}/funded-by`, `funderType=Centralized Exchange`).
- After filters pass, validates **first-four unique SWAP buys** and starts the same **bundler-funder monitoring** as follow-wallet (`startFromFollowTokenMigration` — no follow wallet required).
- Telegram: **Start / Stop Follow-Token**; optional auto-start via `INSIDER_FOLLOW_TOKEN_ENABLED=true`.
- Added `getWalletFundedBy`, `countDevCreatedTokenMints` on `HeliusClient`; `pump-migrate-detector.ts`; `FollowTokenMigrationOrchestrator`.

## 2026-07-23 (57)

### Funder-first: handoff Telegram shows only the confirmed group events

- Fixed handoff message listing every historical fund to the four group wallets (e.g. 7 SOL then 8 SOL re-funds) — it now shows only the four **10s-window** events that formed the active watched group and spread is computed from those.

## 2026-07-22 (56)

### Funder-first: latest ≥100 SOL handoff when watched 4-in-10s group exists

- On potential feePayer drain (zero SOL), if that wallet had at least one watched **4-in-10s** bundler group (active, exhausted, subscribed recipients, or `normal_candidate`), zero-balance handoff uses the **latest** ≥100 SOL transfer-out since watch start — not the highest by amount.
- FeePayers with no watched group keep **highest** ≥100 SOL handoff unchanged.
- Telegram handoff titles reflect latest vs highest.

## 2026-07-22 (55)

### Funder-first: confirm watched group = mint first-four bundlers

- Handoff Telegram / info logs only when the **active watched 4-in-10s group** (recipients being monitored) **exactly matches** the mint's first-four initial SWAP buyers (slot-sorted Helius extract).
- Removed loose overlap against all feePayer funding history; mismatches are debug-only (no TG / info spam).
- Shared `extractFirstUniqueEarlyBundlerBuys` + `watchedGroupMatchesFirstFourBundlers` in `wallet-swap-detector.ts`.

## 2026-07-22 (54)

### Normal mode: round/dust buy threshold 20 → 17 txs

- `BUNDLER_FUNDER_NORMAL_TINY_MIN_ROUND_GROUP_TXS_FOR_BUY` lowered from **20** to **17** (same 10s window; cumulative dust skip uses the same threshold).

## 2026-07-22 (53)

### Normal mode: add ~0.15 / ~0.2 SOL round buy triggers

- Round group sizes expanded: **~0.02 / ~0.05 / ~0.1 / ~0.15 / ~0.2 SOL** (±0.004 SOL), same 10s / ≥17 txs / $100 first-buy gates.
- **~0.02 / ~0.05 / ~0.1** still require transfer-out **&lt; $10** USD.
- **~0.15 / ~0.2** are **exempt** from the $10 cap (may exceed $10 at current SOL price).
- **~0.15 / ~0.2 / ~0.1** use the +180% MC exit band; **~0.02 / ~0.05** stay at +90%.

## 2026-07-22 (52)

### Rug reset: MC below $3k

- Added `INSIDER_RUG_RESET_MARKET_CAP_USD = 3_000` — live MC poll triggers the same pre-buy reset / in-position sell path as dev CLOSE_ACCOUNT and dev zero-SOL rug signals.
- Pre-buy buy-gate rug check (`INSIDER_RUG_MARKET_CAP_USD = 5_000`) unchanged.

## 2026-07-21 (51)

### Fix funder-first WebSocket subscription exhaustion (1000 cap)

- **Cause**: each bundler recipient in an active 4-in-10s group opened its own `accountSubscribe` on the funder-first RPC WebSocket; with the **5 SOL** threshold many more groups formed and hit Helius’s **1000 subscriptions per connection** limit (`-32006`).
- **Fix**: removed per-recipient `accountSubscribe` — recipient zero-balance is detected via Enhanced WSS txs (`checkRecipientDrain`) only.
- Added **900** subscription budget guard + **100** concurrent potential-feePayer cap with idle-watch eviction.

## 2026-07-20 (50)

### Funder-first: 5 SOL bundler group threshold + silent until confirmed

- 4-in-10s recipient detection lowered from **≥20 SOL** to **≥5 SOL** post-balance (`NORMAL_MIN_POST_SOL`).
- Post-balance spread tolerance unchanged: **≤0.5 SOL** (`POST_BALANCE_TOLERANCE_SOL`).
- Valid groups are tracked silently — no Telegram or per-group backend info logs until a recipient buy overlaps the token's **first-four bundlers**; then one confirm log + handoff Telegram (with group details).
- Insider-bot normal-mode **funding** gate after handoff stays **≥20 SOL**.

## 2026-07-20 (49)

### Follow-wallet max raised to 4

- `MAX_FOLLOW_WALLETS` increased from **3** to **4** on bot 1.
- Startup env: `INSIDER_FOLLOW_WALLET_4` now loaded with `_1` / `_2` / `_3`.

## 2026-07-20 (48)

### Follow-wallet max raised to 3

- `MAX_FOLLOW_WALLETS` increased from **2** to **3** on bot 1 (Telegram add/remove, Enhanced WSS monitors, funder-first merge).
- Startup env: `INSIDER_FOLLOW_WALLET_3` now loaded with `INSIDER_FOLLOW_WALLET` / `_2` (config field already existed).

## 2026-07-20 (47)

### Telegram: follow-wallet add + funder-first stop button

- Plain-text wallet messages no longer add a follow wallet — only **Add follow wallet** → reply flow (or **Resume** when no wallets are configured).
- Removed **Stop Funder-First** from the home menu while funder-first is running (**Start** still shown if stopped).

## 2026-07-20 (46)

### Normal mode: Telegram for dust vs round race outcomes

- **Round wins**: when cumulative dust is ≥20 but a round 10s group already reached ≥20 txs first, sends **🏁 Round Group Won Race to 20** (one per token) instead of log-only.
- **Dust wins at round gate**: **⏭️ Cumulative Dust Before Round Buy** skip message now includes prior dust count and round group size (follow wallet when applicable).

## 2026-07-20 (45)

### Fix duplicate Telegram on round-group first-buy skip

- When a qualifying ~0.02 / ~0.05 / ~0.1 SOL round group failed the **$100** first-buy gate, `normalTinyRoundGroupFound` was only set on **pass**, so every later transfer-out in the same sync batch re-ran the gate and sent another identical skip message.
- Now the round group is **claimed once** (`normalTinyRoundGroupFound`) before the USD gate runs; pre-buy skip helpers stop feePayer discovery immediately (`discoveryStopped`) so only **one** Telegram is sent per token.

## 2026-07-19 (44)

### Follow-wallet vs funder-first normal-mode funding threshold

- **Follow-wallet**: normal-mode **funding** threshold lowered from **20 SOL** to **>3 SOL**. Buy signals unchanged — tiny round groups on transfer-outs **&lt; $10**, cumulative dust race-to-20, and the **$100** first-buy gate on the first two unique recipients.
- **Funder-first**: normal-mode **funding** threshold stays **≥20 SOL**. Uses the **same** tiny round-group buy path as follow-wallet (not the old large 20 SOL+ transfer-out → wait-for-recipient-buy path).

### Follow-wallet Telegram: show responsible follow wallet

- Follow-sourced skip, watch-start, and buy-gate messages now include **Follow wallet: `…`** (the wallet whose buy started that token flow), including:
  - Low-funding mode disabled / funding-below-threshold skips
  - **Shared FeePayer Locked** (feePayer watch armed after funding gate passes)
  - Normal round-group buy gate and dust / first-buy skip notices
- Omitted on funder-first messages (`flowSource !== "follow"`).

## 2026-07-19 (43)

### Normal mode: round-group buy requires selected recipient first buy &gt; $100

- Before a normal-mode ~0.02 / ~0.05 / ~0.1 SOL round buy, the bot picks the **first two unique recipients** in the 10s group (not the first two txs), syncs each wallet's history, and finds its **first buy on the current token**.
- Buy proceeds if **either** recipient's first buy exceeds **$100 USD**; the qualifying wallet triggers the buy. Otherwise the token is skipped and reset.

## 2026-07-19 (42)

### Funder-first: ignore feePayer-funder transfer-outs below 1 SOL

- `funder-first-orchestrator.ts`: outgoing SOL transfers from the configured feePayer funder below **1 SOL** are skipped and do not arm or top up a potential feePayer watch.

## 2026-07-18 (40)

### Normal mode: sub-$0.10 outs tracked as dust; round group wins the race to 20 txs

- `insider-bot.ts`: sub-$0.10 feePayer transfer-outs are recorded as dust (no longer ignored).
- **Race to 20**: dust skip uses **cumulative** dust count (≥20 total, any timing). Round buy still requires ≥20 same-size outs in a **10s window**. Skip only when cumulative dust hits 20 **before** a round 10s group does; round wins if its 10s group reaches 20 first.
- Removed the **>20 individual dust txs** skip and the **not-first-qualifying-group** round skip (small dust totals no longer block round buys).

## 2026-07-17 (39)

###  

### Follow-wallet: watch up to 2 wallets on bot 1

- Insider bot 1 can monitor **up to 2 follow wallets** concurrently (separate Enhanced WSS monitors). **Add follow wallet** on `/start` adds without replacing; **🗑 Remove** per wallet.
- Each wallet’s buy triggers its own backtrack flow (with idle-bot delegation unchanged). Funder-first merge checks **both** follow wallets.
- Optional env: `INSIDER_FOLLOW_WALLET` + `INSIDER_FOLLOW_WALLET_2` load as paused defaults on startup.

## 2026-07-17 (37)

### Funder-first: 100+ SOL zero-balance handoff chain

- Each potential feePayer tracks **≥100 SOL** native transfer-outs **only from when that watch episode started** (funder receive, fast-track, or zero-balance handoff subscribe time).
- On native SOL **→ zero**: unsubscribe the drained wallet; if a ≥100 SOL out was recorded in that window, **hand off** watch to the **highest** such recipient by amount (ties → most recent). Live balance check, fresh bundler pipeline. If that recipient is the **feePayer funder**, stop only — funder is already watched, no duplicate subscribe. If none (or recipient already zero), stop with no handoff.

## 2026-07-17 (36)

### Fast-track adds feePayers; menu remove buttons

- **Fast-track** now **adds** to the watch list without resetting other feePayers. Re-fast-tracking an address already in `watching` / `normal_candidate` is a no-op (preserves bundler group progress).
- **/start menu**: **🗑 Remove** button per watched potential feePayer — unsubscribes Enhanced WSS, recipient watches, and cooldown dev watch. Blocked only while `active` on Insider bot.

## 2026-07-17 (35)

### Normal mode: dust-first group skip at ≥20 txs + feePayer resume on pre-buy skip

- **Dust skip threshold**: first qualifying **dust** 10s group now requires **≥20 txs** (same as round buy gate), not ≥2. Logs “waiting for 20+ txs” while 2–19 dust outs accumulate in the window.
- **Funder-first pre-buy skip**: rug MC guard, dust-first group, excessive dust, etc. no longer enter dev cooldown. FeePayer watch resumes (`watching` / `normal_candidate`) after `tokenFlowEnded` with `hadPosition: false`.
- **Cooldown** only after a completed trade (`hadPosition: true`). Pre-buy skip removes mint from `boughtMints` so the same token can hand off again if needed.

## 2026-07-17 (34)

### Parallel follow-wallet + funder-first on two tokens

- **Funder-first** handoff picks the **first idle insider bot** in the Helius key pool (not only bot 1). If bot 1 is on a follow-wallet token, funder-first uses bot 2+ when configured.
- **Follow-wallet** monitoring stays on bot 1; if bot 1 is already on a token when the follow wallet buys again, the new flow **delegates to the first idle bot** with the same follow-wallet validation.
- Mint claiming across bots prevents the same mint on two bots. MC checks, buy/sell triggers, and cooldown `tokenFlowEnded` listeners already run per bot.

## 2026-07-17 (33)

### Telegram: fast-track potential feePayer from /start menu

- **index.ts**: new **Fast-track feePayer** button on the home menu. Prompts for a wallet address and arms funder-first monitoring as if the feePayer funder had just funded that recipient (current SOL balance baseline, Enhanced WSS + REST sync from baseline timestamp).
- **funder-first-orchestrator.ts**: `fastTrackPotentialFeePayer()` — validates address/balance, rejects active/cooldown/zero-balance wallets, resets group state, sends Telegram alert, runs forced REST sync. Timestamp-only baseline (no funder tx signature) uses recent desc + timestamp filter for the first sync.

## 2026-07-17 (32)

### Dev zero native SOL treated as rug signal

- **Insider bot**: while watching a token, dev wallet native SOL balance is subscribed alongside CLOSE_ACCOUNT detection. Zero balance triggers the same pre-buy reset / in-position sell path as a dev WSOL close-account rug. Immediate balance check on subscribe catches devs already drained.
- **Funder-first cooldown**: dev SOL balance subscription during cooldown; zero balance resumes the feePayer watch (same outcome as CLOSE_ACCOUNT rug). Immediate check on cooldown entry.

## 2026-07-16 (31)

### Normal mode: first sol group + buy at ≥20 round txs

- **First qualifying group** = first 10s window with **≥2 txs of the same sol type** (dust, or one of ~0.02 / ~0.05 / ~0.1 SOL).
- If that first group is **dust** → skip token.
- **Buy** only when a **~0.02 / ~0.05 / ~0.1 SOL** group has **≥20 txs** in 10s and is the first qualifying sol group (2–19 txs waits, no buy).
- Removed the >20 round-group skip cap.

## 2026-07-16 (30)

### Normal mode: first 10s group uses tx counts (2–20), not dust-group tally

- **First qualifying group** = first 10s window with **≥2 transfer-out txs**.
- If that first group is **dust** (not ~0.02 / ~0.05 / ~0.1 SOL) → skip token.
- If first group is **~0.02 / ~0.05 / ~0.1 SOL** (same round size) with **2–20 txs** in 10s → buy.
- **>20 txs** in a valid round 10s window → skip.
- **>20 individual dust txs** total before the first qualifying group → skip.
- Removed counting separate “dust groups” over time.

## 2026-07-16 (29)

### Normal mode: dust skip uses groups, not individual outs

- Dust skip threshold is now **>20 dust groups** (≥2 non-round transfer-outs in 10s), not >20 individual dust outs.
- Up to **20 dust groups** may precede the first valid ~0.02 / ~0.05 / ~0.1 SOL round group; the 21st dust group skips the token.
- Removed immediate skip on any single dust group before a valid round group.

## 2026-07-16 (28)

### Normal mode: exact ~0.02 / ~0.05 / ~0.1 SOL round groups only

- Replaced the ~~0.02–~~0.1 SOL range logic with **three discrete round sizes** only (±0.004 SOL each).
- Buy requires **≥2 recipients at the same round size within 10s**, and that group must be the **first valid SOL group** (no prior dust group of ≥2 in 10s).
- **Dust** = any transfer not matching ~0.02 / ~0.05 / ~0.1 SOL. **>20 dust groups** (≥2 in 10s) before the first valid round group skips the token.
- Removed range-based buys, micro-dust threshold, out-of-range immediate skip, preamble wait, and stale $1–$5 lock timeout.

## 2026-07-16 (27)

### Normal mode: restore >20 micro-dust counter + ~~0.02–~~0.1 first group

- **Micro-dust** is strictly below **~0.006 SOL** (0.01 − 0.004 tolerance). Up to **20** allowed before the first buy group; **>20** skips the token.
- **~0.01 SOL** and other amounts between micro-dust and ~0.02 SOL are allowed as preamble (no immediate skip).
- **Buy** still requires the **first group**: ≥2 recipients in 10s with each transfer in ~~**0.02–~~0.1 SOL** (any amount in range, not just exact round sizes).
- Only transfers **above ~0.1 SOL** before the first group still skip immediately.

## 2026-07-16 (26)

### Funder-first: REST sync dev wallet on cooldown entry

- When a feePayer enters post-trade cooldown and starts watching the dev for `CLOSE_ACCOUNT`, the bot now immediately REST-syncs recent dev-wallet transactions.
- Catches dev rugs that occurred before the Enhanced WSS subscription was armed; processed signatures are deduped between REST and WSS.

## 2026-07-16 (25)

### Normal mode: unified ~~0.02–~~0.1 SOL first-round buy gate

- Dust is anything **below ~0.02 SOL** seen before the first group — skips the token.
- Buy triggers on **any transfer-out from ~0.02 to ~0.1 SOL** (not just exact 0.02/0.05/0.1), **≥2 unique recipients within 10s**, and only when that group is the **first** feePayer transfer-out activity (not a dust preamble).
- Removed same-USD-band grouping — a valid first group can mix any amounts in the ~~0.02–~~0.1 SOL range within the same 10s window.
- Exit: **+90% MC** for triggers below ~0.1 SOL, **+180% MC** when the triggering transfer is ~0.1 SOL.

## 2026-07-16 (24)

### Funder-first: stop potential feePayer on exchange send

- When a potential feePayer sends SOL to a recipient (transfer ≤50% of its initial funder-receive balance), the bot resolves the recipient via Helius `wallet/identity`.
- If the recipient `type` is **exchange** (e.g. Bybit hot wallet), the potential feePayer watch is unsubscribed immediately and a Telegram alert is sent.
- Identity results are cached per wallet address to limit API calls.

## 2026-07-15 (23)

### Normal mode: first feePayer activity must be a round SOL group

- Removed the “>20 dust outs” counter. Any **non-round** feePayer transfer-out (not ~~0.02 / 0.05 / 0.1 SOL within tolerance) that appears **before** the first qualifying same-band group now skips the token immediately — including **~~0.01 SOL** preamble dust.
- Transfers below **$0.10 USD** are still ignored entirely (not tracked).
- The **first** buy-triggering group must be ≥2 recipients in 10s at **~0.02**, **~0.05**, or **~0.1 SOL** only.

## 2026-07-15 (22)

### Insider: -40% P/L stop-loss sell trigger

- While holding a position, periodic MC checks now trigger a full sell when P/L vs entry MC falls to **-40%** or below.
- Stop-loss runs even when the +% MC profit exit is disabled (recipient sell-all / zero-SOL paths).
- Shown in Insider status and post-buy Telegram.

## 2026-07-15 (21)

### Funder-first: follow-wallet merge + zero-only bundler drain

- When a bundler recipient is the follow wallet, funder-first skips duplicate Enhanced WSS / zero-balance subscriptions; follow-wallet txs are forwarded into funder-first buy/drain handling.
- Bundler recipient drain stop changed from ≤50% to **native SOL → zero** only.
- After a bundler recipient token buy is seen, drain-based unsubscribe is skipped so buy confirmation / handoff logic can continue.

## 2026-07-15 (20)

### Funder-first: stop on zero balance, no handoff

- When a potential feePayer's native SOL hits zero, monitoring stops and unsubscribes. No follow/handoff to the drain recipient wallet.

## 2026-07-15 (19)

### Dev rug: CLOSE_ACCOUNT only, not sell-all SWAP

- WSS tx normalizer now classifies SWAP before inner `closeAccount` instructions, so Pump AMM sell-all txs (which close token/WSOL accounts) stay `SWAP` instead of mislabeled `CLOSE_ACCOUNT`.
- Shared `isDevRugCloseAccountTx` rejects `SWAP` txs and any tx involving known DEX programs. Funder-first cooldown resume and Insider dev full-exit reset only fire on standalone dev `CLOSE_ACCOUNT` txs.

## 2026-07-15 (18)

### Funder-first: ≥20 SOL only, zero-balance follow, no low-funding spam

- Removed the 5–19.99 SOL low-funding band entirely from funder-first. Sub-20 SOL bundler sends are silently ignored — no Telegram or backend info logs.
- Potential feePayer watches no longer stop on half-drain (50% balance drop). Monitoring continues until native SOL hits zero.
- On zero balance: follow the primary drain recipient as the next potential feePayer; if the drain returned to the top-level funder, stop and unsubscribe.
- Telegram/UI help text updated to match.

## 2026-07-15 (17)

### Funder-first: handoff REST sync fallback when after-signature rejected

- Helius often rejects `after-signature` on half-drain handoff (signature from the old feePayer's drain tx is not a valid cursor on the new wallet). On that 400, REST sync now falls back to recent desc fetch + timestamp filter instead of failing with a warn.
- Watches store `balanceAtFunderReceiveTimestamp` for the fallback path.

## 2026-07-15 (16)

### Follow-wallet flow: cut Helius REST before low-funding skips

- **Early skip:** when low-funding mode is disabled, reject tokens after `getEarlyInsiderSwaps` if all four bundler buy SOL amounts are known and below 20 SOL — skips ~40+ funding/balance-at REST calls.
- **Sequential funding resolve:** if early skip can't run, stop after the first sub-threshold funding record instead of fetching all four.
- **Defer dev lookup:** `getMintCreateTransaction` + dev WSS watch only run after funding gate passes.
- **Follow wallet WSS:** no longer unsubscribes on buy; guards prevent duplicate flows — avoids extra `transactionSubscribe` on every reset.
- **Balance-at:** use `accountData.nativePostBalance` from funding TRANSFER txs when present; removed duplicate identical `getWalletBalanceAt` call.

## 2026-07-15 (15)

### Funder-first: require exactly 4 bundler funding txs (remove 5s grace)

- Valid group is now **exactly 4** unique recipients in 10s (not 3–4). Group activates immediately once the 4th matching funding tx is recorded.
- Removed `GROUP_ACTIVATION_GRACE_SEC` and deferred 3-wallet activation timers from `(14)`.
- 5+ recipients in the same tolerance window is still skipped entirely.

## 2026-07-15 (14)

### Funder-first: 5s grace before locking 3-bundler groups + asset-based swap detection

- **Group activation:** a 3-wallet cluster is not activated (Telegram / recipient subscribe) until **5 seconds** after the anchor funding tx, giving a 4th recipient time to land in the same 10s window. A cluster of **4** still activates immediately.
- `**wallet-swap-detector.ts`:** new per-wallet swap classifier — requires tx success, known DEX program participation, and a wallet-level asset exchange (SOL/WSOL ↔ token or token ↔ token). Used for funder-first bundler buy detection and for WSS `type: "SWAP"` normalization (no longer program-ID-only).
- Added Meteora DLMM and Orca Whirlpool to the known swap program set.

## 2026-07-15 (13)

### Remove REST backstops; cap forced ascending sync at 20 txs

- Removed all periodic / WS-down REST backstop polling: funder-first feePayer interval sync, insider `runPollTick` REST/pollWallet loop, `scheduleBundlerFunderWsSync`, and onLogs→batch-sync recipient paths (push-only via Enhanced WSS when configured).
- Forced ascending feePayer sync limits reduced **50 → 20** (`BUNDLER_FUNDER_SYNC_LIMIT`, `POTENTIAL_FEEPAYER_SYNC_LIMIT`). One-shot forced sync on flow events (funder receive, handoff, bundler-funder start/migration, sell rearm) is unchanged.

## 2026-07-15 (12)

### Funder-first: connect post-funder-receive pipeline with logging (REST backstop removed in `(13)`)

- After funder SOL receive: explicit logs for WSS subscribe, pipeline armed, REST sync start/complete (fetched/processed counts), bundler funding events, and group evaluation.
- Awaits the initial forced REST sync so immediate post-receive txs are not missed silently.
- ~~Periodic REST backstop~~ — removed in `(13)`.
- WSS potential-feePayer notifications drive detection; re-arm watch if subscription was dropped.

## 2026-07-15 (11)

### Funder-first: REST sync + pre-group half-drain handoff chain

- Each potential feePayer now runs a REST backstop sync (`getAddressTransactionsAsc`, 50 tx limit, 1s min interval, single-flight queue) from the funder-receive signature onward — forced on first funder funding and after handoff.
- **Before any bundler group:** half-drain (≤50% of post-funder-receive balance) inspects the drain tx; if SOL returns to the top-level funder, keep watching; otherwise stop the current feePayer and start fresh on the SOL recipient (chain can repeat).
- **After a bundler group is active:** the feePayer itself is only dropped on native SOL → 0 (no more half-drain on the feePayer wallet).

## 2026-07-15 (10)

### Funder-first: skip 10s windows with 5+ tolerance matches

- If 5 or more recipients in a 10s window have post-balances within 0.5 SOL of each other, that window is skipped entirely — no group is formed from it (previously the earliest 4 were kept).

## 2026-07-15 (9)

### Funder-first: tighten 3–4 bundler group selection

- Cluster detection now finds the largest valid 3–4 recipient set whose post-balances span ≤0.5 SOL (balance-sorted sliding window), preferring 4 over 3 when both are valid.
- If 5+ recipients meet the tolerance in one 10s window, that window is skipped (see `2026-07-15 (10)` — was briefly capped to earliest 4).
- Concurrent groups no longer share recipients: once a wallet is in an active group, it is excluded from forming another overlapping group on the same feePayer.

## 2026-07-15 (8)

### Fix funder-first missing SOL transfer detection on Enhanced WSS

- `extractOutgoingSolTransfers` now handles delta-reconstructed `nativeTransfers` from Enhanced WSS (`from=funder, to=__pool__` paired with `from=__pool__, to=recipient`). Previously every funder send was silently skipped because the recipient was always `__pool__` on the outgoing leg.
- `handleFunderTx` uses the fixed helper so new potential feePayer watches and Telegram alerts fire again.

## 2026-07-15 (7)

### Remove Token Transfer mode; drop all idle token-account RPC polling

- Deleted `token-transfer-orchestrator.ts` and removed every Token Transfer code path from `index.ts` (orchestrator, callbacks, startup/shutdown, Telegram UI).
- `WalletMonitor` no longer runs any recurring `getParsedTokenAccountsByOwner` poll — only a one-time startup snapshot; buy detection is push-driven (Enhanced WSS primary, `onLogs` + `getParsedTransaction` fallback).
- Removed dead config: `DEFAULT_BOT_MODE`, `WALLET_POLL_INTERVAL`, `defaultBotMode`, `walletPollInterval`, and unused `TokenExitEvent` type.

## 2026-07-15 (6)

### Follow-wallet: drop idle token-account RPC poll when Enhanced WSS is up

- `WalletMonitor` no longer runs recurring `getParsedTokenAccountsByOwner` polls while Enhanced WSS is connected — only a one-time startup snapshot plus a 15s WS health check. RPC poll resumes as backstop if Enhanced WSS drops.

## 2026-07-15 (5)

### Funder-first: concurrent groups + 0.5 SOL post-balance clustering

- Multiple 3–4 bundler groups per feePayer can be monitored **concurrently** (each keyed by anchor); abandoning one group does not stop others.
- Group detection now requires 3–4 unique recipients in 10s whose post-balances are within **0.5 SOL** of each other (still in normal ≥20 or low 5–19.99 band).
- Per-recipient drain/zero only unsubscribes when no remaining active group needs that wallet.
- Token confirm uses the band of the group that contained the buying wallet.

## 2026-07-15 (4)

### Follow-wallet Enhanced WSS + funder-first multi-group recipient rules

- **Follow-wallet**: `WalletMonitor` accepts optional `enhancedWs`; Insider bot passes the shared client so follow-wallet resume uses `transactionSubscribe` (token buy detection from normalized SWAP payloads) with poll backstop when WS is down.
- **Funder-first groups**: feePayer keeps watching for new 10s bundler windows (normal or low) until a recipient buy overlaps the token's first-four bundlers. Exhausted groups are abandoned and the next window is evaluated.
- **Per-recipient rules**: each bundler in the active group is unsubscribed if post-balance drops to ≤50% after the feePayer send or native SOL hits zero (`onAccountChange`). When all recipients in a 3–4 group stop, that group is abandoned.
- FeePayer zero-balance `onAccountChange` unchanged.

## 2026-07-15 (3)

### Shared Enhanced WSS + batched recipient subscriptions (API optimization)

- **Why**: Insider bot, funder-first orchestrator, and Token Transfer each opened their own WebSocket to the same Developer-plan key — wasteful connections and duplicate feePayer watches after handoff. Funder-first also used one `transactionSubscribe` per recipient wallet.
- `index.ts`: one process-wide `HeliusEnhancedWsClient` (`Shared Helius Enhanced WS`) injected into Insider bots, Token Transfer, and funder-first; closed once on shutdown.
- `helius-enhanced-ws.ts`: added `watchMulti(addresses, callback)` and `updateWatchAddresses()` — batches `accountInclude: [addr1, addr2, …]` into a single server subscription; routes notifications to the matched address.
- `funder-first-orchestrator.ts`:
  - All active recipient wallets share one batched subscription; recipients are unsubscribed promptly on stop, handoff, or cooldown.
  - Funder re-send logic: if funder sends SOL to an already-watched **undecided** potential feePayer (`watching` / `normal_candidate` / `low_candidate`), updates the balance baseline on the existing monitor; if the wallet is **confirmed** (`active` / `cooldown`), skips the funder tx.
  - Failed Insider handoff restores candidate status and re-subscribes recipients.

## 2026-07-15 (2)

### Parallel "funder-first" discovery flow (feePayer funder → potential feePayers → bundler validation → normal buy)

- **Why**: the existing follow-wallet flow backtracks from a followed wallet's buy to find a shared feePayer. The user also wants to start from a known top-level **feePayer funder** wallet and discover potential feePayers upstream, without replacing the follow-wallet path.
- New `funder-first-orchestrator.ts`:
  - Always watches `INSIDER_FEEPAYER_FUNDER_ADDRESS` via Enhanced WSS for SOL transfer-outs.
  - Each new recipient becomes a **potential feePayer** watch (also Enhanced WSS + zero-balance `onAccountChange`).
  - **Stop rules**: post-balance ≤50% of balance right after the funder receive, or native SOL balance hits zero.
  - **Normal candidate** (≥20 SOL *recipient post-balance*, 3–4 unique wallets in 10s): Telegram notice, subscribe recipients, wait for SWAP buy, confirm overlap with the token's first-four bundlers, then hand off to `InsiderBot.startFromFunderFirst`.
  - **Low-funding pattern** (5–19.99 SOL post-balance, 3–4 in 10s): Telegram **Skipped** notice, no buy; note dev wallet; feePayer enters cooldown until dev `CLOSE_ACCOUNT` rug, then resumes.
  - After a normal trade completes (`tokenFlowEnded`), feePayer pauses until dev rugs, then resumes watching.
- `insider-bot.ts`: added `startFromFunderFirst(mint, feePayer, earlyBuys)` and `startKnownFeePayerBundlerFlow` (skips four-bundler funding backtrack); emits `tokenFlowEnded` on reset/cycle-complete for orchestrator cooldown. Follow-wallet flow unchanged.
- `tx-normalizer.ts` / `helius-client.ts`: `accountData[].nativePostBalance` (lamports) for recipient post-balance checks.
- `index.ts`: wires orchestrator, auto-starts when funder address is configured, Telegram `/start` shows funder address + watched potential feePayers, buttons for **FeePayer funder** / **Start/Stop Funder-First**.
- `config.ts` / `types.ts` / `env.example`: `INSIDER_FEEPAYER_FUNDER_ADDRESS`.

## 2026-07-15

### Migrated the 6 active "free push → 100-credit REST fetch" sites to Helius Enhanced WSS `transactionSubscribe` (Phase 3)

- **Why**: every push-driven detection path in the bot (shared-feePayer sync, recipient watch, insider/bundler wallet monitoring, dev-wallet full-exit detection, and Token Transfer mode's dev-wallet poll) followed the same pattern — a free `onLogs` websocket notification (or, for Token Transfer, a flat-out 4s polling loop) immediately followed by a **100-credit** Helius Enhanced Transactions REST call just to get the parsed transaction. Helius's Developer-plan-only `transactionSubscribe` extension pushes the fully parsed (`jsonParsed`) transaction *in the notification itself*, so the REST call — and everything hanging off it (the 4-key `withHeliusFallback` pool rotation, backoff, rate-limit handling) — can be skipped entirely for these sites. `INSIDER_HELIUS_API_KEY` (the only key confirmed to be on a Developer plan) is now designated as *the* key used for every Enhanced WSS connection in the codebase, regardless of which Helius key a given site's REST fallback/backstop otherwise uses.
- Two new files:
  - `tx-normalizer.ts`: converts a raw Solana `jsonParsed` transaction (native shape: `message.instructions`, `meta.preBalances`/`postBalances`, `meta.preTokenBalances`/`postTokenBalances`) into the same `HeliusTransaction` shape (`type`, `source`, `feePayer`, `tokenTransfers[]`, `nativeTransfers[]`, `accountData[]`) the rest of the codebase already reads from the REST API, so every existing consumer (`classifyTx`, `inspectBundlerFunderTransaction`, `isDevFullExitCloseAccountTx`, `findTokenTransferOut`, etc.) works unmodified. `nativeTransfers`/`tokenTransfers` are reconstructed from balance deltas (the same technique Helius's own enrichment pipeline uses) rather than walking/pairing raw instructions — each nonzero delta becomes one transfer record with the real wallet on the correct side and a `"__pool__"` placeholder on the other side, which is safe because every consumer in this codebase only ever checks `fromUserAccount === <specific known wallet>` / `toUserAccount === <specific known wallet>`, never the other side's identity. `type` is inferred from which program IDs appear anywhere in the instruction tree: Pump.fun/PumpSwap/Raydium/Jupiter program IDs → `SWAP`; an SPL Token `closeAccount` parsed instruction → `CLOSE_ACCOUNT` + `source: "SOLANA_PROGRAM_LIBRARY"`; otherwise `TRANSFER` if any transfer was reconstructed, else `UNKNOWN`.
  - `helius-enhanced-ws.ts`: `HeliusEnhancedWsClient`, a small hand-rolled WebSocket JSON-RPC client (`@solana/web3.js`'s `Connection` doesn't expose `transactionSubscribe` — it's a Helius-only extension). Exposes `watch(address, callback)`/`unwatch(id)`, mirroring `connection.onLogs`/`removeOnLogsListener` so call sites could swap over with minimal churn. One WebSocket connection per instance watches N addresses via N separate `transactionSubscribe` calls; on disconnect it reconnects with exponential backoff + jitter and re-subscribes every still-active watch; a ping/pong heartbeat forces a reconnect if the socket goes silently dead. `package.json`: added `ws`/`@types/ws` as explicit dependencies (previously only present transitively via `@solana/web3.js`'s `rpc-websockets`).
- `insider-bot.ts`: added `this.enhancedWs`, constructed once per bot instance from `config.insiderHeliusApiKey || config.heliusApiKey` (never a fallback-pool key). Migrated 5 sites, each with the exact same fallback rule — if `this.enhancedWs` is null (key not configured), the site transparently falls back to its original `onLogs` + REST fetch behavior:
  - **FeePayer sync** (`subscribeBundlerFunder`): notifications feed `applyBundlerFunderNotificationTx` → `inspectBundlerFunderTransaction` directly, no `getAddressTransactionsAsc` call.
  - **Recipient watch** (`subscribeFunderRecipient`): notifications feed `applyFunderRecipientNotificationTx` → `applyFunderRecipientTransaction(..., "notification")` directly (this `source` parameter already existed but was previously unused/dead). A per-recipient `recipientEnhancedWatchSeenSignatures` dedup set guards against a reconnect redelivering an already-applied signature.
  - **Insider/bundler wallet monitoring** (`startInsiderMonitoring`/`startBundlerMonitoring`): notifications feed a new shared `handleEnhancedWsMintTx` helper straight into `handleInsiderTransaction`/`handleBundlerTransaction`, replacing the `queueSignature`/`processSignatureBatch` batched-REST-fetch pipeline for the push path.
  - **Dev-wallet full-exit detection** (`subscribeDevWalletFullExitWatch`): notifications feed a new `evaluateDevWalletFullExitTx` (shares the `isDevFullExitCloseAccountTx` + `devCreateTimestamp` check with the REST fallback path) with no `getTransactionsBySignatures` call.
  - Each site's `stop*`/`unsubscribe*` cleanup now tears down both the Enhanced WSS watch handle and the legacy `onLogs` sub ID (whichever is active), and `isRunning()` checks both.
- **REST calls demoted to a rare safety net, not removed**: `runPollTick` now computes `wsHealthy = this.enhancedWs?.isConnected ?? false` once per tick. `syncBundlerFunderTransactions`/`syncFunderRecipientBatch` are now called with `force = !wsHealthy` (both already supported a `force` param) — while the WS connection is healthy this keeps them near-permanently idle (no dirty signatures, no forced refetch), but the instant it drops, they revert to firing on every tick exactly like before this migration. The pre-buy `pollWallet` backstop only runs if `!wsHealthy || insiderEnhancedWatchId === null`; a new holding-phase backstop loop over `bundlerWatch.wallets` (previously nonexistent — post-buy bundler monitoring had zero REST backstop at all) only runs when `!wsHealthy`.
- `token-transfer-orchestrator.ts`: added its own `HeliusEnhancedWsClient` (also always keyed to `config.insiderHeliusApiKey`/the Developer key — **not** `config.insiderHeliusApiKey4`, which this mode otherwise uses exclusively for REST/MC, since `transactionSubscribe` needs a Developer-plan key specifically). `start()` now calls `subscribeEnhancedWs(devAddress)` and reschedules the old `pollDevWallet` loop at a new `DEV_WALLET_BACKSTOP_POLL_INTERVAL_MS` (45s) instead of the original always-on `DEV_WALLET_POLL_INTERVAL_MS` (4s) — the poll loop still exists and both the push callback and the poll loop now funnel through a shared `processDevWalletTx` helper, but the poll only runs at the original tight 4s cadence when `!enhancedWs.isConnected`. `stop()`/`shutdown()` unwatch/close the WS connection.
- `index.ts`: calls `bot.closeEnhancedWs()` for every Insider bot during graceful shutdown (added `InsiderBot.closeEnhancedWs()`), alongside the existing `tokenTransferOrchestrator.shutdown()` which already closes its own WS connection.
- `env.example`: documented that `INSIDER_HELIUS_API_KEY` must be on a Developer+ plan and is the sole key used for every Enhanced WSS connection in the codebase.
- **Net effect**: for every site above, per-event cost drops from a 100-credit REST call to a data-metered WebSocket message (typically a few KB, ~0.1-0.3 credits) whenever the Enhanced WSS connection is up, and latency drops from "notify, then wait ~100-400ms for a second request" to "notify with the data already attached." A dropped/unconfigured WS connection degrades gracefully to the exact pre-migration behavior at every single site, not a broken or silently-degraded one.
- **Not runtime-verified against a live Helius connection** in the environment this was authored in (no confirmed network egress to `wss://mainnet.helius-rpc.com` from that sandbox — even the npm registry hit a TLS verification error there). `tx-normalizer.ts`'s field-extraction logic is defensive (optional chaining throughout, warns-and-skips instead of throwing on an unrecognized shape) specifically because of this. **After deploying, watch for** `[TX-NORMALIZER]`**/**`[...ENHANCED WS]` **warn logs** — if the exact `transactionSubscribe` notification shape differs from what's assumed here, those logs will show a truncated sample of the actual payload, which is what's needed to adjust the normalizer.

## 2026-07-14 (5)

### Fixed followed-wallet buys being permanently dropped when Helius hadn't indexed the mint's early transactions yet

- **Why**: `getEarlyInsiderSwaps` (fetched via `startInsiderFlow` right after a followed-wallet buy) requires at least one of the mint's first 5 transactions to already be classified `type === "SWAP"` by Helius. For a token that's only seconds old, Helius sometimes hasn't finished indexing/classifying those transactions yet, so this throws `"No SWAP transactions found for mint yet"` even though the buy is real. Two compounding issues made this worse than it should've been: (1) `withHeliusFallback`'s `isTransientHeliusError` didn't recognize this message, so it was treated as a **permanent** failure and immediately gave up instead of trying the other 3 Helius pool keys — the "Helius pool metrics" log showed only key index 0 was ever attempted (`requests: 1, permanentFailures: 1`) while keys 2-4 sat idle (`requests: 0`); (2) once `startInsiderFlow` threw, `handleFollowWalletBuy`'s catch block called `resetForNewToken(true)` and gave up on the mint for good — since the mint was already added to `boughtMints` before the flow started (and that set is only cleared when the followed wallet address itself changes), the bot would never retry that same followed-wallet buy again for the rest of the session, silently missing a real signal.
- `insider-bot.ts`:
  - `isTransientHeliusError` now also matches `"No transactions found for mint yet"` / `"No SWAP transactions found for mint yet"`, so `withHeliusFallback` cycles through the whole Helius pool for this error instead of bailing after the first key (each key still runs its own ~3-4s internal retry loop, so this alone extends the effective window from ~4s to ~15s across 4 keys).
  - Added `startInsiderFlowWithIndexingLagRetry(mint)`, now called from `handleFollowWalletBuy` in place of a direct `startInsiderFlow(mint)` call: if `startInsiderFlow` still throws this same indexing-lag error after exhausting the whole Helius pool, it retries the whole flow up to 2 more times with real-world delays (4s, then 8s) before giving up and letting the existing catch block run `resetForNewToken(true)` as before. Any other error (e.g. `InsiderMinBuySolFilterError`) is rethrown immediately with no extra delay, unchanged from before.
- Net effect: a followed-wallet buy on a brand-new mint now gets a much more generous window (up to ~10x longer) for Helius's indexing to catch up before the bot gives up on it, instead of failing permanently after a single ~4-second attempt against one API key.

## 2026-07-14 (4)

### Dust is now defined purely by ~0.01 SOL, not "$0.10-$0.99" USD — and fixed a dead-code bug where dust-group tracking never actually ran

- **Why**: the previous change (2026-07-14 (3)) only added the ~0.01 SOL check as an *override* on top of the existing USD-based dust definition (`amountUsd < $1`). The user asked to fully replace the USD-based definition with the SOL-based one. While tracing through this, found that `getTinyUsdBand`'s dust case (`amountUsd < BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD`) was mathematically identical to a check already sitting *earlier* in `inspectBundlerFunderTransaction` (`if (transferOutUsd < BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD) { ...; return false; }`), which meant the whole dust-group-building branch (`if (tinyUsdBand === "lt2_5")`, which sets `normalTinyDustGroupSeen` and sends the "Dust Group Observed" notification) was unreachable dead code for any transfer-out classified as dust purely by USD — it always got caught and returned by the earlier check first.
- `insider-bot.ts`:
  - `getTinyUsdBand(amountUsd, amountSol)`: removed the `amountUsd < BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD` fallback entirely. `"lt2_5"` (dust) is now returned *only* when `amountSol` is within `BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL` (±0.004 SOL) of `BUNDLER_FUNDER_NORMAL_TINY_DUST_ROUND_SOL_AMOUNT` (0.01 SOL); everything else bands purely by USD (`<= $5` → `"2_5_to_5"`, else `"gt5"`).
  - `inspectBundlerFunderTransaction`: reordered so the `tinyUsdBand === "lt2_5"` dust-group-tracking block now runs (and returns) *before* the `transferOutUsd < BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD` skip check, instead of after it. This makes the dust branch reachable again — it now runs unconditionally for any ~0.01 SOL transfer-out regardless of its USD value, while the min-buy-USD check still catches any leftover sub-$1 transfer-out that isn't SOL-dust-shaped (e.g. a coincidental non-round tiny amount), which then also fails the round-SOL-amount check for the `2_5_to_5`/`gt5` bands anyway.
  - Updated comments, log messages, and the Telegram "Dust Group Observed"/"dust group was already seen" copy that referenced the old "$0.10-$0.99" USD range to describe the new "~0.01 SOL" definition instead. `BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD` ($0.10) is unchanged and still serves its original, separate purpose: an absolute USD floor below which a transfer-out is ignored entirely (not tracked as dust or anything else), independent of the dust band's own SOL-based definition.
- Net effect: dust-group detection for the $1-$5 band's "not first group" rule now actually fires in practice (previously silently dead for USD-defined dust), and dust classification no longer drifts with SOL price.

## 2026-07-14 (3)

### ~0.01 SOL transfer-outs are always treated as dust, regardless of USD value

- **Why**: the $0.10-$0.99 "dust" band (`"lt2_5"`) was classified purely by USD amount. At a higher SOL price, a ~0.01 SOL transfer-out (itself a recognizable small gas-funding round, just like the other non-round dust sizes already tracked in that band) can compute to more than $1 and get misclassified into the real $1-$5 buy-triggering band instead of being treated as dust.
- `insider-bot.ts`: added `BUNDLER_FUNDER_NORMAL_TINY_DUST_ROUND_SOL_AMOUNT = 0.01`. `getTinyUsdBand` now takes `amountSol` in addition to `amountUsd`, and returns `"lt2_5"` (dust) whenever the SOL amount is within the existing `BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL` (±0.004 SOL — i.e. 0.006-0.014 SOL) of 0.01 SOL, before falling back to the normal USD-based banding. Both call sites (`inspectBundlerFunderTransaction`'s live band check, and `getNormalTinySameBandGroup`'s per-entry re-check) were updated to pass `amountSol` through.
- Net effect: a transfer-out landing in the ~0.006-0.014 SOL range is now always routed through the dust-group logic (contributing to `normalTinyDustGroupSeen`, the "not first group" tracking via `recordNormalTinyTransferOut`, etc.) instead of being eligible for the $1-$5 buy gate — even if its USD value alone would otherwise put it there.

## 2026-07-14 (2)

### Replaced the "$5,000 MC" rug reset/sell logic with dev-wallet full-exit detection (CLOSE_ACCOUNT / SOLANA_PROGRAM_LIBRARY)

- **Why**: watching market cap drop below a fixed $5,000 floor is a lagging, noisy signal for "this token is rugged." A much sharper signal is the dev wallet itself fully cashing out — on Pump.fun/PumpSwap this shows up as a `CLOSE_ACCOUNT` transaction (source `SOLANA_PROGRAM_LIBRARY`) paid for by the dev wallet, closing its WSOL token account right after unwrapping sell proceeds to native SOL.
- `insider-bot.ts`: detection is push-based (websocket log subscription), not polled — matching the same subscribe-then-`getTx` pattern used everywhere else in the file (`queueSignature`/`processSignatureBatch`, `subscribeLowFundingDevWalletSubscription`, `subscribeFunderRecipient`). `subscribeDevWalletFullExitWatch()` opens a `connection.onLogs` subscription on the tracked `devWallet` as soon as it's identified in `startInsiderFlow`; each new log notification calls `checkDevWalletSignatureForFullExit(signature)`, which fetches *only that signature* via `getTransactionsBySignatures` and checks `isDevFullExitCloseAccountTx` (`type === "CLOSE_ACCOUNT"`, `source === "SOLANA_PROGRAM_LIBRARY"`, `feePayer === devWallet`) plus a `devCreateTimestamp` floor. A `devFullExitHandled` flag and a `devFullExitSeenSignatures` dedup set (both reset per-token) prevent re-triggering/re-fetching. The subscription survives the pre-buy → holding transition (only `stopPreBuyMonitoring`'s unrelated insider-wallet sub is torn down on buy) and is cleaned up via `stopDevWalletFullExitWatch` in `stopFlowMonitoring`/`completeFlowCycle`.
- On a match, `handleDevWalletFullExit(mint, tx)` runs: pre-buy (no active position) sends a "🧹 Dev Full-Exit Reset — Token Skipped" notification and calls `resetForNewToken(true)`, same shape as the old rug reset. Post-buy (active position) calls the existing `triggerPositionSell` helper with a "🚨 Dev Full-Exit Detected — Selling ASAP" notification, routing through the same `sellTrigger` event/sell pipeline the MC-based rug sell used to use — no changes needed on the sell-execution side.
- `index.ts`: removed the `currentMc < INSIDER_MIN_MARKET_CAP_USD` rug block from `checkInsiderMcapFlow` (the `exitMc` profit-target exit check right below it is untouched) and deleted `checkAndSellIfLowMcap` entirely (it was a second, fully redundant MC<$5k sell trigger only ever called for insider positions). Removed the now-unused `INSIDER_MIN_MARKET_CAP_USD` constant and updated the Insider status-menu footer text to describe the new dev-wallet-based rug detection.
- Not touched: the three pre-buy buy-gate checks against `INSIDER_RUG_MARKET_CAP_USD` in `insider-bot.ts` (low-funding shared-feePayer/recipient buy gates) — these only run inside low-funding mode, which is currently disabled (`BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED = false`), so they're inert dead code for now.

## 2026-07-14

### Relaxed the shared-feePayer validation: 3-of-4 matching feePayers now passes (was requiring all 4)

- **Why**: a real token had 3 of the first 4 early bundlers funded by the same feePayer (`5TDYtdbM...`, ~30-33 SOL each), but the 4th bundler (`ALVy3Qw6...`) had its largest/selected funding transfer come from a different, unrelated feePayer (a ~59 SOL transfer, clearly a different funding source/pattern). The old all-or-nothing check (`feePayers.size !== 1`) reset on this single outlier even though the shared-feePayer signal from the other 3 was strong and unambiguous.
- `insider-bot.ts`: added `BUNDLER_FUNDER_MIN_MATCHING_FEEPAYER_COUNT = 3`. After fetching the 4 funding records, they're grouped by `fundingFeePayer` and the largest group ("majority group") is taken. If the majority group has fewer than 3 matching records (e.g. a 2-2 split, or all 4 different), the bot still resets ("Not enough bundler funding tx feePayers matched"). Otherwise, the watch proceeds using **only the majority group's records** — the outlier record's feePayer/amount is ignored entirely and logged separately ("Majority of bundler funding tx feePayers matched; proceeding with the majority feePayer and ignoring the outlier(s)").
- `earliestFundingTimestamp`, `cursorSignature`, `largestFundingSol`, `processedSignatures`, and the tracked `funderWallet` are now all derived from the majority group only, not the full 4. `bundlerWallets` (the set used to exclude the original early buyers from being mistaken for new recipients later) still comes from all 4 original bundler-buy wallets (`firstFour`), regardless of which ones matched the majority feePayer — the outlier bundler genuinely did buy early, it just isn't used to determine the shared feePayer itself.
- Net effect: if 4-of-4 match, behavior is unchanged. If 3-of-4 match, the token now proceeds using the 3 matching records. If only 2-of-4 (or fewer) match, the bot still resets as before.

## 2026-07-13 (6)

### Round-SOL-amount check is now band-specific: >$5-$10 (+180%) only accepts ~0.1 SOL, $1-$5 (+90%) only accepts ~0.02/0.05 SOL

- Previously `isRoundBundlerTinySolAmount` checked a transfer-out's SOL amount against *all three* round targets (0.02/0.05/0.1 SOL) regardless of which USD band it was in — harmless in practice at typical SOL prices (0.02/0.05 SOL essentially can't reach the >$5 USD floor), but not strictly correct, and not what was asked: for the >$5-$10 band (+180% MC exit), the only valid round size should be **~0.1 SOL** (±0.004 SOL tolerance) — not 0.02 or 0.05.
- `insider-bot.ts`: replaced the flat `BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS` list with a per-band map, `BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS_BY_BAND`: `"2_5_to_5"` ($1-$5) → `[0.02, 0.05]`, `"gt5"` (>$5-$10) → `[0.1]`, `"lt2_5"` (dust) → `[]` (unused, dust never goes through this check).
- `isRoundBundlerTinySolAmount(amountSol, band)` now takes the band and only matches against that band's own target(s). Both call sites (the group-formation filter in `getNormalTinySameBandGroup`, and the diagnostic log in `inspectBundlerFunderTransaction`) now pass the band through.
- Net effect: a >$5-$10 group's members must each be within ±0.004 SOL of exactly **0.1 SOL** to be trusted as a genuine round bundler-funding size and trigger the +180% MC buy; the $1-$5 band remains gated on ~0.02 or ~0.05 SOL for the +90% MC buy, unchanged from before.

## 2026-07-13 (5)

### Round-SOL-amount tolerance widened from ±0.001 to ±0.004 SOL

- **Why**: production logs showed a real bundler funding round using ~~0.018 SOL (~~$1.37 each, 15 recipients) getting stuck at "Normal tiny transfer waiting for same-band 10s group" with `isRoundBundlerSolAmount: false` forever — 0.018 SOL is 0.002 SOL away from the 0.02 SOL target, just outside the old ±0.001 tolerance.
- `insider-bot.ts`: `BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL` changed from `0.001` to `0.004`. At this tolerance the three target ranges are still non-overlapping (0.02 → [0.016, 0.024], 0.05 → [0.046, 0.054], 0.1 → [0.096, 0.104]), so there's no ambiguity between round sizes — just more headroom for real-world fee/slippage variance like the 0.018 SOL case above.

## 2026-07-13 (4)

### Dust-group-preceded $1-$5 sub-band split now keys off the group's round SOL size, not its USD amount

- `insider-bot.ts`: the "dust group already seen, so route the next $1-$5 group by sub-band" check (added earlier the same day) previously split on USD amount — `groupMaxUsd <= $2.50` skipped, `> $2.50` bought. Replaced with a check on the group's round SOL size instead: **~0.02 SOL skips/resets**, **~0.05 SOL (or larger round) still buys** with the usual +90% MC exit.
- Added `isNearBundlerTinySolAmount(amountSol, target)` (slim-tolerance match against a single target) and refactored `isRoundBundlerTinySolAmount` to reuse it against all of `BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS`.
- Removed the now-unused `BUNDLER_FUNDER_NORMAL_TINY_LOW_MID_SPLIT_USD` ($2.50) constant.
- Every member of a $1-$5 group already has to match one of the round bundler sizes (0.02/0.05/0.1 SOL) to even form a group at all (see the "round SOL amount" filter above), so this check just reads which round size the group landed on rather than re-deriving anything from USD.

## 2026-07-13 (3)

### $1-$5/>$5-$10 buy-triggering bands now require a "round" bundler SOL amount (0.02/0.05/0.1 SOL)

- Context: a $1-$5 band group bought on two ~~0.03 SOL (~~$2.14 each) transfer-outs — an amount that doesn't match the round SOL sizes bundlers actually use for gas-funding rounds, unlike a genuine group such as two ~~0.1 SOL (~~$7.67 each) transfer-outs in the >$5-$10 band, which should trigger a buy.
- `insider-bot.ts`: added `BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_AMOUNTS = [0.02, 0.05, 0.1]` and a slim tolerance `BUNDLER_FUNDER_NORMAL_TINY_ROUND_SOL_TOLERANCE_SOL = 0.001`, plus a new `isRoundBundlerTinySolAmount(amountSol)` helper.
- `getNormalTinySameBandGroup` now additionally requires, for the `"2_5_to_5"` ($1-$5) and `"gt5"` (>$5-$10) bands only, that every member of the candidate group's `amountSol` be within that tolerance of one of `0.02`/`0.05`/`0.1` SOL — otherwise the whole candidate group is rejected (same "reject the batch" style already used for the same-band check). The `"lt2_5"` dust-band check (used only for the not-first-group/dust-group flag) is deliberately excluded from this filter, since dust transfers use much smaller, non-round amounts by nature.
- Net effect: `0.100099385`/`0.099955288` SOL (both ≈0.1 SOL) still forms a valid >$5-$10 buy group; `0.030108614`/`0.030170117` SOL (≈0.03 SOL, not close to 0.02/0.05/0.1) no longer forms a valid $1-$5 buy group — it now falls through to the same "Normal tiny transfer waiting for same-band 10s group" log path (now also logging `isRoundBundlerSolAmount` for visibility) instead of triggering a buy.

## 2026-07-13 (2)

### New buy filter: buy MC must not be below the token's initial bundler-buy MC

- `insider-bot.ts`: added `initialBundlerMarketCapUsd`, captured once per token in `handleFollowWalletBuy` from the same `followWalletBuyMc` fetch already used for the high-MC ceiling check (i.e. the market cap at the moment the followed/early-bundler wallet first bought this token). Stays `null` (no gate) if that fetch failed.
- Added `isBelowInitialBundlerMarketCap(currentMc)` and wired it into all three buy-emit paths (`emitBundlerFunderBuy` for normal mode, `emitLowFundingRecipientBuy` and `emitLowFundingSharedFeePayerBuy` for low-funding mode), right alongside the existing rug-threshold check: if the market cap fetched at actual buy time is now **below** `initialBundlerMarketCapUsd`, the buy is skipped and the token is reset (`resetForNewToken(true)`) exactly like a rug-threshold hit.
- Rationale: if MC has fallen back below where the earliest bundler bought by the time our own buy gate fires, the token's early momentum has already reversed — not worth buying into a declining chart.
- `initialBundlerMarketCapUsd` is reset to `null` in both `completeFlowCycle` and `resetForNewToken`, alongside `highestObservedMarketCapUsd`, so each new token starts with a clean slate.

## 2026-07-13

### Replaced the timestamp-based "not first group" check with an explicit dust-*group* flag (fixes a bug where dust preceding a $1-$2.5 group still bought)

- **Bug**: production logs showed 15 dust transfer-outs (~$0.60 each, to 15 distinct recipients) all landing in the same second for a token, followed later by a $1-$2.5 group — which still triggered a buy. Root cause: the old check compared `entry.timestamp < earliestGroupTimestamp` using Solana's 1-second-resolution `blockTime`. When dust and a same-band group land in/near the same second, that strict "earlier than" comparison can simply never be true, so the disqualification silently never fired.
- `insider-bot.ts`: removed that timestamp-scan entirely and replaced it with an explicit, sticky state flag: `normalTinyDustGroupSeen` (new field on `BundlerFunderWatchState`, defaults to `false`).
- When a transfer-out lands in the dust band ($0.10-$0.99), the bot now runs the *same* same-band/10s/≥2-recipients grouping logic used for the $1-$5 and >$5-$10 bands (`getNormalTinySameBandGroup`, now widened to also accept the `"lt2_5"` band) to check for a genuine **dust group** — not just one dust tx, but ≥2 distinct recipients within 10s. Once such a group is seen, `normalTinyDustGroupSeen` is set permanently for this token and a `🟡 ... Normal Dust Group Observed` Telegram notice is sent.
- From that point on, whichever qualifying group forms *next* is routed by sub-band instead of being bought unconditionally:
  - **$1.00-$2.50** → token is skipped entirely (`resetForNewToken(true)`), same as before.
  - **>$2.50-$5.00** → still buys, watches for the sell trigger, +90% MC exit.
  - **>$5.00-$10.00** → unaffected either way — always buys with +180% MC exit (now just logged for visibility when a dust group preceded it).
- If no dust group is ever observed for a token, behavior is unchanged from before — groups buy normally with no extra check.
- This is a strictly more reliable version of the same rule requested earlier (`$1-$2.5 skips, $2.5-$5+ buys, when preceded by dust`) — same intended outcome, but keyed off an explicit grouped-dust-event flag instead of a same-second timestamp comparison that could silently fail to trigger.

## 2026-07-12

### Normal-funding threshold raised to 20 SOL+; low-funding mode disabled for now

- `insider-bot.ts`: `BUNDLER_FUNDER_LOW_FUNDING_SOL` changed from `15` to `20`. This is the sole threshold that decides which mode a shared feePayer falls into (`lowFundingMode = largestFundingSol < BUNDLER_FUNDER_LOW_FUNDING_SOL`), so normal-funding mode now requires the feePayer's largest bundler funding to be **≥20 SOL** (was ≥15 SOL); everything below that is the low-funding tier.
- Added a kill switch, `BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED = false`. In `startBundlerFunderFlow`, right after computing `lowFundingMode` (and before any watch state is even created), if a feePayer would have qualified for low-funding mode but the switch is off, the token is skipped outright: a `⏭️ ... Low-Funding Mode Disabled — Token Skipped` Telegram notice is sent and `resetForNewToken(true)` runs, same as any other pre-watch rejection. No watch/state is created for these feePayers at all, so none of the existing low-funding-mode logic (tiny bands, dev-buy gate, large-transfer flow, etc.) runs for them — they're just skipped and the bot waits for the next token.
- This is a single boolean flag specifically so low-funding mode can be re-enabled later by flipping `BUNDLER_FUNDER_LOW_FUNDING_MODE_ENABLED` back to `true` — none of the underlying low-funding logic was removed.
- Normal-funding mode (now ≥20 SOL) and its own tiny same-band logic (including the recent $1-$5 not-first-group rules) are completely unaffected by this change.

## 2026-07-11

### Not-first-group disqualification for the $1-$5 band is now split by sub-band: $1-$2.5 still skips, $2.5-$5 buys anyway

- `insider-bot.ts`: added `BUNDLER_FUNDER_NORMAL_TINY_LOW_MID_SPLIT_USD = 2.5`, splitting the $1-$5 band in two for the not-first-group check added earlier. When a $1-$5 group is found to be preceded by an earlier transfer-out (one dust group, or several separate dust groups/rounds — the timestamp-based check already covers any number of them regardless of how they're clustered):
  - If the group's amounts stay within **$1.00-$2.50** (`groupMaxUsd <= 2.5`), the token is still fully disqualified exactly as before: a `⏭️ ... Normal $1-$2.5 Sub-Band Buy Skipped — Preceded By Dust` notice is sent and `resetForNewToken(true)` runs.
  - If the group reaches into the **>$2.50-$5.00** half instead (`groupMaxUsd > 2.5`), the disqualification is overridden — a `Normal-mode $2.5-$5 sub-band buy gate accepted despite earlier dust/transfer-outs` log line is written and execution falls through to the normal buy-gate flow, so the bot buys and follows the usual MC-based sell trigger through to exit/reset, same as any other successful buy.
- The exit percent for this override case is unchanged (still the mid-band's `BUNDLER_FUNDER_NORMAL_TINY_MID_EXIT_PERCENT`, i.e. +90% MC) — this only changes whether the group is trusted as a buy signal, not what happens after the buy.
- Groups that genuinely have no earlier transfer-out at all (truly first) are unaffected by any of this — they proceed exactly as before regardless of amount within $1-$5.

### The "less than $1" dust band tracked for the not-first-group check is now $0.10-$0.99 (was anything under $1)

- `insider-bot.ts`: added `BUNDLER_FUNDER_NORMAL_TINY_DUST_FLOOR_USD = 0.1`. In `inspectBundlerFunderTransaction`, any feePayer transfer-out below this floor is now skipped outright (logged at debug level, not even recorded into `normalTinyTransferOuts`) before the dust-tracking/band-classification logic runs.
- Practically this means the "less than $1" band used by the $1-$5 band's not-first-group check (added just above) now only considers transfer-outs in the **$0.10-$0.99** range as trackable dust that can disqualify a $1-$5 group from being "first." Amounts under $0.10 are treated as noise and ignored entirely — they neither count as dust nor affect group formation in any way.

### Normal mode's $1-$5 band buy gate now requires that group to be the *first* tiny transfer-out group for the token

- `insider-bot.ts`: `inspectBundlerFunderTransaction` now records every qualifying feePayer transfer-out into `state.normalTinyTransferOuts` — including sub-$1 amounts that fall below `BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD` and were previously never tracked at all (they were dropped before the recording call even ran). Recording now happens before the minimum-USD check; the check itself still gates whether processing continues past that point.
- When a $1-$5 ("2_5_to_5") same-band group of ≥2 recipients forms, a new guard checks whether any other transfer-out already recorded for this token/feePayer (of any size, including that newly-tracked sub-$1 dust) has an earlier timestamp than the group's earliest member. If one exists, the $1-$5 group is rejected outright: it isn't the first tiny transfer-out activity seen for the token, so it no longer qualifies as the buy signal. A `⏭️ ... Normal $1-$5 Band Buy Skipped — Not The First Group` Telegram notice is sent and `resetForNewToken(true)` is called, mirroring the existing staleness-skip pattern.
- This check only applies to the $1-$5 band (as requested) — the >$5-$10 band's grouping/buy logic is unaffected by it, though it now also benefits from the more accurate `normalTinyTransferOuts` data (a dust transfer-out landing inside its own 10s lookback window already caused group-rejection via the pre-existing mixed-band check, now that dust is actually visible there).
- Net effect: the $1-$5 band now only ever triggers a buy off the very first cluster of tiny transfer-outs seen for a token — any $1-$5 group that follows an earlier transfer-out of any size (dust or otherwise) is ignored and the bot resets to wait for the next token.

## 2026-07-10

### Both normal- and low-funding modes' tiny same-band group are now $1-$5 (was $2-$5)

- `insider-bot.ts`: `BUNDLER_FUNDER_NORMAL_TINY_MIN_BUY_USD` changed from `2` to `1`. This is the lower bound `getTinyUsdBand` uses to classify a normal-mode feePayer transfer-out into the mid band (previously $2.00-$5.00, now $1.00-$5.00) vs. skipped-as-too-small (`lt2_5`, now effectively "less than $1"). The upper bound (`BUNDLER_FUNDER_NORMAL_TINY_MID_MAX_USD = 5`) and the >$5-$10 band are unchanged.
- Updated the corresponding log messages and Telegram notification text ("Normal $2-$5 Band Buy Skipped — Too Stale" → "Normal $1-$5 Band Buy Skipped — Too Stale", the buy-gate card's `Band:` line, etc.) to say $1-$5 instead of $2-$5.
- Followed up by also changing `BUNDLER_FUNDER_LOW_FUNDING_TINY_MIN_BUY_USD` from `2` to `1`, so low-funding mode's own (separate) tiny band — used for its bundler gate and dev-buy-after-create gate — is likewise now $1.00-$5.00 instead of $2.00-$5.00. Updated the "Low-Funding Tiny Bundler Gate" and "Low-Funding Tiny Candidate Pending" notification text accordingly. The overall watched range shown at the "Shared FeePayer Locked" step and in low-funding status logs (built from this constant plus the shared $10 upper bound) also shifts from $2.00-$10 to $1.00-$10.
- Net effect: **both** funding modes now start grouping/gating tiny transfer-outs at $1 instead of $2; anything below $1 is still ignored as too small in either mode.

## 2026-07-10

### Token Transfer mode: dev-wallet watch no longer stops after buy; return transfer-in is now an automatic sell signal

- `token-transfer-orchestrator.ts`: `handleTransferOutDetected` no longer calls `stop()` after a transfer-out triggers a buy. The dev-wallet poll loop keeps running for the lifetime of the position, so the wallet is never left unwatched between buy and sell.
- While a position is open, the poll loop's per-tx branching skips the swap-buy/transfer-out candidate logic entirely and instead calls the new `findMatchingTransferIn`, which matches a plain `TRANSFER` where the dev wallet is the *recipient* of the currently-held mint from some other wallet.
- `TokenTransferPosition` gained `transferOutTokenAmount` (the raw token amount originally sent out) and `transferOutUsdValue` (its USD worth). The USD value isn't known synchronously at buy time — pricing a brand-new position can lag — so it's captured lazily by whichever fires first: the existing MC-monitor tick (every 5s) or the pricing lookup inside a transfer-in evaluation.
- New `evaluateSellSignal` prices each qualifying transfer-in via the same Helius-key-4-backed `PumpReserveMarketCapClient.fetchMarketCapUsd` used for MC monitoring (which now also returns `priceUsd`), and emits a `sellSignal` event the moment an incoming transfer's USD value is the same as or greater than the recorded transfer-out's USD value. If a price genuinely isn't available yet on either side, it falls back to comparing raw token amounts (incoming >= outgoing) rather than blocking the signal entirely.
- `index.ts`: new `tokenTransferOrchestrator.on("sellSignal", ...)` listener sends a Telegram alert (amounts + USD values on both sides) and calls a new shared `triggerTokenTransferSell(mint, reason, chatId)` helper to execute the sell through the existing pipeline — this same helper now backs the manual "Sell Position" button too (previously duplicated inline in the `sell:tokentransfer:` callback handler).
- Sell signals are evaluated per-transfer-in (not accumulated across multiple partial transfers-in) — a single incoming transfer must alone match or exceed the original outgoing USD value to trigger the sell.
- Home/status cards for Token Transfer mode now show the priced (or pending-priced) transfer-out value while a position is held, e.g. "Watching dev wallet for a return transfer-in worth >= $1,234 as a sell signal".
- The existing zero-balance auto-stop (previous entry below) and the manual "Sell Position" button both still work unchanged and continue to fully stop the dev-wallet watch once the position closes, regardless of which of the three ways (manual, sell-signal, zero-balance) closed it.

## 2026-07-10

### Skip stale normal-mode $2-$5 band buys (30+ min since shared feePayer lock)

- `insider-bot.ts`: `BundlerFunderWatchState` gained a `lockedAt` timestamp, set to `Date.now()` the moment the shared feePayer is confirmed and the `✅ ... Shared FeePayer Locked` notification is sent. It's preserved across feePayer migration (only the large-drain migration path mutates `funderWallet` in place; the state object itself, and its `lockedAt`, is unchanged).
- In `inspectBundlerFunderTransaction`, right before a normal-mode same-band tiny group would pass the buy gate (i.e. right before the `🟢 ... Normal FeePayer Tiny Funding Group Buy Gate` notification), if the band is specifically **$2.00-$5.00** and `Date.now() - state.lockedAt` is **≥ 30 minutes**, the buy is skipped entirely: a `⏭️ ... Normal $2-$5 Band Buy Skipped — Too Stale` Telegram notice is sent, and the bot calls `resetForNewToken(true)` to fully drop this token and resume watching the followed wallet for the next one. The >$5-$10 band is unaffected and still buys regardless of elapsed time.
- This targets exactly the scenario reported: a $2-$5 band buy gate that took a very long time (tens of minutes) to form after the feePayer lock — by then the token has likely already moved without the bot, so buying in is skipped instead of chasing a stale signal.

## 2026-07-09

### Token Transfer mode: gate transfer-out buys on a prior dev swap-buy; auto-stop on zero balance; startup Telegram summary; more action logging

- `token-transfer-orchestrator.ts` no longer treats *any* outgoing SPL token transfer from the dev wallet as a buy signal. Detection is now two-step: (1) a `SWAP` tx where the dev wallet receives a mint it isn't already tracking marks that mint as a "candidate" (logged as `Dev wallet swap-bought a new token; now watching it for a transfer-out`); (2) only a plain `TRANSFER` of a *candidate* mint out of the dev wallet triggers the buy. This stops the bot from reacting to the dev wallet moving some old/unrelated token it happens to hold — only tokens it just bought are eligible. Candidates are reset whenever the dev address is changed (`setDevAddress`) and cleared once a transfer-out fires. New `getWatchedCandidateMints()` getter surfaces the current candidate set in the Token Transfer home/status cards.
- `clearActivePosition()` now also explicitly calls `stop()` (defensive — `isEnabled` is already false at that point in every caller) and takes an optional `reason`, so both a manual "Sell Position" click and an auto-detected sell leave the mode fully idle until Start is pressed again, with a clear log line either way.
- Added a periodic check (in `index.ts`'s `startMarketCapChecker`) that watches the trading wallet's on-chain balance for the held Token Transfer mint; if it's ever observed at zero (e.g. sold through some means other than the bot's own button), the position is auto-cleared, the mode is left stopped, and a Telegram notice is sent.
- Fixed a startup ordering bug: `telegramBot.start()` (which begins polling/handling Telegram updates) was called before `tokenTransferOrchestrator` was constructed and before the Insider bots' `buyTrigger`/`sellTrigger` listeners were wired up. Any update arriving in that window (e.g. a queued `/start` from before a restart) could hit `homeReply()` referencing an as-yet-unassigned `tokenTransferOrchestrator` and be silently dropped. Telegram polling now starts only after every orchestrator and bot listener is fully wired, right before the "Start active mode" step.
- Added a one-off Telegram "🟢 Bot Started" summary sent right after full startup (mode, Insider bot count/follow wallet/running state, Token Transfer dev address/buy SOL/running state, trading wallet, watched-wallet count, health port) — previously nothing was ever sent to Telegram on boot, only to the console/process logs, so there was no way to confirm from Telegram alone that the process came back up after a deploy/restart.
- Added previously-missing `log.info` calls for: wallet add/remove/pause/resume (`startWallet`/`stopWallet`/`pauseWallet`/`resumeWallet`), the `mode:insider`/`mode:tokentransfer` display-switch buttons, Insider per-bot settings changes (buy SOL, normal/low-funding buy SOL, exit %, bundler min/max USD), and Token Transfer's buy-SOL setter — these previously only produced a Telegram reply with no corresponding backend log line.

### Removed Reverse CopySell mode entirely

- Deleted the entire "Reverse CopySell" top-level Telegram mode: `reverse-copysell-orchestrator.ts` (`ReverseCopySellOrchestrator` class), the `reverse_copysell` value from `botMode`, the `REVERSE_COPYSELL_TARGET_WALLET` config field (`ServiceConfig.reverseCopySellTargetWallet`, `env.example`), and its home-screen card/buttons (`Reverse CopySell Bot` text, `Set Target Wallet` button, `reverse:set_target` callback, `reverseTargetWallet` pending-text-input handler).
- Removed the trading-wallet monitor plumbing that only existed to feed this mode: `tradingWalletMonitor`, `wireTradingWalletMonitor` (its `newToken`/`tokenExited` listeners called `handleTradingWalletBuy`/`handleTradingWalletExit`), and the `startReverseCopySellModeServices`/`stopReverseCopySellModeServices` lifecycle functions. `pauseWallet`/`resumeWallet`/`walletSummaryReply`'s trading-wallet special case (pausing/resuming `tradingWalletMonitor`) was removed accordingly — the trading wallet was never monitored via any other path.
- Removed the `reverseCopySellOrchestrator.on("sellTrigger", ...)` handler (auto-sell when the target wallet bought the same token) and the periodic "Reverse CopySell MC loop" rug-check in `startMarketCapChecker` (`checkAndSellIfLowMcap`'s `"reverse_copysell"` context branch is gone; it now only supports `"insider"`).
- `mode:insider` / `mode:tokentransfer` no longer call `stopReverseCopySellModeServices()` (there is nothing left to stop between those two).
- This is unrelated to, and does not affect, the separate per-watched-wallet "reverse-buy trigger" feature (`reverse:add`/`reverse:remove` callbacks, `db.addReverseBuyWallet`/`isReverseBuyWallet`, the `reverse_buy_wallets` table, `WalletFilterSettings.reverseBuySellTriggerEnabled`) — that toggle still works exactly as before and was kept as-is.

### Insider and Token Transfer modes now run concurrently

- `index.ts` previously treated `mode:insider` / `mode:tokentransfer` as mutually exclusive, mirroring the old Insider/Bundler toggle: switching to Token Transfer called `bot.pause()` on every Insider bot (fully stopping their follow-wallet monitors), and switching to Insider called `stopTokenTransferModeServices()`, which stopped the dev-wallet watch. At startup, only whichever mode `DEFAULT_BOT_MODE` pointed at was actually started — the other never ran at all. The Token Transfer buy-trigger handler also silently dropped any buy if `botMode !== "tokentransfer"` at the moment the transfer-out was detected.
- `botMode` is now purely a display selector for which card `/start`, `menu:refresh`, etc. render — it no longer starts/stops/pauses anything for Insider or Token Transfer. Both are controlled solely by their own explicit controls (Insider's `Stop`/`Resume` buttons; Token Transfer's `Start`/`Stop` buttons) and both are started at service boot regardless of `DEFAULT_BOT_MODE`, so either can be actively watching/holding/buying/selling at the same time without the other being paused, and switching which card is shown never drops a pending buy.
- Reverse CopySell is unchanged and remains mutually exclusive with the other two (switching to/from it still stops/starts its trading-wallet watch), since it fundamentally reacts to *any* buy on the trading wallet and would otherwise create feedback with Insider/Token Transfer's own buys.
- Removed the now-dead `stopTokenTransferModeServices` helper (its only callers were the mutual-exclusivity paths above).

### Replaced Bundler mode with Token Transfer mode

- Removed the entire "Bundler" top-level Telegram mode (`EarlyBundlerOrchestrator`, `early-bundler-orchestrator.ts`, all `bundler:*`/`mode:bundler` callbacks and `bundlerFollowWallet`/`bundlerBuySol`/`bundlerExitPercent` pending-action handlers, the `checkBundlerMcapFlow` auto-exit loop, and the DB-restart `getActiveEarlyBundlerPosition` fallback) and replaced it with a new manually-driven **Token Transfer** mode (`token-transfer-orchestrator.ts`, `TokenTransferOrchestrator`).
- Flow: set a dev wallet address from Telegram (`Set Dev Address`), then press `Start`. The bot polls that wallet's transactions (dedicated to Helius API key 4 / `INSIDER_HELIUS_API_KEY_4`, with `HELIUS_API_KEY` as a fallback) for a plain SPL token transfer-out (a `TRANSFER`-type tx where the dev wallet sends the token to another wallet — not a swap/sell). The moment a transfer-out is seen, the dev wallet watch stops automatically and the token is bought immediately with the mode's configured `Buy SOL` amount.
- There is no automatic sell of any kind (no MC exit target, no rug-below-$5k exit) — the position is only ever closed by pressing the `🔴 Sell Position` button on the buy card, mirroring the manual-close screenshot from Insider mode. The buy card also has a `🔄 Refresh P/L & MC` button, and after the buy, the token's market cap is continuously monitored (also via Helius key 4, through a dedicated `PumpReserveMarketCapClient`) purely for display on refresh — it never triggers a sell.
- Mode switch button/callback renamed `mode:bundler` → `mode:tokentransfer`; refresh-card context code `b` → `t`; new `sell:tokentransfer:<mint>` manual-sell callback (reuses the same `FilterFailEvent`/`pendingSells`/`executeSellAndNotify` pipeline as every other mode's manual/auto sells). `DEFAULT_BOT_MODE` now accepts `insider|tokentransfer` (was `insider|bundler`); `ServiceConfig['defaultBotMode']` and the Insider/Reverse-CopySell home-screen mode-switch buttons were updated to match.
- The generic "watched wallet reverse-buy" and `/addwallet` wallet-monitor plumbing that previously only ran during Bundler mode now runs during Token Transfer mode instead (same start/stop lifecycle, just re-gated on the new mode name) — this is unrelated to the dev-wallet watch and unchanged in behavior.

### Fixed post-buy log spam and duplicate recipient-watch rejections

- `insider-bot.ts`: `syncBundlerFunderTransactions` / `stopBundlerFunderSourceDiscovery` logged "Stopped shared feePayer transfer-out discovery" on *every* poll tick after the recipient buy cap was reached (i.e. for the entire remaining life of a held position, every ~2s), because the stop routine had no idempotency guard. Added a `discoveryStopped` flag on `BundlerFunderWatchState`; the stop routine and its callers now short-circuit once discovery has already been stopped, so the log (and the redundant unsubscribe/timer-clear work) fires exactly once per token.
- `insider-bot.ts`: `syncBundlerFunderTransactions`'s per-transaction loop kept calling `inspectBundlerFunderTransaction` for the rest of a fetched batch even after the recipient buy cap was already reached, which caused the normal-mode tiny-transfer-group logic to keep recomputing the same rejected candidates (e.g. ~30 duplicate "Shared feePayer recipient watch cap reached" warnings for the same 2 signatures within a few milliseconds). The loop now breaks as soon as the cap is reached.
- `index.ts`: the background active-position balance/quote refresh (`startMarketCapChecker`) logged an `ERROR` every retry when a freshly-bought mint's token account wasn't indexed by the RPC node yet (`SolanaJSONRPCError -32602 "could not find mint"`). This is a transient, self-resolving condition (it clears up within a few seconds once the RPC indexes the new position) and is now logged as a `WARN` instead, while genuine unexpected errors still log at `ERROR`.

## 2026-07-08

### Removed legacy Axiom/authority-pattern-matching code

- Deleted the entire dead Axiom single-buy trader scan and lookup-table-authority pattern-matching subsystem from `insider-bot.ts` (~2,600 lines): `AxiomWatchedWallet`, `AuthorityMonitorState`, `AuthorityPatternWalletState`, `AuthorityCandidateWallet`, `LargeBuyerWatchState`, `ExistingAtaWalletSolBalance`, and `SimilarSolBalanceGroup` interfaces; all `axiom*`/`authority*`/`largeBuyer*` state fields, timers, and WS subscription maps; and ~30 methods (`startAxiomAtaPollLoop`, `checkAxiomWatchedWalletAtaExits`, `startAuthorityTriggerFlow`, `syncAuthorityTransactions`, `syncLargeBuyerAtaBalances`, `scanAxiomSingleBuyTradersPreBuy/PostBuy`, etc.).
- This flow had already been superseded by the shared feePayer tracing system (see below) and was unreachable dead code — its only entry points were behind conditions (`this.monitoredWallet`, `this.authorityMonitor`) that current buy flows never set.
- Trimmed `InsiderBot`'s constructor from 3 `GmgnClient` params to 1 (`bundlerGmgnClient` and `preBuyAxiomGmgnClient` were only used by the removed Axiom scans); updated the `index.ts` instantiation to match.
- `rearmPositionMonitoringAfterSellFailure` and `markPositionBought` now re-sync the shared feePayer / funder-recipient watchers instead of the removed authority monitor.
- Behavior is unchanged for the live flow — the shared feePayer buy/sell/exit logic was not touched.

### Shared feePayer tracing rewrite (retroactive summary)

The insider bot's core detection/entry logic was rewritten to trace the shared **feePayer** funding early bundler buyers, replacing the old GMGN/Axiom trader-scan and lookup-table-authority pattern-matching approach:

- **Normal-funding mode** (feePayer holds ≥15 SOL): watches small ($2–$10) same-band transfer-outs from the feePayer to recipient wallets to trigger buys. Exit is +90% MC for the $2–$5 band or +180% MC for the $5–$10 band.
- **Low-funding mode** (feePayer holds <15 SOL): uses its own tiny same-band grouping, gated by a dev-wallet buy-after-create event, with a fixed $25k MC exit.
- **Live SOL-zero exit**: real-time sell trigger via `onAccountChange` when a recipient wallet's SOL balance drops to zero.
- **Shared feePayer migration**: automatically switches the monitored feePayer if the current one drains >100 SOL, so tracking follows the active funding wallet.
- **Helius API pooling**: requests rotate across multiple Helius API keys with backoff and rate-limit handling for reliability.

## 2026-06-15

### Telegram refresh P/L & MC fixes

- Refresh handler resolves insider bot index from callback (`i0`, `i1`), shows entry/current/ATH/exit MC, cost basis, and quote errors.
- Token balance for P/L quotes now sums Token + Token-2022 accounts (fixes missing balance / stuck "Calculating...").
- Refresh timestamp uses ISO format so Telegram edits always apply; toast shown when already up to date.

### Axiom scan excludes multi-buy wallets

- Axiom/empty trader filter now requires exactly `buy_tx_count_cur === 1`; multi-buy entries are counted in `skippedMultiBuy` logs.

### Axiom single-buy scan limit increased to 50

- Pre-buy and post-buy `fetchBuyVolumeTraders` scans now request limit 50 (was 20) for a wider view.

### Axiom/empty single-buy trader scan replaces profitable-trader GMGN scans

- Pre-buy and post-buy GMGN scans now use `fetchBuyVolumeTraders` (no `tag`, order-by `buy_volume_cur`, limit 50).
- Filters: single-buy (`buy_tx_count_cur ≤ 1`), buy USD in `INSIDER_BUNDLER_BUY_MIN_USD`–`INSIDER_BUNDLER_BUY_MAX_USD`, `tags` is `["axiom"]` or `[]`.
- Skip-list exclusions unchanged (initial 4 insiders, dev wallet, transfer-in sources).
- Logs include `validCount`, `soldAmongValid`, `soldPositionRatio`, `soldWallets`, and `holdingWallets`.
- Post-buy sell triggers when all eligible axiom/empty single-buy wallets in range have fully exited.

## 2026-06-14

### Pre-buy profitable trader scan — buy_volume_cur + $100 exit filter

- Pre-buy GMGN scan (key 3) uses `tag=bundler`, orders by `buy_volume_cur` with limit 20.
- Pre-buy logs count wallets that pass skip-list exclusions, bought above $100, and sold all positions (`matchingWallets`, `soldPositionRatio`).
- Post-buy profitable scan uses `tag=bundler`, order-by `profit`, top-5 exit trigger.

## 2026-06-11

### GMGN_API_KEY_3 — pre-buy profitable trader scan

- `GMGN_API_KEY_3` (falls back to key 2) runs profitable-trader GMGN scan during pre-buy.
- Pre-buy logs use `[pre-buy]` label; no sell trigger.
- Post-buy profitable scan continues on `GMGN_API_KEY_2` with sell trigger when top 5 eligible wallets fully exit.

### Pre-buy stop + profitable trader exit trigger

- GMGN bundler scan and insider/transfer monitoring stop as soon as the buy gate passes (before MC fetch / buy execution).
- Post-buy profitable-trader scan: exclusions applied first; `soldPositionRatio` is sold/valid (e.g. `6/16`). `topExitedRatio` tracks current top 5 eligible wallets for sell trigger.
- Sell when all 5 of the current top profitable wallets have fully exited on that scan; logs and Telegram include those wallets and reason.

### Bundler transfer-out sell

- Post-buy bundler monitoring now triggers an immediate sell when either tracked wallet transfers the token out (Helius `TRANSFER` + `fromUserAccount`).
- Existing rule remains: sell when both bundlers have sold at least once.

### Bundler match race (single vs multi)

- Two parallel bundler tracks: **single-buy** (`buy_tx_count_cur ≤ 1`) and **multi-buy** (`> 1`), both in USD range.
- First-seen wallets are locked at discovery; snapshots frozen so later buys can't change locked matches.
- Whichever track locks **2 wallets first** triggers the bundler gate; buy proceeds after insider sells are ready.
- Buy notification classifies trigger as "Single-buy pair" or "Multi-buy pair".
- Rug-reset Telegram card when MC falls below $5k during pre-buy monitoring.

### Insider parallel buy gate (stop-and-wait)

- `INSIDER_REQUIRED_SELLS` env var (default 5) configures how many insider sells are needed before buy.
- After lowest insider is found, insider monitoring and GMGN bundler scan run in parallel every 2s.
- Whichever finishes first stops its own monitor and waits; buy fires only when both insider sells and 2 bundler matches are ready.
- Post-buy bundler sell trigger unchanged: sell when each matched bundler has sold once.

### Insider parallel buy gate

- GMGN bundler scan starts immediately when lowest insider is found (parallel with insider monitoring).
- Buy only triggers when both insider sell threshold and 2 bundler matches ($110–$120 default) are ready.
- Post-buy: sell when both tracked bundlers have sold at least once.
- Buy/sell tx logs include running totals per token.
- Bot 1 and Bot 2 auto-resume in parallel on insider mode start.
- Removed dev wallet sell triggers.

### Insider bundler buy flow

- After 5 insider sells, scan GMGN bundler traders (limit 20, API key 2) for buy_volume_cur in configurable USD range.
- Buy when 2 bundler wallets match; then monitor both via WSS + Helius until each sells once before resuming follow wallet.
- Cross-bot mint lock prevents Bot 1 and Bot 2 from working the same token simultaneously.
- Dev wallet sell triggers: 3rd buy after mint, or sell amount exceeding initial mint amount; full dev exit uses ATH % MC.
- Removed legacy PnL-at-transfer, dev $10 cap, and $40 buy threshold logic.
- Telegram: bundler min/max USD settings added.

### Insider mode rewrite

- Follow-wallet monitoring now pauses on new token buy and switches to lowest early insider wallet detection via Helius.
- Insider wallet activity (buy/sell/transfer in/out) is tracked to trigger bot buys after sell signals and positive wallet PnL.
- Transfer-out events chain monitoring to the recipient wallet with Helius history sync.
- Rug threshold raised to $5,000 market cap; bot resets back to follow-wallet monitoring after rug, failed filters, or sell completion.
- Exit strategy now uses ATH price converted to market cap (default +40% from entry, configurable in Telegram).
- Telegram insider menu simplified to follow wallet, buy SOL, and exit % settings.

