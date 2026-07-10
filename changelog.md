# Changelog

## 2026-07-11

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
