# Changelog

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
