# Changelog

## 2026-06-11

### Insider mode rewrite

- Follow-wallet monitoring now pauses on new token buy and switches to lowest early insider wallet detection via Helius.
- Insider wallet activity (buy/sell/transfer in/out) is tracked to trigger bot buys after sell signals and positive wallet PnL.
- Transfer-out events chain monitoring to the recipient wallet with Helius history sync.
- Rug threshold raised to $5,000 market cap; bot resets back to follow-wallet monitoring after rug, failed filters, or sell completion.
- Exit strategy now uses ATH price converted to market cap (default +40% from entry, configurable in Telegram).
- Telegram insider menu simplified to follow wallet, buy SOL, and exit % settings.
