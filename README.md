# Alpaca Screener Bot - Extended Session Gate Patch

This is the screener-only repo. It writes only columns A:F and preserves column G for a Google Sheets scoring formula.

## Output columns

A:F are owned by the bot:

```text
symbol | close | sma_200 | sma_50 | pos_52w | dollar_vol_m
```

Column G is intentionally never written or cleared by the bot.

## Session behavior

The bot pulls data when one of these is true:

1. Alpaca regular-market clock says the market is open.
2. `ALLOW_EXTENDED_HOURS_DATA_PULL=true`, the current New York time is within the configured extended window, and Alpaca calendar confirms today is a trading day.
3. `ALLOW_OFF_HOURS_DATA_PULL=true`, which is a development override.

If none are true, the bot clears only A:F and preserves G.

This patch changes when the daily-bar screener runs. It does not change the data type to live quote/intraday data.

## New environment variables

```text
ALLOW_EXTENDED_HOURS_DATA_PULL=true
MARKET_TIMEZONE=America/New_York
EXTENDED_HOURS_START=04:00
EXTENDED_HOURS_END=20:00
ALLOW_OFF_HOURS_DATA_PULL=false
```

`ALLOW_EXTENDED_HOURS_DATA_PULL` defaults to `false` in code. Set it explicitly in Railway when you want the screener populated during extended sessions.

`ALLOW_OFF_HOURS_DATA_PULL` is stronger and should generally be used only for development/testing.

## Existing important behavior preserved

- Recent IPOs are included if `close` and `dollar_vol_m` are present and greater than zero.
- `sma_50`, `sma_200`, and `pos_52w` use whatever valid history exists.
- Failed symbol chunks are omitted from the current refresh; successful chunks still write.
- Old leftover rows in A:F are cleared after each write.
- Column G formulas are preserved.
