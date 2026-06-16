# Alpaca Screener

Minimal perpetual screener for Alpaca tradable US equities.

It writes these columns to Google Sheets:

```text
symbol, close, sma_200, sma_50, pos_52w, dollar_vol_m
```

Behavior:

- Checks Alpaca market clock before each run.
- If the market is closed, clears the Screener tab so downstream systems see a blank sheet.
- Set `ALLOW_OFF_HOURS_DATA_PULL=true` to populate the sheet during off-hours for development/programming work.
- If the market is open, leaves the old sheet data visible while it builds the next full screener.
- After a successful full run, updates the sheet.
- If a run fails badly, it does not overwrite the prior sheet.
- Runs continuously on Railway.

## Deploy notes

1. Create a new Google Sheet.
2. Rename the first tab to `Screener`, or set `GOOGLE_WORKSHEET_NAME` to whatever tab name you want.
3. Create a Google Cloud service account with Sheets API access.
4. Create a JSON key for the service account.
5. Share the Google Sheet with the service account `client_email` as Editor.
6. Add the environment variables from `.env.example` to Railway.
7. Deploy this repo to Railway.

## Local run

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Health endpoint:

```text
/healthz
```

## Off-hours development mode

By default, the screener blanks the sheet when Alpaca says the market is closed:

```text
ALLOW_OFF_HOURS_DATA_PULL=false
```

For development, set this in Railway to keep pulling the latest available daily bars even after hours:

```text
ALLOW_OFF_HOURS_DATA_PULL=true
```

When this is enabled, the bot still checks the Alpaca clock and records `market_open` in `/healthz`, but it does not let a closed market prevent a screener run. If the clock request itself fails, the bot will still attempt the screener run instead of clearing the sheet.
