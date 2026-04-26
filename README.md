# Weather Ensemble Dago

Single-file multi-source weather ensemble collector for Dago, Bandung.

The project collects hourly forecasts from BMKG, Open-Meteo model feeds, and MET Norway, then builds ensemble CSV outputs for fixed target hours:

- `10:00`
- `13:00`
- `16:00`
- `19:00`
- `22:00`

## Main Script

- [weather_ensemble_dago.py](./weather_ensemble_dago.py)

## Features

- Single Python script
- Multi-source forecast collection
- Ensemble weighting
- Confidence score and coverage status
- Historical observation sync
- Historical evaluation and automatic weight derivation
- Source health tracking
- Runtime log files
- Raw payload archival
- GitHub Actions friendly

## Requirements

- Python `3.11+` recommended
- `pip install -r requirements.txt`

## Local Usage

Run a built-in sanity check:

```bash
python weather_ensemble_dago.py --mode self-test
```

Run the next-day forecast:

```bash
python weather_ensemble_dago.py --mode forecast
```

Sync historical observations:

```bash
python weather_ensemble_dago.py --mode sync-observations --lookback-days 14
```

Evaluate historical forecast performance and derive weights:

```bash
python weather_ensemble_dago.py --mode evaluate --lookback-days 14
```

Import your own observation CSV:

```bash
python weather_ensemble_dago.py --mode import-observations --observations-csv observations.csv
```

## Important Outputs

These are generated at runtime and ignored by Git:

- `forecast_dago.csv`
- `ensemble_dago.csv`
- `source_status_dago.csv`
- `canva_dago.csv`
- `bmkg_dago.csv`
- `run_summary.json`
- `source_health.json`
- `source_weights.json`

Supporting folders:

- `logs/`
- `raw_payloads/`
- `observations/`
- `reports/`

## GitHub Actions

This repo includes a scheduled GitHub Actions workflow:

- runs every day
- runs `self-test`
- runs `forecast`
- uploads generated outputs as workflow artifacts

The workflow uses UTC scheduling. The included cron is set to match `23:15` Asia/Jakarta:

- `15 16 * * *`

## Notes

- Runtime outputs are uploaded as workflow artifacts, not auto-committed to the repo.
- If you want daily result commits later, that can be added, but it usually makes the repo noisy.
- `source_weights.json` and `source_health.json` are generated at runtime. If you want stable tuned weights in Git, you can commit them manually after a good evaluation run.
