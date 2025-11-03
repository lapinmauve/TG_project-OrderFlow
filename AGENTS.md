# Repository Guidelines

## Project Structure & Module Organization
- `OrderFlow_base.py` holds the headless market-data collector: connection bootstrap, rolling numpy buffer, and sliding window aggregators.
- `OrdreFLow_base_vis.py` extends the collector with Matplotlib visualisation for order-flow and level-1 liquidity.
- `TG_Reference_code.py` is a legacy reference from the original IBKR script; consult it only for API usage patterns.
- Logs and artifacts are user-defined; keep generated files under `logs/` or `tmp/` folders to stay out of version control.

## Build, Test, and Development Commands
- `python OrderFlow_base.py --symbol UVXY` runs the base collector and prints tick snapshots plus window summaries.
- `python OrdreFLow_base_vis.py --symbol UVXY --price-bin-size 0.02` opens the live dashboard for a single ticker.
- `python -m compileall OrderFlow_base.py OrdreFLow_base_vis.py` performs a quick syntax check; run after edits.
- Set `IB_HOST`, `IB_PORT`, and `IB_CLIENT_ID` in `.env` when deviating from the defaults (`127.0.0.1`, `7497`, `1`).

## Coding Style & Naming Conventions
- Python 3.9+ with standard 4-space indentation. Follow PEP8 unless a deviation improves readability.
- Functions and classes use snake_case and PascalCase respectively; keep filenames in snake_case.
- Prefer `loguru` for runtime messaging; keep stdout logs concise and leverage structured logging sinks when possible.
- Import order: stdlib → third-party → local modules.

## Testing Guidelines
- No automated tests exist yet; validate changes by running the collector and watching for stable tick ingestion/output.
- When adding logic, provide a reproducible command sequence (e.g., sample `--symbol` run) in the PR description.
- Consider adding doctests or lightweight unit tests under `tests/` if you introduce reusable utilities.

## Commit & Pull Request Guidelines
- Use clear, imperative commit messages (e.g., “Add sliding histogram for mid window”) and keep commits scoped.
- For PRs, include: summary of changes, verification steps (commands run), screenshots for visual updates, and links to tickets if applicable.
- Ensure PRs stay focused (one feature or fix) and document any new configuration keys or environment variables.

## Security & Configuration Tips
- API credentials stay outside the repo; rely on `.env` files or OS keychains.
- Client IDs must be unique per running IBKR session; avoid reusing IDs already attached to a live connection.

