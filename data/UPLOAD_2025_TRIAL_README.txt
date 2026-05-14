2025 Trial Upload Bundle

This bundle is intended to be unpacked at the root of the racing_assistant repo.

Included:
- data/punting_form_monthly_exports/
  - 2025 monthly Punting Form zip exports for the overlap months:
    Jan, Feb, Mar, May, Jun, Jul, Aug, Sep, Dec
- data/betfair_history/BASIC/2025/
  - Betfair historical market files for the same overlap months:
    Jan, Feb, Mar, May, Jun, Jul, Aug, Sep, Dec

Expected server workflow after upload and extraction:

1. cd /home/samueljasonhines/racing_assistant
2. source venv/bin/activate
3. git pull
4. tar -xzf racing_assistant_2025_trial_inputs.tar.gz -C /home/samueljasonhines/racing_assistant
5. python3 -m app.research.build_2025_trial_dataset

Notes:
- The importer now prefers repo-local monthly exports under:
  data/punting_form_monthly_exports
- If needed, it still falls back to /Users/sam/Downloads on Mac.
