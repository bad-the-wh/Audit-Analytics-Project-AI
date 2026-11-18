# Audit Analytics Lakehouse

A Databricks pipeline that loads a company's general ledger, checks its data quality and runs eight
journal-entry tests over **every** entry, the way an external auditor would, then ranks the riskiest
postings so a reviewer knows where to start.

The ledger is synthetic: a generator builds a year of bookkeeping for a fictional company and plants
known anomalies and data defects in it. Because the generator writes down everything it planted, the
pipeline can be scored: it currently detects **100 % of planted anomalies with 0 false positives**,
and quarantines every defective entry and nothing else.

## What it does

```
generate_ledger.py ──► CSV files ──► Unity Catalog volume
                                         │
                       01 Bronze   raw files as Delta tables, all strings, with source file and load time
                                         │
                       02 Silver   typed data + 6 data-quality rules
                                   ├── silver_journal_lines   (clean entries)
                                   ├── silver_quarantine      (whole entry held back, with the failed rule codes)
                                   └── dq_results             (pass rate per rule, appended each run)
                                         │
                       03 Gold     fact_journal_lines, fact_journal_entries, dim_accounts, dim_users, dim_periods
                                         │
                       04 Tests    8 journal-entry tests + weighted risk score ──► gold_jet_results
                                         │
                       05 Validate recall / precision against the answer key; the job fails if anything is missed
```

### Data-quality rules (silver)

| Code | Rule |
|---|---|
| DQ01 | Account exists in the chart of accounts |
| DQ02 | User is filled and exists in the user list |
| DQ03 | Posting date is a valid date that matches the fiscal period |
| DQ04 | Creation timestamp is valid |
| DQ05 | Exactly one of debit / credit is filled and positive |
| DQ06 | Amounts are numeric and not negative |

If any line of an entry fails, the **whole entry** goes to quarantine: posting half an entry would
unbalance the ledger. `bronze rows = silver rows + quarantine rows` is asserted on every run.

### Journal-entry tests (gold)

| Test | Flags an entry when | Weight |
|---|---|---|
| unbalanced_entry | total debits ≠ total credits | 5 |
| duplicate_entry | same date, description and lines as an earlier entry | 4 |
| post_close_entry | posted after the period's close date | 4 |
| below_approval_limit | amount is 95–100 % of the user's approval limit (splitting to avoid approval) | 3 |
| unusual_user_account | the department rarely or never posts to that account | 3 |
| round_amount | ≥ 10,000 and a multiple of 1,000 | 2 |
| weekend_posting | posted by a person on a Saturday or Sunday | 2 |
| late_night_posting | created by a person between 22:00 and 05:00 | 2 |

`risk_score` is the sum of the weights of the tests an entry fails. System postings (depreciation batch)
are exempt from the weekend and late-night tests.

### Latest validation (full data set, seed 2025)

212,164 journal lines · 76,841 entries · 369 planted anomalies · 44 defective entries (149 lines)

| Test | Planted | Found | Missed | False positives |
|---|---|---|---|---|
| unbalanced_entry | 20 | 20 | 0 | 0 |
| duplicate_entry | 30 | 30 | 0 | 0 |
| post_close_entry | 25 | 25 | 0 | 0 |
| below_approval_limit | 25 | 25 | 0 | 0 |
| unusual_user_account | 20 | 20 | 0 | 0 |
| round_amount | 38 | 38 | 0 | 0 |
| weekend_posting | 48 | 48 | 0 | 0 |
| late_night_posting | 43 | 43 | 0 | 0 |

Quarantine: 44 of 44 defective entries held back, 0 clean entries held back by mistake.

## Run it on Databricks (Free Edition works)

1. **Generate the data** on your machine:
   ```bash
   python data_generator/generate_ledger.py --out data/raw
   ```
2. **Import the repo**: *Workspace → Create → Git folder* and paste the GitHub URL.
3. Run `notebooks/00_setup` on serverless compute. It creates the `workspace.audit` schema and the `raw` volume.
4. **Upload** the six CSV files from `data/raw/` to the volume
   (*Catalog → workspace → audit → Volumes → raw → Upload to this volume*).
5. Run notebooks `01` to `05` in order, or create a **job** with one task per notebook,
   each depending on the previous one. Notebook `05` fails the job if any anomaly is missed.

The catalog and schema are widgets (`catalog`, `schema`), so the same notebooks run in another workspace
without code changes.

## Run it locally

Needs Python 3.10+ and Java 17+.

```bash
pip install -r requirements-dev.txt
python data_generator/generate_ledger.py --out data/raw
python scripts/run_local.py --raw data/raw      # whole pipeline + validation tables
python -m pytest -v                              # 18 tests
```

## Tests and CI

- `tests/test_dq.py`: each data-quality rule on small hand-made entries, and the whole-entry quarantine.
- `tests/test_jet.py`: each journal-entry test, including edge cases (approval-limit boundaries,
  system user exemptions, a duplicate created with a new entry id).
- `tests/test_end_to_end.py`: generates a small ledger, runs the full pipeline and requires zero missed
  anomalies, zero false positives and an exact quarantine.

`azure-pipelines.yml` runs the suite on every push and pull request to `main` in Azure DevOps and
publishes the results.

## Project layout

```
data_generator/generate_ledger.py   synthetic ledger + answer keys
src/audit_lakehouse/                bronze, dq, gold, jet, validate (plain PySpark, tested locally)
notebooks/                          Databricks notebooks 00–05 that call the library
scripts/run_local.py                same pipeline on a local SparkSession
tests/                              pytest suite
azure-pipelines.yml                 CI
```

## Roadmap

- Benford's-law first-digit test in R (the generator already plants fabricated amounts for one user)
- Power BI report on the gold tables: risk ranking, flags by user and period, data-quality trend
- Azure Data Factory pipeline to land the files and trigger the Databricks job
- Assistant that answers questions about the flagged entries with retrieval over the results
