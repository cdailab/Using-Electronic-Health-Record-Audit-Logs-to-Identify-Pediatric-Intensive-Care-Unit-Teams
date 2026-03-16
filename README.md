# Using Electronic Health Record Audit Logs to Identify Pediatric Intensive Care Unit Teams

**Authors:** Liem M. Nguyen, BS; Stefanie S. Sebok-Syer, PhD; Dane Jacobson, MD; Karley Mariano, MSN, CPNP-AC; John Grunyk, MD; Shelby Burk, MS; Yasaman Nourkhalaj, BS; Kimberley Kirk, BA; Jochen Profit, MD, MPH; Christian Rose, MD; A Jay Holmgren, PhD; Thomas Kannampallil, PhD; Daniel Tawfik, MD, MS  
Stanford University School of Medicine¹ · UCSF School of Medicine² · Washington University School of Medicine³

---

This repository contains code for identifying PICU teams (nurse, frontline provider, attending) from EHR audit logs. Two algorithms—**clinically-informed heuristic** and **LCS** (Longitudinal Contribution Score)—assign roles per patient-day using activity features and note-signing patterns. Attending identification is frontline-dependent (linked by note-modify actions via audit logs).

## Pipeline Overview

1. **SQL (BigQuery):** Build activity features, note signers, and REDCap-enriched gold standard tables.
2. **Python:** Load data, run algorithms, compare to gold standard, bootstrap for confidence intervals, generate tables.

## Repository Structure

| Path | Description |
|------|-------------|
| `sql/run_development.sql` | Development phase: PICU, first-round REDCap |
| `sql/run_validation.sql` | Validation phase: PICU/NICU/CVICU, second-round REDCap |
| `python/config.py` | BigQuery config, provider types, algorithm constants |
| `python/algorithms.py` | Heuristic and LCS team identification logic |
| `python/data_loader.py` | BigQuery load and preprocessing |
| `python/evaluation.py` | Gold-standard comparison and accuracy metrics |
| `python/bootstrap.py` | Patient-day bootstrap |
| `python/run_development.py` | Development pipeline entry point |
| `python/run_validation.py` | Validation pipeline entry point |
| `notebooks/primary_teams_analysis.ipynb` | Full analysis with caching |

## Setup

1. **Python:** `pandas`, `numpy`, `google-cloud-bigquery`, `tqdm`
2. **Config:** Edit `config.py` — set `BQ_PROJECT_DATASET` and replace `INSERT_TABLE_NAME` placeholders in SQL with your BigQuery project/dataset.
3. **BigQuery:** Run SQL scripts before Python. Required tables: REDCap team observation ground truth, reference tables (attending, frontline, nurse) to match redcap names to, Clarity SER for user IDs and role information, Clarity access log and associated detailed tables. 

## Running the Analysis

```bash
# Development (PICU only)
python python/run_development.py

# Validation (PICU, NICU, CVICU)
python python/run_validation.py
```

Results are written to `results/` with timestamps. Logs go to `logs/`.
