# Using Electronic Health Record Audit Logs to Identify Pediatric Intensive Care Unit Teams

**Authors:** Liem M. Nguyen, BS¹; Stefanie S. Sebok-Syer¹, PhD; Dane Jacobson, MD¹; Karley Mariano, MSN, CPNP-AC¹; John Grunyk, MD¹; Shelby Burk, MS¹; Yasaman Nourkhalaj, BS¹; Kimberley Kirk, BA¹; Jochen Profit, MD¹, MPH; Christian Rose, MD¹; A Jay Holmgren, PhD²; Thomas Kannampallil, PhD³; Daniel Tawfik, MD, MS¹ 


Stanford University School of Medicine¹ · UCSF School of Medicine² · Washington University School of Medicine³

Corresponding Author: Liem M. Nguyen, liemn@stanford.edu

---

This repository contains code for identifying pediatric ICU teams (nurse, frontline provider, attending) from EHR audit logs. Two algorithms 1) clinically-informed heuristics and 2) LCS (Longitudinal Contribution Score) assign roles per patient-day using activity features and note-signing patterns. Attending identification is frontline-dependent (linked by note-modify actions via audit logs).

## Pipeline Overview

1. **SQL (BigQuery):** Build activity features, note signers, and REDCap-enriched gold standard tables.
2. **Python:** Load data, run algorithms, compare to gold standard, bootstrap for confidence intervals, generate tables.

## Repository Structure

| Path | Description |
|------|-------------|
| `sql/run_development.sql` | Development phase: PICU |
| `sql/run_validation.sql` | Validation phase: PICU/NICU/CVICU |
| `python/config.py` | BigQuery config, provider types, algorithm constants |
| `python/algorithms.py` | Clinically-informed heuristics and LCS team identification logic |
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

Results are written to `results/` with timestamps. Logs go to `logs/`.

AI-Use Disclosure: Claude Sonnet 4.5/4.6 were used throughout the repository for code documentation and organization. No models had any access to PHI. 
