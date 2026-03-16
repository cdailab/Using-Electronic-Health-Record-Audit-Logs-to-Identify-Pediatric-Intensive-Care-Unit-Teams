#!/usr/bin/env python
# coding: utf-8

# development phase entry point (picu, first round)
# pipeline: load -> preprocess -> run algorithms (heuristic + lcs, frontline-dep attending) ->
#           evaluate -> dual bootstrap -> generate tables

import gc
import logging
import os
import pickle
from datetime import datetime

import pandas as pd

from config import DEVELOPMENT_CONFIG, ROLES
from data_loader import load_data, preprocess_data
from algorithms import run_all_algorithms
from evaluation import (
    compare_with_gold_standard, compute_accuracy,
    compute_return_rate, compute_conditional_accuracy,
)
from bootstrap import run_dual_bootstrap
from table_generator import create_by_role_table

# -------------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------------
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"development_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


def main():
    """run the full development phase analysis

    returns:
        dict with all results (tables, comparisons, bootstrap)
    """
    logger.info("=== DEVELOPMENT PHASE ANALYSIS ===")
    logger.info(f"config: {DEVELOPMENT_CONFIG['phase']} | units: {DEVELOPMENT_CONFIG['units']}")
    start_time = datetime.now()

    results_dir = os.path.join(
        os.path.dirname(__file__), '..', 'results',
        f"development_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    os.makedirs(results_dir, exist_ok=True)

    # -------------------------------------------------------
    # STEP 1: LOAD DATA
    # -------------------------------------------------------
    activity_data, note_signers, redcap_data = load_data(DEVELOPMENT_CONFIG)

    # -------------------------------------------------------
    # STEP 2: PREPROCESS (development is picu-only; unit=None = no unit filtering)
    # -------------------------------------------------------
    merged_data, note_signers, redcap_proc = preprocess_data(
        activity_data, note_signers, redcap_data, unit=None
    )
    if merged_data is None:
        logger.error("preprocessing failed -- aborting")
        return None

    del activity_data
    gc.collect()

    # -------------------------------------------------------
    # STEP 3: RUN ALL ALGORITHMS (heuristic + lcs, attending is frontline-dependent)
    # -------------------------------------------------------
    alg_results = run_all_algorithms(merged_data, note_signers)

    h_results   = alg_results['heuristic']
    lcs_results = alg_results['lcs']

    # -------------------------------------------------------
    # STEP 4: COMPARE WITH GOLD STANDARD
    # -------------------------------------------------------
    h_comp   = compare_with_gold_standard(h_results,   redcap_proc)
    lcs_comp = compare_with_gold_standard(lcs_results, redcap_proc)

    if h_comp.empty or lcs_comp.empty:
        logger.error("gold standard comparison failed -- aborting")
        return None

    n = len(h_comp)
    logger.info(f"evaluating on {n} patient-days")

    # -------------------------------------------------------
    # STEP 5: PRIMARY METRICS (accuracy)
    # -------------------------------------------------------
    h_acc   = compute_accuracy(h_comp)
    lcs_acc = compute_accuracy(lcs_comp)

    logger.info("accuracy (primary metric, attending is frontline-dependent):")
    for role in ROLES:
        logger.info(
            f"  {role}: heuristic={h_acc.get(role)*100:.1f}% | lcs={lcs_acc.get(role)*100:.1f}%"
        )

    # -------------------------------------------------------
    # STEP 6: DUAL BOOTSTRAP
    # -------------------------------------------------------
    boot_results = {}
    if DEVELOPMENT_CONFIG.get('run_bootstrap', True):
        logger.info("running patient-day bootstrap...")
        boot_results = run_dual_bootstrap(
            h_comp, lcs_comp,
            n_iter=DEVELOPMENT_CONFIG.get('n_bootstrap', 2500),
        )

    # -------------------------------------------------------
    # STEP 7: GENERATE TABLES
    # -------------------------------------------------------
    by_role_table = create_by_role_table(h_acc, lcs_acc, boot_results, n)
    logger.info("\n=== DEVELOPMENT: BY-ROLE ACCURACY ===")
    print(by_role_table.to_string(index=False))

    # -------------------------------------------------------
    # STEP 8: SAVE OUTPUTS
    # -------------------------------------------------------
    by_role_table.to_csv(os.path.join(results_dir, 'by_role.csv'), index=False)

    complete_results = {
        'phase':            'development',
        'n':                n,
        'heuristic': {'acc': h_acc,   'comp': h_comp},
        'lcs':       {'acc': lcs_acc, 'comp': lcs_comp},
        'boot':             boot_results,
        'runtime_minutes':  (datetime.now() - start_time).total_seconds() / 60,
    }
    with open(os.path.join(results_dir, 'complete_results.pkl'), 'wb') as f:
        pickle.dump(complete_results, f)

    elapsed = (datetime.now() - start_time).total_seconds() / 60
    logger.info(f"\ncomplete in {elapsed:.1f} minutes | results: {results_dir}")
    return complete_results


if __name__ == '__main__':
    main()
