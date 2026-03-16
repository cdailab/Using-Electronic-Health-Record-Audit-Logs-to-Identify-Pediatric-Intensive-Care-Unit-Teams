#!/usr/bin/env python
# coding: utf-8

# validation phase entry point (picu, nicu, cvicu -- second round)
# pipeline: load -> per-unit loop (preprocess, algorithms, evaluate, bootstrap) ->
#           cross-unit combined analysis -> generate tables

import gc
import logging
import os
import pickle
from datetime import datetime

import pandas as pd

from config import VALIDATION_CONFIG, ROLES
from data_loader import load_data, preprocess_data
from algorithms import run_all_algorithms
from evaluation import (
    compare_with_gold_standard, compute_accuracy,
    compute_return_rate, compute_conditional_accuracy,
)
from bootstrap import run_dual_bootstrap
from table_generator import (
    create_by_role_table,
    create_by_unit_table,
    create_role_x_unit_table,
)

# -------------------------------------------------------
# LOGGING SETUP
# -------------------------------------------------------
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


def run_unit_analysis(unit, activity_data, note_signers, redcap_data,
                      run_bootstrap=True, n_bootstrap=2500):
    """run the full analysis pipeline for a single unit"""
    logger.info(f"--- processing {unit} ---")

    merged_data, note_signers_unit, redcap_unit = preprocess_data(
        activity_data, note_signers, redcap_data, unit=unit
    )
    if merged_data is None:
        return {'unit': unit, 'status': 'error', 'error': 'preprocessing failed'}

    alg_results = run_all_algorithms(merged_data, note_signers_unit)
    h_results   = alg_results['heuristic']
    lcs_results = alg_results['lcs']

    h_comp   = compare_with_gold_standard(h_results,   redcap_unit)
    lcs_comp = compare_with_gold_standard(lcs_results, redcap_unit)

    if h_comp.empty or lcs_comp.empty:
        return {'unit': unit, 'status': 'error', 'error': 'gold standard comparison failed'}

    n       = len(h_comp)
    h_acc   = compute_accuracy(h_comp)
    lcs_acc = compute_accuracy(lcs_comp)

    logger.info(f"{unit}: {n} patient-days")
    for role in ROLES:
        logger.info(
            f"  {role}: heur={h_acc.get(role)*100:.1f}% | lcs={lcs_acc.get(role)*100:.1f}%"
        )

    boot = {}
    if run_bootstrap:
        logger.info(f"  bootstrapping {unit}...")
        boot = run_dual_bootstrap(h_comp, lcs_comp, n_iter=n_bootstrap)

    return {
        'unit':     unit,
        'status':   'success',
        'n':        n,
        'merged_data': merged_data,
        'redcap':      redcap_unit,
        'heuristic':   {'acc': h_acc,   'comp': h_comp},
        'lcs':         {'acc': lcs_acc, 'comp': lcs_comp},
        'boot':     boot,
    }


def main():
    """run the full validation phase analysis"""
    logger.info("=== VALIDATION PHASE ANALYSIS ===")
    logger.info(f"config: {VALIDATION_CONFIG['phase']} | units: {VALIDATION_CONFIG['units']}")
    start_time = datetime.now()

    results_dir = os.path.join(
        os.path.dirname(__file__), '..', 'results',
        f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    os.makedirs(results_dir, exist_ok=True)

    # -------------------------------------------------------
    # STEP 1: LOAD ALL DATA ONCE
    # -------------------------------------------------------
    activity_data, note_signers, redcap_data = load_data(VALIDATION_CONFIG)

    # -------------------------------------------------------
    # STEP 2: PER-UNIT ANALYSIS
    # -------------------------------------------------------
    unit_results = {}
    for unit in VALIDATION_CONFIG['units']:
        result = run_unit_analysis(
            unit, activity_data, note_signers, redcap_data,
            run_bootstrap=VALIDATION_CONFIG.get('run_bootstrap', True),
            n_bootstrap=VALIDATION_CONFIG.get('n_bootstrap', 2500),
        )
        unit_results[unit] = result
        gc.collect()

    successful_units = [u for u, r in unit_results.items() if r.get('status') == 'success']

    # -------------------------------------------------------
    # STEP 3: CROSS-UNIT COMBINED ANALYSIS
    # -------------------------------------------------------
    all_h_comps, all_lcs_comps = [], []
    for unit in successful_units:
        r = unit_results[unit]
        hc  = r['heuristic']['comp'].copy();  hc['unit']  = unit
        lc  = r['lcs']['comp'].copy();        lc['unit']  = unit
        all_h_comps.append(hc); all_lcs_comps.append(lc)

    cross_h_comp   = pd.concat(all_h_comps,   ignore_index=True)
    cross_lcs_comp = pd.concat(all_lcs_comps, ignore_index=True)
    cross_n        = len(cross_h_comp)

    cross_h_acc    = compute_accuracy(cross_h_comp)
    cross_lcs_acc  = compute_accuracy(cross_lcs_comp)

    cross_boot = {}
    if VALIDATION_CONFIG.get('run_bootstrap', True):
        logger.info("running cross-unit bootstrap...")
        cross_boot = run_dual_bootstrap(
            cross_h_comp, cross_lcs_comp,
            n_iter=VALIDATION_CONFIG.get('n_bootstrap', 2500),
        )

    cross_unit_results = {
        'status': 'success',
        'n': cross_n,
        'heuristic': {'acc': cross_h_acc, 'comp': cross_h_comp},
        'lcs':       {'acc': cross_lcs_acc, 'comp': cross_lcs_comp},
        'boot': cross_boot,
    }

    # -------------------------------------------------------
    # STEP 4: GENERATE TABLES
    # -------------------------------------------------------
    cross_by_role = create_by_role_table(cross_h_acc, cross_lcs_acc, cross_boot, cross_n)
    logger.info("\n=== VALIDATION: BY-ROLE ACCURACY (ALL UNITS) ===")
    print(cross_by_role.to_string(index=False))

    by_unit_table = create_by_unit_table(unit_results)
    logger.info("\n=== VALIDATION: BY-UNIT ACCURACY ===")
    print(by_unit_table.to_string(index=False))

    role_x_unit = create_role_x_unit_table(unit_results, cross_unit_results)

    # -------------------------------------------------------
    # STEP 5: SAVE OUTPUTS
    # -------------------------------------------------------
    cross_by_role.to_csv(    os.path.join(results_dir, 'cross_unit_by_role.csv'), index=False)
    by_unit_table.to_csv(    os.path.join(results_dir, 'by_unit.csv'),           index=False)
    role_x_unit.to_csv(      os.path.join(results_dir, 'role_x_unit.csv'),      index=False)

    complete_results = {
        'unit_results':        unit_results,
        'cross_unit_results':  cross_unit_results,
        'runtime_minutes':     (datetime.now() - start_time).total_seconds() / 60,
    }
    with open(os.path.join(results_dir, 'complete_results.pkl'), 'wb') as f:
        pickle.dump(complete_results, f)

    elapsed = (datetime.now() - start_time).total_seconds() / 60
    logger.info(f"\ncomplete in {elapsed:.1f} minutes | results: {results_dir}")
    return complete_results


if __name__ == '__main__':
    main()
