#!/usr/bin/env python
# coding: utf-8

# patient-day bootstrap for comparing heuristic vs lcs algorithm accuracy
#
# resamples individual (CSN, access_date) pairs with replacement.
# treats each patient-day as independent.
# both algorithms evaluated on IDENTICAL resampled sets (paired structure).
# 95% CIs from 2.5th / 97.5th percentiles.
# p-value = two-tailed: 2 * min(proportion <= 0, proportion >= 0).

import logging
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from config import ROLES
from evaluation import compute_accuracy

logger = logging.getLogger(__name__)


def _summarize_boot(h_orig, lcs_orig, boot_h, boot_lcs, boot_delta):
    """build result dict for one role from raw bootstrap arrays"""
    return {
        'h_accuracy':  float(h_orig),
        'h_ci_lower':  float(np.percentile(boot_h,     2.5)),
        'h_ci_upper':  float(np.percentile(boot_h,    97.5)),
        'lcs_accuracy':  float(lcs_orig),
        'lcs_ci_lower':  float(np.percentile(boot_lcs,  2.5)),
        'lcs_ci_upper':  float(np.percentile(boot_lcs, 97.5)),
        'mean_diff':   float(lcs_orig - h_orig),   # observed point estimate, not bootstrap mean
        'ci_lower':    float(np.percentile(boot_delta,  2.5)),
        'ci_upper':    float(np.percentile(boot_delta, 97.5)),
        # two-tailed p: 2 * min(proportion <= 0, proportion >= 0)
        'p_value':     float(2 * min((boot_delta <= 0).mean(), (boot_delta >= 0).mean())),
        'significant': bool(np.percentile(boot_delta, 2.5) > 0),
    }


def _boot_pd_level(h_comp, lcs_comp, n_iter, rng):
    """
    patient-day level bootstrap.
    resamples individual patient-day rows with replacement.
    same indices used for both algorithms.
    """
    # get aligned (CSN, access_date) pairs shared by both comparison dfs
    h_keys = h_comp.set_index(['CSN', 'access_date'])
    l_keys = lcs_comp.set_index(['CSN', 'access_date'])

    n = len(h_comp)
    results = {}

    for role in ROLES:
        mc = f'{role}_match'
        if mc not in h_comp.columns:
            results[role] = None
            continue

        # skip if either df is missing the column (e.g. updated algo uses different attending cols)
        if mc not in lcs_comp.columns:
            results[role] = None
            continue

        h_vals   = h_comp[mc].to_numpy()
        lcs_vals = lcs_comp[mc].to_numpy()

        boot_h     = np.zeros(n_iter)
        boot_lcs   = np.zeros(n_iter)
        boot_delta = np.zeros(n_iter)

        for i in range(n_iter):
            idx          = rng.integers(0, n, size=n)
            boot_h[i]    = h_vals[idx].mean()
            boot_lcs[i]  = lcs_vals[idx].mean()
            boot_delta[i] = boot_lcs[i] - boot_h[i]

        results[role] = _summarize_boot(
            h_vals.mean(), lcs_vals.mean(), boot_h, boot_lcs, boot_delta
        )

    return results


def run_dual_bootstrap(h_comp, lcs_comp, n_iter=2500, seed=42):
    """
    run patient-day bootstrap accuracy comparison (heuristic vs lcs).

    args:
        h_comp:   comparison dataframe for heuristic (from evaluation.compare_with_gold_standard)
        lcs_comp: comparison dataframe for lcs
        n_iter:   number of bootstrap iterations (default 2500)
        seed:     random seed for reproducibility

    returns:
        dict with key 'patient_day' mapping role -> result_dict, plus n_iter, n_patient_days.
        result_dict keys: h_accuracy, h_ci_lower, h_ci_upper, lcs_accuracy, lcs_ci_lower,
                          lcs_ci_upper, mean_diff, ci_lower, ci_upper, p_value, significant
    """
    rng = np.random.default_rng(seed)

    logger.info(f"patient-day bootstrap: {n_iter} iterations...")
    pd_results = _boot_pd_level(h_comp, lcs_comp, n_iter, rng)

    return {
        'patient_day':     pd_results,
        'n_iter':          n_iter,
        'n_patient_days':  len(h_comp),
    }


def get_boot_result(boot_results, level='patient_day', role=None):
    """
    convenience accessor for a single role from run_dual_bootstrap output.

    args:
        boot_results: dict from run_dual_bootstrap()
        level:        'patient_day' (only level supported; kept for api compat)
        role:         role string, or None to return all roles

    returns:
        dict for the role, or dict of all roles
    """
    level_data = boot_results.get('patient_day', {})
    if role is not None:
        return level_data.get(role, {})
    return level_data
