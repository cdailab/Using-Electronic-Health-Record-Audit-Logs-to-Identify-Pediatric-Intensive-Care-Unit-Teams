#!/usr/bin/env python
# coding: utf-8

# generates formatted output tables for paper
# primary metric is accuracy; tables include average rows and support both by-role and by-unit structures

import numpy as np
import pandas as pd
from config import ROLES


# -------------------------------------------------------
# FORMATTING HELPERS
# -------------------------------------------------------

def _pct(v):
    """0.85 -> '85.0%'"""
    return f"{v * 100:.1f}%" if v is not None else 'N/A'


def _pct_ci(v, lo, hi):
    """0.85, 0.82, 0.88 -> '85.0% (82.0–88.0%)'"""
    if None not in (v, lo, hi):
        return f"{v*100:.1f}% ({lo*100:.1f}–{hi*100:.1f}%)"
    return _pct(v)


def _diff_pct(m, lo, hi):
    """+0.05, 0.02, 0.08 -> '+5.0% (+2.0–+8.0%)'"""
    if None not in (m, lo, hi):
        return f"{m*100:+.1f}% ({lo*100:+.1f}–{hi*100:+.1f}%)"
    return 'N/A'


def _p(v):
    if v is None:
        return 'N/A'
    if v < 0.001:
        return '<0.001'
    return f"{v:.3f}"


# -------------------------------------------------------
# ACCURACY TABLE BUILDERS
# -------------------------------------------------------

def _accuracy_row(role_label, n, h_acc, lcs_acc, boot_pd):
    """
    build a single accuracy row for a role (or average).
    boot_pd: patient-day bootstrap result dict for this role
    """
    b = boot_pd or {}
    return {
        'Role':                          role_label,
        'n (patient-days)':              n,
        'Heuristic Accuracy (95% CI)':   _pct_ci(h_acc,  b.get('h_ci_lower'),  b.get('h_ci_upper')),
        'LCS Accuracy (95% CI)':         _pct_ci(lcs_acc, b.get('lcs_ci_lower'), b.get('lcs_ci_upper')),
        'Δ Accuracy (LCS−Heur, 95% CI)': _diff_pct(b.get('mean_diff'), b.get('ci_lower'), b.get('ci_upper')),
        'p-value':                       _p(b.get('p_value')),
    }


def _average_accuracy(h_acc_dict, lcs_acc_dict):
    """mean accuracy across all roles (for average row)"""
    h_vals  = [v for v in h_acc_dict.values()   if v is not None]
    lcs_vals = [v for v in lcs_acc_dict.values() if v is not None]
    return (
        float(np.mean(h_vals))   if h_vals   else None,
        float(np.mean(lcs_vals)) if lcs_vals else None,
    )


def create_by_role_table(h_acc, lcs_acc, boot_results, n):
    """
    table 1 / by-role: Nurse, Frontline, Attending, Average rows.

    args:
        h_acc:        dict {role: float} heuristic accuracy
        lcs_acc:      dict {role: float} lcs accuracy
        boot_results: output of bootstrap.run_dual_bootstrap()
        n:            int patient-days

    returns:
        pd.DataFrame
    """
    pd_boot = boot_results.get('patient_day', {})

    rows = []
    for role in ROLES:
        rows.append(_accuracy_row(
            role.capitalize(), n,
            h_acc.get(role), lcs_acc.get(role),
            pd_boot.get(role),
        ))

    # average row: mean across roles (no bootstrap CI -- bootstrapping the mean is complex
    # and the average is a secondary summary, not the primary estimate)
    h_avg, lcs_avg = _average_accuracy(h_acc, lcs_acc)
    diff_avg = (lcs_avg - h_avg) if None not in (h_avg, lcs_avg) else None
    avg_row = {
        'Role':                          'Average',
        'n (patient-days)':              n,
        'Heuristic Accuracy (95% CI)':   _pct(h_avg),
        'LCS Accuracy (95% CI)':         _pct(lcs_avg),
        'Δ Accuracy (LCS−Heur, 95% CI)': f"{diff_avg*100:+.1f}%" if diff_avg is not None else 'N/A',
        'p-value':                       '—',
    }
    rows.append(avg_row)

    return pd.DataFrame(rows)


def create_by_unit_table(unit_results):
    """
    table 2: validation by unit.
    one row per unit (accuracies averaged across roles within the unit) + All Units row.

    args:
        unit_results: dict {unit: result_dict} — each result_dict has
                      'n', 'heuristic' {'acc': ...}, 'lcs' {'acc': ...},
                      'boot' (bootstrap results)

    returns:
        pd.DataFrame
    """
    def _avg_or_none(lst):
        v = [x for x in lst if x is not None]
        return float(np.mean(v)) if v else None

    rows = []
    for unit, result in unit_results.items():
        if result.get('status') != 'success':
            continue
        n        = result.get('n', 0)
        h_acc    = result.get('heuristic', {}).get('acc', {})
        lcs_acc  = result.get('lcs', {}).get('acc', {})
        boot     = result.get('boot', {})
        pd_boot  = boot.get('patient_day', {})

        h_avg, lcs_avg = _average_accuracy(h_acc, lcs_acc)

        # use average of per-role bootstrap differences as the unit-level summary
        pd_diffs = [pd_boot.get(r, {}).get('mean_diff') for r in ROLES if pd_boot.get(r)]
        pd_los   = [pd_boot.get(r, {}).get('ci_lower')  for r in ROLES if pd_boot.get(r)]
        pd_his   = [pd_boot.get(r, {}).get('ci_upper')  for r in ROLES if pd_boot.get(r)]
        pd_ps    = [pd_boot.get(r, {}).get('p_value')   for r in ROLES if pd_boot.get(r)]

        row = {
            'Unit':                          unit,
            'n (patient-days)':              n,
            'Heuristic Accuracy (95% CI)':   _pct(h_avg),
            'LCS Accuracy (95% CI)':         _pct(lcs_avg),
            'Δ Accuracy (avg, 95% CI)':      _diff_pct(_avg_or_none(pd_diffs), _avg_or_none(pd_los), _avg_or_none(pd_his)),
            'p-value':                       _p(_avg_or_none(pd_ps)),
        }
        rows.append(row)

    return pd.DataFrame(rows)


def create_role_x_unit_table(unit_results, cross_unit_results):
    """
    supplementary: full role × unit cross-tabulation.
    rows: (unit, role) pairs + All Units section.

    args:
        unit_results:       dict {unit: result_dict}
        cross_unit_results: result_dict for all units combined
    """
    rows = []

    def _unit_role_rows(unit_label, n, h_acc, lcs_acc, boot):
        pd_boot = boot.get('patient_day', {})
        unit_rows = []
        for role in ROLES:
            unit_rows.append({
                'Unit': unit_label,
                **_accuracy_row(
                    role.capitalize(), n,
                    h_acc.get(role), lcs_acc.get(role),
                    pd_boot.get(role),
                )
            })
        return unit_rows

    for unit, result in unit_results.items():
        if result.get('status') != 'success':
            continue
        rows.extend(_unit_role_rows(
            unit, result['n'],
            result['heuristic']['acc'], result['lcs']['acc'], result.get('boot', {})
        ))

    if cross_unit_results:
        rows.extend(_unit_role_rows(
            'All Units', cross_unit_results.get('n', 0),
            cross_unit_results.get('heuristic', {}).get('acc', {}),
            cross_unit_results.get('lcs', {}).get('acc', {}),
            cross_unit_results.get('boot', {}),
        ))

    return pd.DataFrame(rows)


def create_additional_metrics_table(metrics_dict):
    """
    supplementary: return rate, conditional accuracy, top-k accuracy per role per phase/unit.

    args:
        metrics_dict: list of dicts, each with keys:
            phase, unit, algorithm, role, n,
            return_rate, accuracy, conditional_accuracy, top2_accuracy, top3_accuracy

    returns:
        pd.DataFrame
    """
    rows = []
    for m in metrics_dict:
        rows.append({
            'Phase':                 m.get('phase'),
            'Unit':                  m.get('unit'),
            'Algorithm':             m.get('algorithm'),
            'Role':                  m.get('role'),
            'n':                     m.get('n'),
            'Return Rate':           _pct(m.get('return_rate')),
            'Accuracy (top-1)':      _pct(m.get('accuracy')),
            'Conditional Accuracy':  _pct(m.get('conditional_accuracy')),
            'Top-2 Accuracy':        _pct(m.get('top2_accuracy')),
            'Top-3 Accuracy':        _pct(m.get('top3_accuracy')),
        })
    return pd.DataFrame(rows)


def create_pool_size_table(pool_rows):
    """
    supplementary: candidate pool size distributions.

    args:
        pool_rows: list of dicts with phase, unit, algorithm, role,
                   n, median, q1, q3, min, max

    returns:
        pd.DataFrame
    """
    formatted = []
    for r in pool_rows:
        formatted.append({
            'Phase':     r.get('phase'),
            'Unit':      r.get('unit'),
            'Algorithm': r.get('algorithm'),
            'Role':      r.get('role'),
            'N (patient-days)': r.get('n'),
            'Median':    r.get('median'),
            'Q1':        r.get('q1'),
            'Q3':        r.get('q3'),
            'Min':       r.get('min'),
            'Max':       r.get('max'),
        })
    return pd.DataFrame(formatted)
