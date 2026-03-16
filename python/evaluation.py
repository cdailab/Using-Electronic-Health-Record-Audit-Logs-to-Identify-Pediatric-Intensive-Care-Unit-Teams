#!/usr/bin/env python
# coding: utf-8

# evaluation functions: compare algorithm output against gold standard,
# compute accuracy (primary metric)

import logging
import numpy as np
import pandas as pd

from config import ROLES

logger = logging.getLogger(__name__)


def compare_with_gold_standard(team_results, redcap_data, attending_col='top_attending'):
    """
    merge algorithm predictions with gold standard and compute per-row match columns.

    nurse matching is lenient: correct if predicted user_id appears anywhere in
    nurse_user_ids_str (comma-separated list of nurses observed that day).
    frontline and attending matching is strict: exact user_id equality.

    args:
        team_results:   dataframe with CSN, access_date, top_nurse, top_frontline,
                        and top_attending
        redcap_data:    dataframe with CSN, access_date, attending_user_id,
                        frontline_user_id, nurse_user_ids_str
        attending_col:  column in team_results to use as predicted attending
                        (default 'top_attending')

    returns:
        merged dataframe with nurse_match, frontline_match, attending_match columns
        empty dataframe if merge fails
    """
    try:
        team_results = team_results.copy()
        redcap_data  = redcap_data.copy()
        team_results['CSN']         = team_results['CSN'].astype(str)
        team_results['access_date'] = pd.to_datetime(team_results['access_date']).dt.date
        redcap_data['CSN']          = redcap_data['CSN'].astype(str)
        redcap_data['access_date']  = pd.to_datetime(redcap_data['access_date']).dt.date

        if attending_col != 'top_attending' and attending_col in team_results.columns:
            team_results = team_results.rename(columns={attending_col: 'top_attending'})

        merged = team_results.merge(redcap_data, on=['CSN', 'access_date'], how='inner')

        if merged.empty:
            logger.warning("compare_with_gold_standard: no rows after merge")
            return pd.DataFrame()

        # parse nurse id list
        merged['nurse_user_ids_list'] = (
            merged['nurse_user_ids_str']
            .fillna('')
            .str.split(',')
            .apply(lambda ids: [nid.strip() for nid in ids if nid.strip()])
        )

        # nurse
        merged['nurse_match'] = (
            merged['top_nurse'].notna()
            & merged.apply(
                lambda row: (
                    str(row['top_nurse']) in row['nurse_user_ids_list']
                    if row['nurse_user_ids_list'] else False
                ),
                axis=1
            )
        )

        # frontline
        merged['frontline_match'] = (
            merged['top_frontline'].notna()
            & merged['frontline_user_id'].notna()
            & (merged['top_frontline'].astype(str) == merged['frontline_user_id'].astype(str))
        )

        # attending
        merged['attending_match'] = (
            merged['top_attending'].notna()
            & merged['attending_user_id'].notna()
            & (merged['top_attending'].astype(str) == merged['attending_user_id'].astype(str))
        )

        return merged

    except Exception as e:
        logger.error(f"error in compare_with_gold_standard: {e}")
        return pd.DataFrame()


def compute_accuracy(comparison_df):
    """
    per-role accuracy: proportion of patient-days where the algorithm selected the correct HCW.
    primary metric for all reporting.
    """
    if comparison_df.empty:
        return {role: None for role in ROLES}
    return {
        'nurse':     comparison_df['nurse_match'].mean()     if 'nurse_match'     in comparison_df.columns else None,
        'frontline': comparison_df['frontline_match'].mean() if 'frontline_match' in comparison_df.columns else None,
        'attending': comparison_df['attending_match'].mean() if 'attending_match' in comparison_df.columns else None,
    }


def compute_return_rate(comparison_df):
    """proportion of patient-days where the algorithm returned any (non-null) result"""
    if comparison_df.empty:
        return {role: None for role in ROLES}
    rates = {}
    for role in ROLES:
        col = f'top_{role}'
        rates[role] = comparison_df[col].notna().mean() if col in comparison_df.columns else None
    return rates


def compute_conditional_accuracy(comparison_df):
    """accuracy restricted to patient-days where algorithm returned a result (not null)"""
    if comparison_df.empty:
        return {role: None for role in ROLES}
    cond = {}
    for role in ROLES:
        alg_col   = f'top_{role}'
        match_col = f'{role}_match'
        if alg_col not in comparison_df.columns or match_col not in comparison_df.columns:
            cond[role] = None
            continue
        returned   = comparison_df[comparison_df[alg_col].notna()]
        cond[role] = returned[match_col].mean() if len(returned) > 0 else None
    return cond


def compute_conditional_attending_accuracy(comparison_df):
    """
    attending accuracy conditional on correct frontline identification.
    isolates the effectiveness of the cosigning relationship from the upstream
    frontline dependency (used in secondary analysis section of paper).

    returns dict with:
        n_fl_correct:                patient-days where frontline was correct
        n_total:                     total patient-days
        att_accuracy_given_fl:       attending accuracy when frontline is correct
        att_accuracy_overall:        unconditional attending accuracy (for comparison)
    """
    if comparison_df.empty:
        return None
    fl_correct = comparison_df[comparison_df['frontline_match'] == True]
    if len(fl_correct) == 0:
        return {'n_fl_correct': 0, 'n_total': len(comparison_df),
                'att_accuracy_given_fl': None, 'att_accuracy_overall': None}
    if 'attending_match' not in comparison_df.columns:
        return {'n_fl_correct': len(fl_correct), 'n_total': len(comparison_df),
                'att_accuracy_given_fl': None, 'att_accuracy_overall': None}
    return {
        'n_fl_correct':           len(fl_correct),
        'n_total':                len(comparison_df),
        'att_accuracy_given_fl':  fl_correct['attending_match'].mean(),
        'att_accuracy_overall':   comparison_df['attending_match'].mean(),
    }
