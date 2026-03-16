#!/usr/bin/env python
# coding: utf-8

# team identification algorithms

import logging
import pandas as pd

from config import (
    NURSE_PROV_TYPES, NURSE_TITLES, NURSE_FILTER_OP,
    FRONTLINE_PROV_TYPES, FRONTLINE_TITLES, FRONTLINE_FILTER_OP,
    ATTENDING_PROV_TYPES, ATTENDING_TITLES, ATTENDING_FILTER_OP,
    HEURISTIC_NURSE_METRICS, HEURISTIC_FRONTLINE_METRICS, HEURISTIC_ATTENDING_METRIC,
    LCS_NURSE_METRIC, LCS_FRONTLINE_METRIC, LCS_ATTENDING_METRIC,
)

logger = logging.getLogger(__name__)


def _filter_candidates(group, prov_types, titles, op='OR'):
    """filter a patient-day group to candidates matching prov_type and/or title

    op='OR'  -> prov_type match OR title match (permissive; catches partial records)
    op='AND' -> prov_type match AND title match (strict; requires both to align)
    """
    type_mask  = group['prov_type'].astype(str).isin(prov_types)
    title_mask = group['clinician_title'].astype(str).str.contains(
        '|'.join(titles), na=False, case=False
    )
    combined = (type_mask & title_mask) if op == 'AND' else (type_mask | title_mask)
    return group[combined].copy()


def _pick_top(candidates, metrics, tiebreaker='n_total'):
    """return user_id of candidate with highest sum of given metrics; break ties by tiebreaker col"""
    valid_metrics = [m for m in metrics if m in candidates.columns]
    if not valid_metrics or candidates.empty:
        return None
    scores = candidates[valid_metrics].fillna(0).sum(axis=1)
    nonzero = scores[scores > 0]
    if nonzero.empty:
        return None
    max_score = nonzero.max()
    # find all candidates tied at the top score
    top = candidates.loc[nonzero[nonzero == max_score].index]
    if len(top) == 1:
        return top.iloc[0]['USER_ID']
    # break tie using n_total (total calendar-day actions)
    if tiebreaker in top.columns:
        tb = top[tiebreaker].fillna(0)
        return top.loc[tb.idxmax(), 'USER_ID']
    return top.iloc[0]['USER_ID']


def identify_team_members(group, algorithm_type='lcs', note_signers=None):
    """
    identify nurse, frontline, and attending for a single patient-day.

    algorithm_type='heuristic':
        nurse:     highest n_barcode_7_11 + n_modify_7_11 (7am-11am window)
        frontline: highest n_order_modify_actions + n_note_modify_actions
        attending: frontline-dependent (cosigning via frontline's notes),
                   tiebreaker/fallback: n_total

    algorithm_type='lcs':
        nurse:     highest lcs_all_7_11 (7am-11am window)
        frontline: highest lcs_all_6_18 (6am-6pm window)
        attending: frontline-dependent (cosigning via frontline's notes),
                   tiebreaker/fallback: n_total

    all ties (nurse, frontline, attending) are broken by n_total (total calendar-day actions).

    attending identification is dependent on the identified frontline: we look for
    the physician who was the last signer on the most notes initiated by the frontline.
    attending may equal frontline (frontline self-signs and no one else signs after).
    when no notes are available or no frontline was identified, falls back to
    metric-based selection.

    args:
        group:          dataframe for one CSN/access_date (from groupby)
        algorithm_type: 'heuristic' or 'lcs'
        note_signers:   note signers dataframe (pre-converted by caller)

    returns:
        pd.Series with top_nurse, top_frontline, top_attending (user ids or None)
    """
    try:
        results = {'top_nurse': None, 'top_frontline': None, 'top_attending': None}

        # extract csn/access_date from group index (set by groupby)
        if isinstance(group.name, tuple) and len(group.name) == 2:
            csn, access_date = group.name
        else:
            csn = group['CSN'].iloc[0]
            access_date = group['access_date'].iloc[0]
        # normalize access_date to python date for consistent comparison
        if isinstance(access_date, pd.Timestamp):
            access_date = access_date.date()
        elif not hasattr(access_date, 'year'):
            access_date = pd.to_datetime(access_date).date()

        # --- nurse ---
        nurse_candidates = _filter_candidates(group, NURSE_PROV_TYPES, NURSE_TITLES, NURSE_FILTER_OP)
        metrics = HEURISTIC_NURSE_METRICS if algorithm_type == 'heuristic' else [LCS_NURSE_METRIC]
        results['top_nurse'] = _pick_top(nurse_candidates, metrics)

        # --- frontline ---
        fl_candidates = _filter_candidates(group, FRONTLINE_PROV_TYPES, FRONTLINE_TITLES, FRONTLINE_FILTER_OP)
        fl_metrics = HEURISTIC_FRONTLINE_METRICS if algorithm_type == 'heuristic' else [LCS_FRONTLINE_METRIC]
        results['top_frontline'] = _pick_top(fl_candidates, fl_metrics)

        # --- attending (frontline-dependent) ---
        att_candidates = _filter_candidates(group, ATTENDING_PROV_TYPES, ATTENDING_TITLES, ATTENDING_FILTER_OP)
        if att_candidates.empty:
            return pd.Series(results)

        top_frontline_user_id = results['top_frontline']
        # tiebreaker/fallback metric when cosign logic unavailable or tied
        tiebreaker_col = HEURISTIC_ATTENDING_METRIC if algorithm_type == 'heuristic' else LCS_ATTENDING_METRIC

        # try note-based cosigning logic if we have a frontline and note data
        cosign_counts = {}
        use_notes_logic = False

        if (pd.notna(top_frontline_user_id)
                and note_signers is not None and not note_signers.empty
                and csn is not None and access_date is not None):

            # filter note_signers to this patient-day (types pre-converted by run_all_algorithms)
            patient_notes = note_signers[
                (note_signers['CSN'] == str(csn)) & (note_signers['access_date'] == access_date)
            ]

            if not patient_notes.empty:
                fl_id = str(top_frontline_user_id)

                # notes where frontline is first signer (used for cosigning logic)
                relevant_notes = patient_notes[patient_notes['first_signer_user_id'].astype(str) == fl_id]

                if not relevant_notes.empty:
                    # count last signer across all frontline-initiated notes, including
                    # self-signed notes (where frontline is also the last signer)
                    raw_counts = relevant_notes['last_signer_user_id'].value_counts().to_dict()
                    cosign_counts = {str(k): v for k, v in raw_counts.items() if pd.notna(k)}
                    use_notes_logic = bool(cosign_counts)

        if use_notes_logic:
            att_candidates = att_candidates.copy()
            att_candidates['cosign_count'] = (
                att_candidates['USER_ID'].astype(str).map(cosign_counts).fillna(0)
            )
            valid_attendings = att_candidates[att_candidates['cosign_count'] > 0]

            if not valid_attendings.empty:
                max_count = valid_attendings['cosign_count'].max()
                top_attendings = valid_attendings[valid_attendings['cosign_count'] == max_count]

                if len(top_attendings) == 1:
                    results['top_attending'] = top_attendings.iloc[0]['USER_ID']
                else:
                    # tie: use tiebreaker column
                    if tiebreaker_col in top_attendings.columns:
                        tb_scores = top_attendings[tiebreaker_col].fillna(0)
                        results['top_attending'] = top_attendings.loc[tb_scores.idxmax(), 'USER_ID']
                    else:
                        results['top_attending'] = top_attendings.iloc[0]['USER_ID']
                return pd.Series(results)

        # fallback: metric-based attending selection
        results['top_attending'] = _pick_top(att_candidates, [tiebreaker_col])
        return pd.Series(results)

    except Exception as e:
        logger.error(f"error in identify_team_members for {getattr(group, 'name', '?')}: {e}")
        return pd.Series({'top_nurse': None, 'top_frontline': None, 'top_attending': None})


def run_all_algorithms(merged_data, note_signers):
    """
    run heuristic and lcs algorithms with frontline-dependent attending.

    returns a dict with two result dataframes:
        'heuristic':  nurse/frontline/attending via heuristic
        'lcs':        nurse/frontline/attending via lcs

    attending in both algorithms is identified via frontline-dependent cosigning:
    the physician who most often cosigned notes authored by the identified frontline.
    """
    grouped = merged_data.groupby(['CSN', 'access_date'])

    # pre-convert note_signers types once (avoids per-group conversion overhead)
    if note_signers is not None and not note_signers.empty:
        ns_prep = note_signers.copy()
        ns_prep['CSN'] = ns_prep['CSN'].astype(str)
        ns_prep['access_date'] = pd.to_datetime(ns_prep['access_date']).dt.date
    else:
        ns_prep = note_signers

    logger.info("running heuristic algorithm (attending: frontline-dependent)...")
    heuristic_results = grouped.apply(
        lambda g: identify_team_members(g, 'heuristic', ns_prep),
        include_groups=False
    ).reset_index()

    logger.info("running lcs algorithm (attending: frontline-dependent)...")
    lcs_results = grouped.apply(
        lambda g: identify_team_members(g, 'lcs', ns_prep),
        include_groups=False
    ).reset_index()

    return {
        'heuristic': heuristic_results,
        'lcs':       lcs_results,
    }
