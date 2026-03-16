#!/usr/bin/env python
# coding: utf-8

# loads and preprocesses data from bigquery for both phases

import logging
import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def load_data(config):
    """load activity features, note signers, and redcap from bigquery

    args:
        config: dict from config.py (DEVELOPMENT_CONFIG or VALIDATION_CONFIG)

    returns:
        tuple: (activity_data, note_signers, redcap_data) as dataframes
    """
    client = bigquery.Client()
    # tables are at project.liemn.table
    project_dataset = config['bq_project_dataset']
    tables = config['tables']

    # load activity features 
    activity_table = f"{project_dataset}.{tables['activity']}"
    logger.info(f"loading activity features from {activity_table}")
    activity_data = client.query(f"SELECT * FROM `{activity_table}`").to_dataframe()

    # load note signers 
    ns_table = f"{project_dataset}.{tables['note_signers']}"
    logger.info(f"loading note signers from {ns_table}")
    note_signers = client.query(f"SELECT * FROM `{ns_table}`").to_dataframe()
    note_signers = note_signers.rename(columns={'csn': 'CSN'})
    note_signers['access_date'] = pd.to_datetime(note_signers['note_start_date']).dt.date

    # load redcap enriched 
    redcap_table = f"{project_dataset}.{tables['redcap']}"
    logger.info(f"loading redcap from {redcap_table}")
    redcap_data = client.query(f"SELECT * FROM `{redcap_table}`").to_dataframe()
    redcap_data = redcap_data.rename(columns={'csn': 'CSN', 'date': 'access_date'})

    # normalize CSN to string across all dataframes to prevent type-mismatch merge issues
    # (bigquery may return INT64 from some tables, STRING from others depending on source)
    for df in [activity_data, note_signers, redcap_data]:
        df['CSN'] = df['CSN'].astype(str)

    logger.info(
        f"loaded: activity={len(activity_data)} rows, "
        f"note_signers={len(note_signers)} rows, "
        f"redcap={len(redcap_data)} patient-days"
    )
    return activity_data, note_signers, redcap_data


def preprocess_data(activity_data, note_signers, redcap_data, unit=None):
    """filter data to the specified unit and prepare for algorithm input

    uses LEFT join so patient-days without any notes are retained.

    args:
        activity_data: dataframe from load_data()
        note_signers:  note signers dataframe from load_data()
        redcap_data:   dataframe from load_data()
        unit:          str unit label (e.g. 'PICU') or None for no filtering

    returns:
        tuple: (merged_data, note_signers_filtered, redcap_filtered) -- all None if insufficient data
    """
    # filter redcap to unit if specified (validation phase)
    if unit is not None:
        redcap_filtered = redcap_data[redcap_data['unit'] == unit].copy()
        logger.info(f"unit {unit}: {len(redcap_filtered)} patient-days in gold standard")
    else:
        redcap_filtered = redcap_data.copy()
        logger.info(f"all units: {len(redcap_filtered)} patient-days in gold standard")

    # exclude patient-days where any of the 3 role annotations are missing in the gold standard;
    n_before = len(redcap_filtered)
    all_roles_present = (
        redcap_filtered['attending_user_id'].notna()
        & redcap_filtered['frontline_user_id'].notna()
        & redcap_filtered['nurse_user_ids_str'].notna()
        & (redcap_filtered['nurse_user_ids_str'].astype(str).str.strip() != '')
    )
    redcap_filtered = redcap_filtered[all_roles_present].copy()
    n_excluded = n_before - len(redcap_filtered)
    if n_excluded > 0:
        logger.warning(
            f"excluded {n_excluded} patient-days from gold standard missing ≥1 role annotation "
            f"({n_before} → {len(redcap_filtered)} patient-days)"
        )

    if len(redcap_filtered) < 20:
        logger.warning(f"insufficient data ({len(redcap_filtered)} rows) -- skipping")
        return None, None, None

    # restrict activity to csns present in gold standard
    valid_csns = set(redcap_filtered['CSN'].astype(str))
    activity_filtered = activity_data[activity_data['CSN'].astype(str).isin(valid_csns)].copy()

    if activity_filtered.empty:
        logger.warning("no activity data after filtering to gold standard csns")
        return None, None, None

    # get unique csn/date pairs that have at least one note
    note_csn_dates = (
        note_signers[['CSN', 'access_date']]
        .drop_duplicates()
        .assign(has_notes=True)
    )

    # left join: keep all activity rows; patient-days without notes get has_notes=NaN
    merged_data = activity_filtered.merge(
        note_csn_dates,
        on=['CSN', 'access_date'],
        how='left'
    )

    # log how many patient-days have no notes at all
    patient_day_note_status = (
        merged_data[['CSN', 'access_date', 'has_notes']]
        .drop_duplicates(['CSN', 'access_date'])
    )
    n_total_pd   = len(patient_day_note_status)
    n_no_notes   = patient_day_note_status['has_notes'].isna().sum()
    n_with_notes = n_total_pd - n_no_notes

    if n_no_notes > 0:
        # collect the specific csn/date pairs missing notes for traceability
        no_note_pairs = (
            patient_day_note_status[patient_day_note_status['has_notes'].isna()][['CSN', 'access_date']]
            .sort_values('access_date')
        )
        pair_list = ', '.join(
            f"CSN={row.CSN} ({row.access_date})" for row in no_note_pairs.itertuples()
        )
        logger.warning(
            f"{n_no_notes}/{n_total_pd} patient-days have no notes "
            f"-- attending identification will use metric fallback for these. "
            f"affected: [{pair_list}]"
        )
    else:
        logger.info(f"all {n_total_pd} patient-days have notes")

    merged_data = merged_data.drop(columns=['has_notes'])

    logger.info(
        f"preprocessed: {len(merged_data)} rows, "
        f"{merged_data['CSN'].nunique()} unique patients, "
        f"{n_with_notes}/{n_total_pd} patient-days have notes"
    )
    return merged_data, note_signers, redcap_filtered
