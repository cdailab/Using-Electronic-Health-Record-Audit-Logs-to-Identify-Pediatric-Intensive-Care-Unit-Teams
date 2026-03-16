#!/usr/bin/env python
# coding: utf-8

# central config for both development and validation phases
# all phase-specific params, bigquery refs, and provider type constants live here

BQ_PROJECT_DATASET = 'INSERT_TABLE_NAME'

# -------------------------------------------------------
# PHASE CONFIGURATIONS
# -------------------------------------------------------

DEVELOPMENT_CONFIG = {
    'phase': 'development',
    'units': ['PICU'],
    'tables': {
        'activity':     'dev_activity_features',
        'note_signers': 'dev_note_signers',
        'redcap':       'dev_redcap_enriched',
    },
    'bq_project_dataset': BQ_PROJECT_DATASET,
    'run_bootstrap': True,
    'n_bootstrap': 2500,
    'n_jobs': -1,  
}

VALIDATION_CONFIG = {
    'phase': 'validation',
    'units': ['PICU', 'NICU', 'CVICU'],
    'tables': {
        'activity':     'val_activity_features',
        'note_signers': 'val_note_signers',
        'redcap':       'val_redcap_enriched',
    },
    'bq_project_dataset': BQ_PROJECT_DATASET,
    'run_bootstrap': True,
    'n_bootstrap': 2500,
    'n_jobs': -1,
}

# -------------------------------------------------------
# PROVIDER TYPE / TITLE CONSTANTS
# -------------------------------------------------------
NURSE_PROV_TYPES  = ['Registered Nurse', 'Clinical Nurse Specialist', 'Nursing Assistant']
NURSE_TITLES      = ['RN', 'CNS']
NURSE_FILTER_OP   = 'OR'

FRONTLINE_PROV_TYPES = ['Resident', 'Fellow', 'Physician', 'Nurse Practitioner', 'Physician Assistant']
FRONTLINE_TITLES     = ['NP', 'PA', 'MD', 'DO']
FRONTLINE_FILTER_OP  = 'OR'

ATTENDING_PROV_TYPES = ['Physician']
ATTENDING_TITLES     = ['MD', 'DO', 'MBBS']
ATTENDING_FILTER_OP  = 'OR'

# -------------------------------------------------------
# ALGORITHM TYPES
# -------------------------------------------------------
ALGORITHM_TYPES = ['heuristic', 'lcs']

# -------------------------------------------------------
# ROLES
# -------------------------------------------------------
ROLES = ['nurse', 'frontline', 'attending']

# -------------------------------------------------------
# HEURISTIC METRIC COLUMN NAMES
# -------------------------------------------------------
HEURISTIC_NURSE_METRICS     = ['n_barcode', 'n_modify']
HEURISTIC_FRONTLINE_METRICS = ['n_order_modify_actions', 'n_note_modify_actions']
HEURISTIC_ATTENDING_METRIC  = 'n_total' # fallback only
LCS_NURSE_METRIC            = 'lcs_all_7_11'       
LCS_FRONTLINE_METRIC        = 'lcs_all_6_18'       
LCS_ATTENDING_METRIC        = 'n_total'     # fallback only        
