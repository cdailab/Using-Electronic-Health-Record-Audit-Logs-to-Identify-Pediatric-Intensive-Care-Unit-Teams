-- ======================================================================================
-- DEVELOPMENT PHASE PIPELINE
-- ======================================================================================

-- ======================================================================================
-- METRIC ID CONSTANTS
-- ======================================================================================
DECLARE NOTE_SIGNING_METRIC_IDS    ARRAY<INT64> DEFAULT [60010, 60011, 60012, 17905, 17150, 60017, 17253, 17222, 60024, 87005];
DECLARE NOTE_COSIGNING_METRIC_IDS  ARRAY<INT64> DEFAULT [60013, 60015, 60016, 60021, 60025, 60026];
DECLARE ORDER_ACTION_METRICS       ARRAY<INT64> DEFAULT [17108, 17314,32335,60022,94008,17188,17210,17220,17320,17321,17333,55005];
DECLARE BARCODE_PATTERN            STRING       DEFAULT '%BARCODE%';
DECLARE ALL_NOTE_SIGN_METRIC_IDS ARRAY<INT64>;
SET ALL_NOTE_SIGN_METRIC_IDS = ARRAY_CONCAT(NOTE_SIGNING_METRIC_IDS, NOTE_COSIGNING_METRIC_IDS);


-- ======================================================================================
-- STEP 1: ENRICH REDCAP DATA
-- Links REDCap team observation names to EHR user IDs.
-- ======================================================================================
CREATE OR REPLACE TABLE `INSERT_TABLE_NAME.dev_redcap_enriched` AS
WITH base AS (
  SELECT
    rt.csn,
    rt.date,
    rt.status,
    rt.attending,
    rt.frontline,
    rt.nurse,
    ar.prov_id   AS attending_prov_id,
    flr.user_id  AS frontline_user_id,
    nr.user_id   AS nurse_user_id
  FROM `INSERT_TABLE_NAME.dev_redcap_teams` AS rt
  LEFT JOIN `INSERT_TABLE_NAME.attending_reference` AS ar
    ON LOWER(TRIM(rt.attending)) = LOWER(TRIM(ar.last_name))
  LEFT JOIN `INSERT_TABLE_NAME.frontline_reference` AS flr
    ON LOWER(TRIM(rt.frontline)) = LOWER(TRIM(flr.first_line))
  LEFT JOIN `INSERT_TABLE_NAME.nurse_reference` AS nr
    ON LOWER(TRIM(rt.nurse)) = LOWER(TRIM(nr.Nurse))
  WHERE
    rt.status = 'Complete'
),
base_with_attending AS (
  SELECT
    b.csn,
    b.date,
    b.status,
    b.attending,
    b.frontline,
    b.nurse,
    b.attending_prov_id,
    c.user_id   AS attending_user_id,
    b.frontline_user_id,
    b.nurse_user_id
  FROM base b
  LEFT JOIN `INSERT_CLARITY_SER_TABLE_NAME` AS c
    ON LPAD(CAST(b.attending_prov_id AS STRING), 6, '0')
     = LPAD(CAST(c.prov_id AS STRING), 6, '0')
)
SELECT
  bw.csn,
  bw.date                                                              AS access_date,
  ANY_VALUE(bw.status)                                                 AS status,
  ANY_VALUE(bw.attending)                                              AS attending,
  ANY_VALUE(bw.frontline)                                              AS frontline,
  ANY_VALUE(bw.nurse)                                                  AS nurse,
  ANY_VALUE(bw.attending_user_id)                                      AS attending_user_id,
  ANY_VALUE(bw.frontline_user_id)                                      AS frontline_user_id,
  ARRAY_TO_STRING(
    ARRAY_AGG(DISTINCT CAST(bw.nurse_user_id AS STRING) IGNORE NULLS),
    ','
  )                                                                    AS nurse_user_ids_str
FROM base_with_attending bw
GROUP BY bw.csn, bw.date
HAVING
  ANY_VALUE(bw.attending_user_id)  IS NOT NULL
  AND ANY_VALUE(bw.frontline_user_id) IS NOT NULL
  AND ARRAY_LENGTH(ARRAY_AGG(DISTINCT bw.nurse_user_id IGNORE NULLS)) > 0;


-- ======================================================================================
-- STEP 2A: EXTRACT AUDIT LOGS
-- All EHR activity for our cohort CSNs on observation dates.
-- ======================================================================================
CREATE OR REPLACE TABLE `INSERT_TABLE_NAME.dev_audit_logs` AS
WITH RedcapCSNDates AS (
  SELECT DISTINCT csn, access_date
  FROM `INSERT_TABLE_NAME.dev_redcap_enriched`
)
SELECT DISTINCT
  ual.process_id,
  ual.access_instant,
  ual.access_time,
  ual.user_id,
  ual.csn,
  ual.metric_id,
  d.string_value   AS dtl_string_value,
  cs.prov_type,
  cs.clinician_title,
  cs.prov_name
FROM `INSERT_CLARITY_ACCESS_LOG_TABLE_NAME` AS ual
JOIN RedcapCSNDates AS rcsd
  ON  ual.csn = rcsd.csn
  AND EXTRACT(DATE FROM ual.access_time) = rcsd.access_date
LEFT JOIN `INSERT_CLARITY_ACCESS_LOG_DTL_TABLE_NAME` AS d
  ON ual.access_instant = d.access_instant
  AND ual.process_id    = d.process_id
LEFT JOIN `INSERT_CLARITY_SER_TABLE_NAME` AS cs
  ON ual.user_id = cs.user_id;


-- ======================================================================================
-- STEP 2B: EXTRACT NOTE AUDIT LOGS
-- Audit log entries for notes that started on observation dates (queries all time
-- to capture late attending cosigning).
-- ======================================================================================
CREATE OR REPLACE TABLE `INSERT_TABLE_NAME.dev_note_audit_logs` AS
WITH NoteCandidates AS (
  SELECT
    d.string_value                                                          AS note_id,
    ual.CSN,
    ual.ACCESS_TIME,
    ual.user_id                                                             AS USER_ID,
    ual.METRIC_ID,
    ROW_NUMBER() OVER(
      PARTITION BY d.string_value
      ORDER BY ual.ACCESS_TIME
    )                                                                       AS first_appearance
  FROM `INSERT_CLARITY_ACCESS_LOG_TABLE_NAME` ual
  JOIN `INSERT_CLARITY_ACCESS_LOG_DTL_TABLE_NAME` d
    ON ual.process_id      = d.process_id
    AND ual.access_instant = d.access_instant
  WHERE
    ual.METRIC_ID IN UNNEST(ALL_NOTE_SIGN_METRIC_IDS)
    AND d.string_value IS NOT NULL
    AND ual.CSN IS NOT NULL
),
note_reference AS (
  SELECT
    nc.note_id,
    nc.CSN,
    DATE(nc.ACCESS_TIME)  AS note_start_date
  FROM NoteCandidates nc
  WHERE nc.first_appearance = 1
),
relevant_csn_notes AS (
  SELECT DISTINCT
    rcsn.csn,
    rcsn.access_date,
    nr.note_id,
    nr.note_start_date
  FROM (
    SELECT DISTINCT csn, access_date
    FROM `INSERT_TABLE_NAME.dev_redcap_enriched`
  ) AS rcsn
  INNER JOIN note_reference AS nr
    ON  rcsn.csn         = nr.CSN
    AND rcsn.access_date = nr.note_start_date
)
SELECT
  ual.process_id,
  ual.access_instant,
  ual.access_time,
  ual.user_id,
  ual.csn,
  ual.metric_id,
  d.string_value  AS dtl_string_value
FROM `INSERT_CLARITY_ACCESS_LOG_TABLE_NAME` AS ual
JOIN `INSERT_TABLE_NAME.dev_redcap_enriched` AS pdcsn
  ON ual.csn = pdcsn.csn
LEFT JOIN `INSERT_CLARITY_ACCESS_LOG_DTL_TABLE_NAME` AS d
  ON ual.process_id      = d.process_id
  AND ual.access_instant = d.access_instant
JOIN relevant_csn_notes AS rcn
  ON d.string_value = rcn.note_id;


-- ======================================================================================
-- STEP 3: COMPUTE NOTE SIGNERS
-- First and last signer per note.
-- ======================================================================================
CREATE OR REPLACE TABLE `INSERT_TABLE_NAME.dev_note_signers` AS
WITH FrontlineNoteActions AS (
  -- frontline signing events only; used to find first signer
  SELECT
    nu.csn                                                                AS csn,
    nu.dtl_string_value                                                   AS note_id,
    nu.access_time,
    nu.user_id                                                            AS USER_ID,
    nu.metric_id,
    ROW_NUMBER() OVER(
      PARTITION BY nu.dtl_string_value
      ORDER BY nu.access_time
    )                                                                     AS sign_order
  FROM `INSERT_TABLE_NAME.dev_note_audit_logs` nu
  WHERE
    nu.metric_id        IN UNNEST(NOTE_SIGNING_METRIC_IDS)
    AND nu.dtl_string_value IS NOT NULL
),
AttendingNoteActions AS (
  -- all signing + cosigning events; used to find last signer (attending)
  SELECT
    nu.csn                                                                AS csn,
    nu.dtl_string_value                                                   AS note_id,
    nu.access_time,
    nu.user_id                                                            AS USER_ID,
    nu.metric_id,
    ROW_NUMBER() OVER(
      PARTITION BY nu.dtl_string_value
      ORDER BY nu.access_time DESC
    )                                                                     AS reverse_sign_order
  FROM `INSERT_TABLE_NAME.dev_note_audit_logs` nu
  WHERE
    nu.metric_id        IN UNNEST(ALL_NOTE_SIGN_METRIC_IDS)
    AND nu.dtl_string_value IS NOT NULL
),
FirstSigners AS (
  SELECT csn, note_id, USER_ID AS first_signer_user_id
  FROM FrontlineNoteActions WHERE sign_order = 1
),
LastSigners AS (
  SELECT csn, note_id, USER_ID AS last_signer_user_id
  FROM AttendingNoteActions WHERE reverse_sign_order = 1
),
NoteSummary AS (
  SELECT csn, note_id, MIN(access_time) AS note_start_time
  FROM AttendingNoteActions
  GROUP BY csn, note_id
)
SELECT
  ns.csn,
  ns.note_id,
  DATE(ns.note_start_time)                                                AS note_start_date,
  fs.first_signer_user_id,
  ls.last_signer_user_id
FROM NoteSummary ns
LEFT JOIN FirstSigners fs  ON ns.note_id = fs.note_id  AND ns.csn = fs.csn
LEFT JOIN LastSigners  ls  ON ns.note_id = ls.note_id  AND ns.csn = ls.csn
WHERE fs.first_signer_user_id IS NOT NULL;


-- ======================================================================================
-- STEP 4: COMPUTE ACTIVITY FEATURES
-- Per-HCW per-patient-day metrics required by the team identification algorithms.
-- ======================================================================================
CREATE OR REPLACE TABLE `INSERT_TABLE_NAME.dev_activity_features` AS
WITH PatientProviderDayMetrics AS (
  -- main per-hcw per-patient-day counts; join metrics_details for name/action_type
  SELECT
    nu.csn,
    nu.user_id,
    DATE(nu.access_time)                                                                         AS access_date,
    COUNT(CASE WHEN UPPER(md.METRIC_NAME) LIKE BARCODE_PATTERN                           THEN 1 END)  AS n_barcode,
    COUNT(CASE WHEN md.event_action_type_c = 2                                           THEN 1 END)  AS n_modify,
    COUNT(CASE WHEN UPPER(md.METRIC_NAME) LIKE BARCODE_PATTERN
                    AND EXTRACT(HOUR FROM nu.access_time) >= 7
                    AND EXTRACT(HOUR FROM nu.access_time) < 11            THEN 1 END)  AS n_barcode_7_11,
    COUNT(CASE WHEN md.event_action_type_c = 2
                    AND EXTRACT(HOUR FROM nu.access_time) >= 7
                    AND EXTRACT(HOUR FROM nu.access_time) < 11            THEN 1 END)  AS n_modify_7_11,
    COUNT(CASE WHEN nu.metric_id IN UNNEST(NOTE_SIGNING_METRIC_IDS)                   THEN 1 END)  AS n_note_modify_actions,
    COUNT(CASE WHEN nu.metric_id IN UNNEST(ORDER_ACTION_METRICS)                      THEN 1 END)  AS n_order_modify_actions,
    COUNT(*)                                                                                         AS n_total
  FROM `INSERT_TABLE_NAME.dev_audit_logs` nu
  LEFT JOIN `INSERT_CLARITY_ACCESS_LOG_METRIC_DETAILS_TABLE` md ON nu.metric_id = md.METRIC_ID
  GROUP BY nu.csn, nu.user_id, DATE(nu.access_time)
),
HourlyActionCounts AS (
  -- per-hcw per-hour action counts (input to LCS)
  SELECT
    csn,
    user_id,
    DATE(access_time)              AS access_date,
    EXTRACT(HOUR FROM access_time) AS hour_of_day,
    COUNT(*)                       AS hour_actions
  FROM `INSERT_TABLE_NAME.dev_audit_logs`
  WHERE access_time IS NOT NULL
  GROUP BY csn, user_id, DATE(access_time), EXTRACT(HOUR FROM access_time)
),
HourlyTotals AS (
  -- total actions across all hcws per hour (denominator for LCS)
  SELECT
    csn,
    access_date,
    hour_of_day,
    SUM(hour_actions)  AS total_hour_actions
  FROM HourlyActionCounts
  GROUP BY csn, access_date, hour_of_day
),
HourlyDomination AS (
  -- fraction of each hour's activity belonging to this hcw
  SELECT
    hac.csn,
    hac.user_id,
    hac.access_date,
    hac.hour_of_day,
    COALESCE(SAFE_DIVIDE(hac.hour_actions, ht.total_hour_actions), 0)  AS hourly_domination
  FROM HourlyActionCounts hac
  LEFT JOIN HourlyTotals ht
    ON hac.csn = ht.csn AND hac.access_date = ht.access_date AND hac.hour_of_day = ht.hour_of_day
),
LCS AS (
  -- longitudinal contribution scores: sum of hourly domination fractions over each window
  SELECT
    csn,
    user_id,
    access_date,
    SUM(CASE WHEN hour_of_day >= 6 AND hour_of_day < 18 THEN hourly_domination ELSE 0 END)  AS lcs_all_6_18,
    SUM(CASE WHEN hour_of_day >= 7 AND hour_of_day < 11 THEN hourly_domination ELSE 0 END)  AS lcs_all_7_11
  FROM HourlyDomination
  GROUP BY csn, user_id, access_date
),
ProviderInfo AS (
  SELECT DISTINCT user_id, prov_type, clinician_title, prov_name
  FROM `INSERT_TABLE_NAME.dev_audit_logs`
  WHERE user_id IS NOT NULL
)
SELECT
  ppdm.csn                                                              AS CSN,
  ppdm.user_id                                                          AS USER_ID,
  ppdm.access_date,
  pi.prov_type,
  pi.clinician_title,
  pi.prov_name,
  COALESCE(ppdm.n_barcode,              0)  AS n_barcode,
  COALESCE(ppdm.n_modify,               0)  AS n_modify,
  COALESCE(ppdm.n_barcode_7_11,         0)  AS n_barcode_7_11,
  COALESCE(ppdm.n_modify_7_11,          0)  AS n_modify_7_11,
  COALESCE(ppdm.n_note_modify_actions,  0)  AS n_note_modify_actions,
  COALESCE(ppdm.n_order_modify_actions, 0)  AS n_order_modify_actions,
  COALESCE(ppdm.n_total,                0)  AS n_total,
  COALESCE(lcs.lcs_all_6_18,            0)  AS lcs_all_6_18,
  COALESCE(lcs.lcs_all_7_11,            0)  AS lcs_all_7_11
FROM PatientProviderDayMetrics ppdm
LEFT JOIN ProviderInfo pi
  ON ppdm.user_id = pi.user_id
LEFT JOIN LCS lcs
  ON ppdm.user_id = lcs.user_id AND ppdm.csn = lcs.csn AND ppdm.access_date = lcs.access_date;
