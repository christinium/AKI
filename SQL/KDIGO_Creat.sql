DROP CASCADE MATERIALIZED IF EXISTS KDIGO_CREAT;
CREATE MATERIALIZED VIEW KDIGO_CREAT as
with admcr as (
select
ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
, le.VALUENUM as AdmCreat
, le.CHARTTIME

-- Create an index that goes from 1, 2, ..., N
-- The index represents how early in the patient's stay a creatinine value was measured
-- Consequently, when we later select index == 1, we only select the first (admission) creatinine
-- In addition, we only select the first stay for the given SUBJECT_ID
, ROW_NUMBER ()
        OVER (PARTITION BY ie.ICUSTAY_ID
              ORDER BY CHARTTIME
                  ) as rn
from mimiciii.icustays ie
left join labevents le
  on ie.SUBJECT_ID = le.SUBJECT_ID
  and le.ITEMID = 50912
  and le.VALUENUM is not null
-- admission creatinine defined as [-6,24] from admission
  and le.CHARTTIME between (ie.INTIME - interval '6' hour) and (ie.INTIME + interval '1' day)
),

-- *****
-- Query to extract highest creatinine within 48 hours
-- *****
highcr as (
select
ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
, le.VALUENUM as HighCreat
, le.CHARTTIME

-- Create an index that goes from 1, 2, ..., N
-- The index represents how high a creatinine value is
-- Consequently, when we later select index == 1, we only select the highest creatinine
, ROW_NUMBER ()
        OVER (PARTITION BY ie.ICUSTAY_ID
              ORDER BY le.VALUENUM DESC
                  ) as rn
from mimiciii.icustays ie
left join labevents le
  on ie.SUBJECT_ID = le.SUBJECT_ID
  and le.ITEMID = 50912
  and le.VALUENUM is not null
  -- highest creatinine defined as [-6,48] from admission
  and le.CHARTTIME between (ie.INTIME - interval '6' hour) and (ie.INTIME + interval '2' day)
),
-- *****
-- Query to extract highest creatinine within 7 days
-- *****
highcr7day as (
  select
  ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
  , le.VALUENUM as HighCreat
  , le.CHARTTIME

  -- Create an index that goes from 1, 2, ..., N
  -- The index represents how high a creatinine value is
  -- Consequently, when we later select index == 1, we only select the highest creatinine
  , ROW_NUMBER ()
          OVER (PARTITION BY ie.ICUSTAY_ID
                ORDER BY le.VALUENUM DESC
                    ) as rn
  from mimiciii.icustays ie
  left join labevents le
    on ie.SUBJECT_ID = le.SUBJECT_ID
    and le.ITEMID = 50912
    and le.VALUENUM is not null
    -- highest creatinine between [-6,24*7] hours from admission
    and le.CHARTTIME between (ie.INTIME - interval '6' hour) and (ie.INTIME + interval '7' day)
)
-- *****
-- Final query
-- *****
select
b.SUBJECT_ID, b.HADM_ID, b.ICUSTAY_ID, b.INTIME
, admcr.AdmCreat, admcr.CHARTTIME as AdmCreatTime
, highcr.HighCreat as HighCreat48hr, highcr.CHARTTIME as HighCreatTime48hr
, highcr7day.HighCreat as HighCreat7day, highcr7day.CHARTTIME as HighCreatTime7day
--, db.DB, db.TIME as DBTIME
from icustays b
left join admcr on b.ICUSTAY_ID = admcr.ICUSTAY_ID and admcr.rn = 1
left join highcr7day on b.ICUSTAY_ID = highcr7day.ICUSTAY_ID and highcr7day.rn = 1
left join highcr on b.ICUSTAY_ID = highcr.ICUSTAY_ID and highcr.rn = 1;
