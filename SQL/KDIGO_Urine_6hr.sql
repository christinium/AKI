-- First we drop the table if it exists
DROP MATERIALIZED VIEW IF EXISTS KDIGO_URINE_6HR CASCADE;
CREATE MATERIALIZED VIEW KDIGO_URINE_6HR as
with urine as
(
select ICUSTAY_ID, CHARTTIME, SUM(VALUE) as VALUE
from
(
  select ICUSTAY_ID, CHARTTIME, VALUE
  from mimiciii.outputevents
  where ITEMID in (
    -- these are the most frequently occurring urine output observations in CareVue
    40055, -- "Urine Out Foley"
    43175, -- "Urine ."
    40069, -- "Urine Out Void"
    40094, -- "Urine Out Condom Cath"
    40715, -- "Urine Out Suprapubic"
    40473, -- "Urine Out IleoConduit"
    40085, -- "Urine Out Incontinent"
    40057, -- "Urine Out Rt Nephrostomy"
    40056, -- "Urine Out Lt Nephrostomy"
    40405, -- "Urine Out Other"
    40428, -- "Urine Out Straight Cath"
    40086,--	Urine Out Incontinent
    40096, -- "Urine Out Ureteral Stent #1"
    40651, -- "Urine Out Ureteral Stent #2"

    -- these are the most frequently occurring urine output observations in CareVue
    226559, -- "Foley"
    226560, -- "Void"
    227510, -- "TF Residual"
    226561, -- "Condom Cath"
    226584, -- "Ileoconduit"
    226563, -- "Suprapubic"
    226564, -- "R Nephrostomy"
    226565, -- "L Nephrostomy"
    226567, --	Straight Cath
    226557, -- "R Ureteral Stent"
    226558  -- "L Ureteral Stent"
  )
  and VALUE < 5000 -- sanity check on urine value
  and ICUSTAY_ID is not null
) tmp
group by ICUSTAY_ID, CHARTTIME
)
select ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID, io.CHARTTIME
, sum(iosum.VALUE) as MaxUrineOutput
from mimiciii.icustays ie
-- these two joins give you the maximum UO over a 6 hour period
left join urine io
  on ie.ICUSTAY_ID = io.ICUSTAY_ID
left join urine iosum
  on ie.ICUSTAY_ID = iosum.ICUSTAY_ID
  and iosum.CHARTTIME between
      io.CHARTTIME and (io.CHARTTIME + interval '5' hour)
group by ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID, io.CHARTTIME
order by ie.ICUSTAY_ID, io.CHARTTIME;
