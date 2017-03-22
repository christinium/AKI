-- First we drop the table if it exists
DROP MATERIALIZED VIEW IF EXISTS KDIGO_URINE_12HR CASCADE;
CREATE MATERIALIZED VIEW KDIGO_URINE_12HR as
with urine as
(
select ICUSTAY_ID, CHARTTIME, SUM(VALUE) as VALUE
from
(
  select ICUSTAY_ID, CHARTTIME, VALUE
  from mimiciii.outputevents
  where ITEMID in (
  40056,--	Urine Out Foley
  43176,--	Urine .
  40070,--	Urine Out Void
  40095,--	Urine Out Condom Cath
  40716,--	Urine Out Suprapubic
  40474,--	Urine Out IleoConduit
  40058,--	Urine Out Rt Nephrostomy
  40057,--	Urine Out Lt Nephrostomy
  40406,--	Urine Out Other
  40429,--	Urine Out Straight Cath
  40086,--	Urine Out Incontinent
  40097,--	Urine Out Ureteral Stent #1
  40652,--	Urine Out Ureteral Stent #2

  226559,--	Foley
  226560,--	Void
  226582,--	Ostomy (output)
  226561,--	Condom Cath
  226584,--	Ileoconduit
  226563,--	Suprapubic
  226564,--	R Nephrostomy
  226565,--	L Nephrostomy
  226567,--	Straight Cath
  226557,--	R Ureteral Stent
  226558 --	L Ureteral Stent
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
      io.CHARTTIME and (io.CHARTTIME + interval '11' hour)
group by ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID, io.CHARTTIME
order by ie.ICUSTAY_ID, io.CHARTTIME;
