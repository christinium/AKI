with uo_base as
(
  select ICUSTAY_ID, CHARTTIME, sum(VALUE) as VALUE
  from outputevents
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

      -- these are the most frequently occurring urine output observations in Metavision
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
  and ICUSTAY_ID < 200100
  GROUP BY ICUSTAY_ID, CHARTTIME
)
select ie.ICUSTAY_ID
  , io.charttime as STARTTIME
  , max(iosum.CHARTTIME) as ENDTIME
  , sum(case when iosum.CHARTTIME = io.CHARTTIME
            then null
          else iosum.VALUE end)
      as UrineOutput
  , sum(case when iosum.CHARTTIME = io.CHARTTIME
            then null
          else iosum.VALUE end) / (extract(epoch from max(iosum.CHARTTIME-io.CHARTTIME))/60.0/60.0)
      as UrineOutputPerHour
  , max((extract(epoch from iosum.CHARTTIME - io.charttime)/60.0/60.0)) as DELTA_T
from icustays ie
-- these two joins give you the maximum UO over a 6 hour period
left join uo_base io
  on ie.ICUSTAY_ID = io.ICUSTAY_ID
left join uo_base iosum
  on  ie.ICUSTAY_ID = iosum.ICUSTAY_ID
  and iosum.CHARTTIME >= io.CHARTTIME
  and iosum.CHARTTIME <= io.CHARTTIME + interval '6' hour
group by ie.ICUSTAY_ID, ie.intime, io.CHARTTIME
order by ie.ICUSTAY_ID, io.CHARTTIME;
