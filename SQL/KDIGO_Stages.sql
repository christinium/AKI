DROP MATERIALIZED VIEW IF EXISTS KDIGO_STAGES;
CREATE MATERIALIZED VIEW KDIGO_STAGES AS
-- Extract weight from height/weight query + echo data
with wt as
(
  select ie.ICUSTAY_ID
    , avg(coalesce(hw.weight_avg, ed.weight)) as weight
  from icustays ie
  left join heightweight hw
    on  ie.hadm_id = hw.hadm_id
  left join ECHODATA ed
    on  ie.hadm_id = ed.hadm_id
    and ed.charttime between ie.intime - interval '3' day and ie.intime + interval '1' day
  group by ie.icustay_id
)
, ur6 as (
select ur.ICUSTAY_ID, ie.INTIME
, CHARTTIME, MaxUrineOutput
, ROW_NUMBER () OVER (PARTITION BY ur.ICUSTAY_ID ORDER BY MaxUrineOutput, CHARTTIME) as rn
, ROW_NUMBER () OVER (PARTITION BY ur.ICUSTAY_ID ORDER BY CHARTTIME DESC) as lastChart
from KDIGO_URINE_6hr ur
inner join icustays ie
  on ur.icustay_id = ie.icustay_id
where CHARTTIME between (ie.INTIME - interval '6' hour) and (ie.INTIME + interval '2' day)
)
, ur12 as (
select ur.ICUSTAY_ID, ie.INTIME
, CHARTTIME, MaxUrineOutput
, ROW_NUMBER () OVER (PARTITION BY ur.ICUSTAY_ID ORDER BY MaxUrineOutput, CHARTTIME) as rn
, ROW_NUMBER () OVER (PARTITION BY ur.ICUSTAY_ID ORDER BY CHARTTIME DESC) as lastChart
from KDIGO_URINE_12hr ur
inner join icustays ie
  on ur.icustay_id = ie.icustay_id
where CHARTTIME between (ie.INTIME - interval '6' hour) and (ie.INTIME + interval '2' day)
)
, ur24 as (
select ur.ICUSTAY_ID, ie.INTIME
, CHARTTIME, MaxUrineOutput
, ROW_NUMBER () OVER (PARTITION BY ur.ICUSTAY_ID ORDER BY MaxUrineOutput, CHARTTIME) as rn
, ROW_NUMBER () OVER (PARTITION BY ur.ICUSTAY_ID ORDER BY CHARTTIME DESC) as lastChart
from KDIGO_URINE_24hr ur
inner join icustays ie
  on ur.icustay_id = ie.icustay_id
where CHARTTIME between (ie.INTIME - interval '6' hour) and (ie.INTIME + interval '2' day)
)
-- merge together the various urine estimates with weight
, ur as (
select  ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
, wt.weight
, ur6.CHARTTIME as UO_6hr_TIME
, cast(case when wt.weight is NULL then NULL
    when wt.weight = 0 then NULL
    else ur6.MaxUrineOutput / wt.weight end
    as numeric)
    as UO_6hr
, ur12.CHARTTIME as UO_12hr_TIME
, cast(case when wt.weight is NULL then NULL
    when wt.weight = 0 then NULL
    else ur12.MaxUrineOutput / wt.weight end
    as numeric)
    as UO_12hr
, ur24.CHARTTIME as UO_24hr_TIME
, cast(case when wt.weight is NULL then NULL
    when wt.weight = 0 then NULL
    else ur24.MaxUrineOutput / wt.weight end
    as numeric)
    as UO_24hr
from icustays ie
left join wt   on ie.ICUSTAY_ID = wt.ICUSTAY_ID
left join ur6  on ie.ICUSTAY_ID = ur6.ICUSTAY_ID  and ur6.rn  = 1
left join ur12 on ie.ICUSTAY_ID = ur12.ICUSTAY_ID and ur12.rn = 1
left join ur24 on ie.ICUSTAY_ID = ur24.ICUSTAY_ID and ur24.rn = 1
)
select ur.ICUSTAY_ID, ur.weight
-- First, whether the patient has AKI or not
, case
    when HighCreat48hr >= (AdmCreat+0.3) then 1
    when HighCreat48hr >= (AdmCreat*1.5) then 1
    when UO_6hr < (0.5)  then 1 -- and we also check for low UO (== AKI)
    when AdmCreat is null then null
  else 0 end as AKI
-- First, the final AKI stages: either 48 hour or 7 day (according to creat)
, case
  when HighCreat48hr >= (AdmCreat*3.0) then 3
  when HighCreat48hr >= 4 -- note the criteria specify an INCREASE to >=4
    and AdmCreat <= (3.7)  then 3 -- therefore we check that adm <= 3.7
  when UO_24hr < 0.3  then 3
  when UO_12hr = 0  then 3 -- anuria for >= 12 hours
  -- TODO: initiation of RRT
  when HighCreat48hr >= (AdmCreat*2.0) then 2
  when UO_12hr < 0.5 then 2
  when HighCreat48hr >= (AdmCreat+0.3) then 1
  when HighCreat48hr >= (AdmCreat*1.5) then 1
  when UO_6hr  < 0.5 then 1
  when UO_12hr < 0.5 then 1
  when HighCreat48hr is null then null
    when AdmCreat is null then null
  else 0 end as AKI_stage_48hr

-- First, the final AKI stages: either 48 hour or 7 day (according to creat)
, case
  when HighCreat7day >= (AdmCreat*3.0) then 3
  when HighCreat7day >= 4 -- note the criteria specify an INCREASE to >=4
    and AdmCreat <= (3.7)  then 3 -- therefore we check that adm <= 3.7
  when UO_24hr < 0.3  then 3
  when UO_12hr = 0  then 3 -- anuria for >= 12 hours
  -- TODO: initiation of RRT
  when HighCreat7day >= (AdmCreat*2.0) then 2
  when UO_12hr < 0.5 then 2
  when HighCreat7day >= (AdmCreat+0.3) then 1
  when HighCreat7day >= (AdmCreat*1.5) then 1
  when UO_6hr  < 0.5 then 1
  when UO_12hr < 0.5 then 1
  when HighCreat7day is null then null
    when AdmCreat is null then null
  else 0 end as AKI_stage_7day

-- AKI stages according to urine output
, case
    when ur.UO_24hr < 0.3 then 3
    when ur.UO_12hr = 0 then 3
    when ur.UO_12hr < 0.5 then 2
    when ur.UO_6hr < 0.5 then 1
    when ur.UO_6hr is null then null
  else 0 end as AKI_Stage_Urine
, case
    when ur.UO_6hr < 0.5 then 1
    when ur.UO_6hr is null then null
  else 0 end as AKI_Urine
-- Creatinine information
  , AdmCreat
  , HighCreatTime48hr, HighCreat48hr
  , HighCreatTime7day, HighCreat7day
-- Urine output information: the values and the time of their measurement
, UO_6hr_TIME
, round(ur.UO_6hr,4) as UO_6hr
, UO_12hr_TIME
, round(ur.UO_12hr,4) as UO_12hr
, UO_24hr_TIME
, round(ur.UO_24hr,4) as UO_24hr
from ur
left join KDIGO_CREAT cr on ur.ICUSTAY_ID = cr.ICUSTAY_ID
order by ur.SUBJECT_ID;
