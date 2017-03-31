SET search_path TO mimiciii;
with order_icu as (
    select icu.*
        ,row_number() over(partition by icu.subject_id order by icu.admittime) as rn
    from mimiciii.icustay_detail as icu
    order by subject_id
),
first_icu as (
select * 
from order_icu
where rn = 1
),
first_serv as(
select ie.hadm_id, curr_service as first_service
    , ROW_NUMBER() over (partition by ie.hadm_id order by transfertime DESC) as rn
  from mimiciii.icustays ie
  inner join mimiciii.services se
    on ie.hadm_id = se.hadm_id
    and se.transfertime < ie.intime + interval '1' day
),
esrd_icd as(
select  hadm_id,
	max(case
        when icd9_code like '5856' then 1 -- ESRD
        else 0 end) as ESRD

from diagnoses_icd 
group by hadm_id
),
all_dis_creats as (
select
ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
, le.VALUENUM as DISCREAT
, le.CHARTTIME

-- Create an index that goes from 1, 2, ..., N
-- The index represents how early in the patient's stay a creatinine value was measured
-- Consequently, when we later select index == 1, we only select the first (admission) creatinine
-- In addition, we only select the first stay for the given SUBJECT_ID
, ROW_NUMBER ()
        OVER (PARTITION BY ie.ICUSTAY_ID
              ORDER BY CHARTTIME DESC
                  ) as rn
from first_icu ie
left join labevents le
  on ie.SUBJECT_ID = le.SUBJECT_ID
  and le.ITEMID = 50912
  and le.VALUENUM is not null
  and le.CHARTTIME between (ie.DISCHTIME - interval '96' hour) and (ie.DISCHTIME + interval '24' hour)
),
last_dis_creat as(
select * 
from all_dis_creats
where rn  = 1
)
select first_icu.*, first_serv.first_service, KDIGO_CREAT.admcreat, KDIGO_CREAT.admcreattime, last_dis_creat.discreat, KDIGO_CREAT.highcreat48hr, KDIGO_CREAT.highcreattime48hr, KDIGO_CREAT.highcreat7day, KDIGO_CREAT.highcreattime7day, rrt.rrt, esrd_icd.esrd, oasis.oasis, patients.dod
, Case WHEN first_icu.hospital_expire_flag = 1 THEN  0
  ELSE (date_part('Day', patients.dod-first_icu.dischtime))
  END
as day_after_icu_discharge_death
from first_icu
inner join
mimiciii.KDIGO_CREAT 
on KDIGO_CREAT.icustay_id = first_icu.icustay_id
join
mimiciii.rrt
on KDIGO_CREAT.icustay_id = rrt.icustay_id
join
mimiciii.oasis
on oasis.icustay_id = first_icu.icustay_id
left join mimiciii.patients
on patients.subject_id = first_icu.subject_id
join esrd_icd
on first_icu.hadm_id = esrd_icd.hadm_id
join last_dis_creat
on last_dis_creat.subject_id = first_icu.subject_id
join first_serv
on first_serv.hadm_id = first_icu.hadm_id
where first_icu.age >=18
order by first_icu.subject_id

--52862
--46428

