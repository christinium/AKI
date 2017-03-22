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
)
select first_icu.*, KDIGO_CREAT.admcreat, KDIGO_CREAT.admcreattime, KDIGO_CREAT.highcreat48hr, KDIGO_CREAT.highcreattime48hr, KDIGO_CREAT.highcreat7day, KDIGO_CREAT.highcreattime7day, rrt.rrt, oasis.oasis, patients.dod
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
where first_icu.age >=18
left join mimiciii.patients
on patients.subject_id = first_icu.subject_id
order by first_icu.subject_id
--52862
--46428

