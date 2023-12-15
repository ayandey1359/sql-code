-- create table 
create table pakriti(
   new_id int ,
   new_cat varchar(50)
)

-- inserting values
insert into pakriti ( new_id,new_cat)
values ( 100,'Agni'),(200,'Agni'),
       (500,'Dharti'),(700,'Dharti'),
	   (200,'Vayu'),(300,'Vayu'),(500,'Vayu')

-- data contain 
select * from pakriti

-- applying window function on this data
-- 1. Aggregate Function .SUM. AVG . COUNT . MIN . MAX 
select new_id,new_cat,
sum(new_id)over(partition by new_cat order by new_id),
avg(new_id)over(partition by new_cat order by new_id),
count(new_cat)over(partition by new_cat order by new_id),
min(new_id)over(partition by new_cat order by new_id),
max(new_id)over(partition by new_cat order by new_id)
from pakriti

-- 2. Ranking function . ROW_NUMBER . RANK . DENSE_RANK . PERCENT_RANK ( How much data covered)
select new_id ,
row_number() over(order by new_id) as row_id,
rank() over(order by new_id) as rank_id,
dense_rank() over(order by new_id) as compact_rank,
percent_rank() over(order by new_id) as data_covered_rank
from pakriti

-- Analytical Function . FIRST_VALUE . LEAD . LAG
select new_id,new_cat,
first_value(new_id)over(order by new_id) as topp,
last_value(new_id)over(order by new_id rows between unbounded preceding and unbounded following)as below,
-- to get participate all rows
lead(new_id)over (order by new_id) as lead_by1, -- by defult it will lead by 1
lag(new_id,2) over(order by new_id) as lag_by2 -- specify the lagging value by 2
from pakriti

/* note : 
 UNBOUNDED PRECEDING : all rows before current row are get participate
 UNBOUNDED FOLLOWING: all row after current row are get participate
 CURRENT ROW :range start or end at CURRENT ROW
*/

