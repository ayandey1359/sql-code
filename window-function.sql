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
sum(new_id)over(partition by new_cat order by new_id desc)
from pakriti