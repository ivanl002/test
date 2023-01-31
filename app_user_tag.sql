SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapreduce.job.reduces=20;
SET hive.merge.mapredfiles = true;
SET hive.merge.smallfiles.avgsize=128000000;
-- SET hive.execution.engine=spark;

insert overwrite table track.app_user_tag partition(report_date)

-- purchased_total
select user_id, 'purchased_total' as tag_type, purchased_total_tag as tag,
'${report_date}' as report_date
from az.dws_user_tag_order_detail
where purchased_total_tag != '-1'
group by user_id, purchased_total_tag

union all 

-- purchased_wgd
select user_id, 'purchased_wgd' as tag_type, purchased_wgd_tag as tag,
'${report_date}' as report_date
from az.dws_user_tag_order_detail
where purchased_wgd_tag != '-1'
group by user_id, purchased_wgd_tag

union all 

-- purchased_nowgd
-- 这里本来想判断：purchased_total_tag != '-1' and purchased_wgd_tag = '-1'，但是这是订单级别的，没法聚合到用户级别
-- 因为：比如用户可能有多个purchased_total:91_180， 
-- 但是其中一个purchased_total:91_180对应：purchased_wgd:91_180，其他则没有purchased_wgd:91_180，
-- 如果使用上面的判断，这种就会判断为purchased_nowgd了
select total.user_id, 'purchased_nowgd' as tag_type, concat('purchased_nowgd:', total.period) as tag,
'${report_date}' as report_date
from 
(select user_id, split(purchased_total_tag, ':')[1] as period
from az.dws_user_tag_order_detail
where purchased_total_tag != '-1'
group by user_id, split(purchased_total_tag, ':')[1]) as total 
left join 
(select user_id, split(purchased_wgd_tag, ':')[1] as period
from az.dws_user_tag_order_detail
where purchased_wgd_tag != '-1'
group by user_id, split(purchased_wgd_tag, ':')[1]) as wgd 
on total.user_id = wgd.user_id and total.period = wgd.period
-- 如果total中有，wgd中没有，那么这批用户就是nowgd用户
where wgd.user_id is null 

union all 

-- purchased_cat
select user_id, 'purchased_cat' as tag_type, purchased_cat_tag as tag,
'${report_date}' as report_date
from az.dws_user_tag_order_detail
where purchased_cat_tag != '-1'
group by user_id, purchased_cat_tag

union all 

-- purchased_order_type33
select user_id, 'purchased_order_type33' as tag_type, 'purchased_order_type33:30_730' as tag,
'${report_date}' as report_date 
from 
(select user_id, collect_set(cat_id) as cat_ids
from az.dws_user_tag_order_detail
where order_status != 2 
group by user_id) as t 
where array_contains(cat_ids, 33) and !array_contains(cat_ids, 2) and !array_contains(cat_ids, 7) and !array_contains(cat_ids, 8)

union all 

-- purchased_order_type206
select user_id, 'purchased_order_type206' as tag_type, 'purchased_order_type206:30_730' as tag,
'${report_date}' as report_date 
from 
(select user_id, collect_set(cat_id) as cat_ids
from az.dws_user_tag_order_detail
where order_status != 2 
group by user_id) as t 
where (array_contains(cat_ids, 206) or array_contains(cat_ids, 207) or array_contains(cat_ids, 208)) and !array_contains(cat_ids, 2) and !array_contains(cat_ids, 7) and !array_contains(cat_ids, 8)

union all 

-- register_no_purchase
-- 这里是不可能会出现-1的
-- 这里能直接用，因为用户的注册时间只能有一个，也就是说，映射后，用户还是只能有一个标签。
select u.user_id, 
'register_no_purchase' as tag_type, 
case when u.reg_time >= date_sub('${report_date}', 30) and u.reg_time <= '${report_date}' then 'register_no_purchase:1_30' 
     when u.reg_time >= date_sub('${report_date}', 90) and u.reg_time < date_sub('${report_date}', 30) then 'register_no_purchase:31_90' 
     when u.reg_time >= date_sub('${report_date}', 180) and u.reg_time < date_sub('${report_date}', 90) then 'register_no_purchase:91_180' 
     when u.reg_time >= date_sub('${report_date}', 730) and u.reg_time < date_sub('${report_date}', 180) then 'register_no_purchase:181_730' 
     else '-1' end as tag,
'${report_date}' as report_date
from 
az.ods_users as u 
left join  
az.dws_user_tag_order_detail as oi 
on u.user_id = oi.user_id 
where u.reg_time >= date_sub('${report_date}', 730) and u.reg_time <= '${report_date}'
and oi.user_id is null 
and u.email not like '%@tetx.com'
and u.email not like '%@i9i8.com'
and u.email not like '%@azazie.com'
and u.email not like '%@gaoyaya.com'
group by u.user_id,
case when u.reg_time >= date_sub('${report_date}', 30) and u.reg_time <= '${report_date}' then 'register_no_purchase:1_30' 
     when u.reg_time >= date_sub('${report_date}', 90) and u.reg_time < date_sub('${report_date}', 30) then 'register_no_purchase:31_90' 
     when u.reg_time >= date_sub('${report_date}', 180) and u.reg_time < date_sub('${report_date}', 90) then 'register_no_purchase:91_180' 
     when u.reg_time >= date_sub('${report_date}', 730) and u.reg_time < date_sub('${report_date}', 180) then 'register_no_purchase:181_730' 
     else '-1' end 


union all 

-- visit_total_tag
-- 这里还是需要聚合，因为一个用户：visit_total_tag, visit_wgd_tag, addtobag_total_tag, addtobag_wgd_tag可能有多个组合，不能保证单个标签不重复
select user_id, 'visit_total' as tag_type, visit_total_tag as tag,
'${report_date}' as report_date
from track.app_track_user_tag 
where visit_total_tag != '-1'
group by user_id, visit_total_tag

union all 

-- visit_wgd_tag
select user_id, 'visit_wgd' as tag_type, visit_wgd_tag as tag,
'${report_date}' as report_date
from track.app_track_user_tag 
where visit_wgd_tag != '-1'
group by user_id, visit_wgd_tag

union all 

-- visit_nowgd
select total.user_id, 'visit_nowgd' as tag_type, concat('visit_nowgd:', total.period) as tag,
'${report_date}' as report_date
from 
(select user_id, split(visit_total_tag, ':')[1] as period
from track.app_track_user_tag
where visit_total_tag != '-1'
group by user_id, split(visit_total_tag, ':')[1]) as total 
left join 
(select user_id, split(visit_wgd_tag, ':')[1] as period
from track.app_track_user_tag
where visit_wgd_tag != '-1'
group by user_id, split(visit_wgd_tag, ':')[1]) as wgd 
on total.user_id = wgd.user_id and total.period = wgd.period
-- 如果total中有，wgd中没有，那么这批用户就是nowgd用户
where wgd.user_id is null 

union all 

-- addtobag_total
select user_id, 'addtobag_total' as tag_type, addtobag_total_tag as tag,
'${report_date}' as report_date
from track.app_track_user_tag 
where addtobag_total_tag != '-1'
group by user_id, addtobag_total_tag

union all 

-- addtobag_wgd
select user_id, 'addtobag_wgd' as tag_type, addtobag_wgd_tag as tag,
'${report_date}' as report_date
from track.app_track_user_tag 
where addtobag_wgd_tag != '-1'
group by user_id, addtobag_wgd_tag

union all 

-- addtobag_nowgd
select total.user_id, 'addtobag_nowgd' as tag_type, concat('addtobag_nowgd:', total.period) as tag,
'${report_date}' as report_date
from 
(select user_id, split(addtobag_total_tag, ':')[1] as period
from track.app_track_user_tag
where addtobag_total_tag != '-1'
group by user_id, split(addtobag_total_tag, ':')[1]) as total 
left join 
(select user_id, split(addtobag_wgd_tag, ':')[1] as period
from track.app_track_user_tag
where addtobag_wgd_tag != '-1'
group by user_id, split(addtobag_wgd_tag, ':')[1]) as wgd 
on total.user_id = wgd.user_id and total.period = wgd.period
-- 如果total中有，wgd中没有，那么这批用户就是nowgd用户
where wgd.user_id is null 
