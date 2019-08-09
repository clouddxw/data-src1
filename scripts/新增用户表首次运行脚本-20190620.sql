
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =10000;
insert into user_action_app_new_d
partition(pt_days)
select pt_days,
       appname,
       device_number
    from 
     (select t.*,
             row_number() over(partition by appname,device_number order by pt_days asc) rn
          from  user_action_app_d t
          where pt_days>='20190301') a
    where rn=1;