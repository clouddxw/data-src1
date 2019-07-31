source ~/.bash_profile;
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8;
pt_days=$1
pt_month=`date -d "${pt_days}-58days" +"%Y%m"`

hive -S -e "
use lf_airui_pro;
set mapreduce.job.queuename=ia_dzh_airui;
set mapreduce.map.memory.mb = 4096;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.map.java.opts=-Xmx3480m;
set mapreduce.reduce.java.opts=-Xmx3480m;
set mapred.max.split.size=300000000;
set mapred.min.split.size.per.node=300000000; 
set mapred.min.split.size.per.rack=300000000;




select '常驻省份' top1_home_province,
       '常用域名日活' uv1,
       'qq日活' uv2,
       'snssdk日活' uv3,
       'baidu日活' uv4,
       'alipay日活' uv5,
       '163日活' uv6;
select  t1.top1_home_province,
        sum(case when tag1=1 then 1 else 0 end) uv1,
        sum(case when tag2=1 then 1 else 0 end) uv2,
        sum(case when tag3=1 then 1 else 0 end) uv3,
        sum(case when tag4=1 then 1 else 0 end) uv4,
        sum(case when tag5=1 then 1 else 0 end) uv5,
        sum(case when tag6=1 then 1 else 0 end) uv6
from          
    (select device_number,
            max(case when (instr(url_host,'qq.com')>0 or instr(url_host,'snssdk.com')>0 or instr(url_host,'baidu.com')>0 or instr(url_host,'alipay.com')>0 or instr(url_host,'163.com')>0) then 1 else 0 end)  tag1,
            max(case when instr(url_host,'qq.com')>0 then 1 else 0 end)  tag2,
            max(case when instr(url_host,'snssdk.com')>0 then 1 else 0 end)  tag3,
            max(case when instr(url_host,'baidu.com')>0 then 1 else 0 end)  tag4,
            max(case when instr(url_host,'alipay.com')>0 then 1 else 0 end)  tag5,
            max(case when instr(url_host,'163.com')>0 then 1 else 0 end) tag6
    from  user_action_tag_d where pt_days=${pt_days}
    group by device_number) t
left join 
    (select * from 
        (select t.*,
            row_number() over(partition by device_number order by age_range desc) rn
        from user_info_sample t  where pt_days=${pt_month}) a
    where rn=1) t1
on t.device_number=t1.device_number
group by t1.top1_home_province;
"