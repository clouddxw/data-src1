drop table if exists tmp_xima_model_t1;
create table tmp_xima_model_t1 as
select b.*,
       b1.device_number device_number1
    from 
    (select      pt_days,
                 device_number,
                 tag1+tag2+tag3+tag4+tag5+tag6+tag7+tag8+tag9+tag10+tag11 tag
            from 
                (select substr(pt_days,0,6) pt_days,
                        device_number,
                        max(case when instr(urltag,'categoryId=')>0 then 1 else 0 end) tag1,
                        max(case when url_host ='liveroom.ximalaya.com' then 1 else 0 end) tag2,
                        max(case when url_host ='mp.ximalaya.com' and instr(urltag,'recharge')>0 then 1 else 0 end) tag3,
                        max(case when url_host ='searchwsa.ximalaya.com'  then 1 else 0 end) tag4,
                        max(case when url_host ='ad.ximalaya.com' and instr(urltag,'positionName=focus%categoryId=-2')>0 then 1 else 0 end) tag5,
                        max(case when url_host='mwsa.ximalaya.com' and instr(urltag,'vip%channel%categoryId=-8')>0 then 1 else 0 end) tag6,
                        max(case when url_host='ifm.ximalaya.com' and instr(urltag,'daily')>0 then 1 else 0 end) tag7,
                        max(case when instr(urltag,'AggregateRankListTabs')>0 then 1 else 0 end) tag8,
                        max(case when instr(urltag,'recommends%categoryId=33')>0 then 1 else 0 end) tag9,
                        max(case when instr(urltag,'topBuzz')>0 then 1 else 0 end) tag10,
                        max(case when instr(urltag,'sceneId=')>0 then 1 else 0 end) tag11
                    from  user_action_tag_d
                    where pt_days>='20190301' and pt_days<='20190631'  
                    and instr(url_host,'ximalaya.com')>0
                    group BY
                        substr(pt_days,0,6),
                        device_number) a) b
        left join 
            user_action_app_m b1 
        ON (cast(b.pt_days as bigint)+1)=b1.pt_days
        and b.device_number=b1.device_number;



select  '喜马拉雅模块使用及次月留存统计' model,
        '月份' pt_days,
        '>=1cnt'      cnt1,
        '>=2cnt'     cnt2,
        '>=3cnt'     cnt3,
        '>=4cnt'     cnt4,
        '>=5cnt'     cnt5,
        '>=6cnt'     cnt6,
        '>=7cnt'     cnt7,
        '>=8cnt'     cnt8,
        '>=9cnt'     cnt9,
        '>=10cnt'     cnt10,
        '>=11cnt'     cnt11,
        '>=1cnt_次月留存'      cy_cnt1,
        '>=2cnt_次月留存'     cy_cnt2,
        '>=3cnt_次月留存'     cy_cnt3,
        '>=4cnt_次月留存'     cy_cnt4,
        '>=5cnt_次月留存'     cy_cnt5,
        '>=6cnt_次月留存'     cy_cnt6,
        '>=7cnt_次月留存'     cy_cnt7,
        '>=8cnt_次月留存'     cy_cnt8,
        '>=9cnt_次月留存'     cy_cnt9,
        '>=10cnt_次月留存'     cy_cnt10,
        '>=11cnt_次月留存'     cy_cnt11
union all 
select '喜马拉雅模块使用及次月留存统计' model,
       pt_days,
       sum(case when tag>=1 then 1 else 0 end) cnt1,
       sum(case when tag>=2 then 1 else 0 end) cnt2,
       sum(case when tag>=3 then 1 else 0 end) cnt3,
       sum(case when tag>=4 then 1 else 0 end) cnt4,
       sum(case when tag>=5 then 1 else 0 end) cnt5,
       sum(case when tag>=6 then 1 else 0 end) cnt6,
       sum(case when tag>=7 then 1 else 0 end) cnt7,
       sum(case when tag>=8 then 1 else 0 end) cnt8,
       sum(case when tag>=9 then 1 else 0 end) cnt9,
       sum(case when tag>=10 then 1 else 0 end) cnt10,
       sum(case when tag>=11 then 1 else 0 end) cnt11,
       count(distinct(case when tag>=1 then device_number1 else 0 end)) cy_cnt1,
       count(distinct(case when tag>=2 then device_number1 else 0 end)) cy_cnt2,
       count(distinct(case when tag>=3 then device_number1 else 0 end)) cy_cnt3,
       count(distinct(case when tag>=4 then device_number1 else 0 end)) cy_cnt4,
       count(distinct(case when tag>=5 then device_number1 else 0 end)) cy_cnt5,
       count(distinct(case when tag>=6 then device_number1 else 0 end)) cy_cnt6,
       count(distinct(case when tag>=7 then device_number1 else 0 end)) cy_cnt7,
       count(distinct(case when tag>=8 then device_number1 else 0 end)) cy_cnt8,
       count(distinct(case when tag>=9 then device_number1 else 0 end)) cy_cnt9,
       count(distinct(case when tag>=10 then device_number1 else 0 end)) cy_cnt10,
       count(distinct(case when tag>=11 then device_number1 else 0 end)) cy_cnt11
from 
    tmp_xima_model_t1 b
group by pt_days;


drop table if exists tmp_xima_model_t1;



select '网易新闻日活-新规则' model,
       pt_days,
       count(distinct device_number) uv,
       count(*) pv 
from user_action_tag_d a 
where pt_days>='20190601' 
  and (url_host in ('m.analytics.126.net','comment.news.163.com','comment.api.163.com','v.monitor.ws.netease.com','nex.163.com','c.m.163.com','nimg.ws.126.net') 
       or url_host like 'img%.126.net')
    group by pt_days;

select '网易新闻月活-新规则' model,
       '201906' pt_days,
       count(distinct device_number) uv,
       count(*) pv 
from user_action_tag_d a 
where pt_days>='20190601' and pt_days<='20190631'
  and (url_host in ('m.analytics.126.net','comment.news.163.com','comment.api.163.com','v.monitor.ws.netease.com','nex.163.com','c.m.163.com','nimg.ws.126.net') 
       or url_host like 'img%.126.net');