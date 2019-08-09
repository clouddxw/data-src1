#!/bin/bash

#头条出网表数据追溯

for ((day=20190701;day<=20190722;day++)) 
do
      if [ $day == 20190701 ]
            then 
            hive -e"
            use lf_airui_pro;
            set mapreduce.job.queuename=ia_dzh_airui;
            set mapreduce.map.memory.mb = 4096;
            set mapreduce.reduce.memory.mb=4096;
            set mapreduce.map.java.opts=-Xmx3480m;
            set mapreduce.reduce.java.opts=-Xmx3480m;
            set mapred.max.split.size=300000000;
            set mapred.min.split.size.per.node=300000000; 
            set mapred.min.split.size.per.rack=300000000;
            drop table if exists tmp_tt_con_userinfo;
            create table tmp_tt_con_userinfo as
            select * from 
                (select 
                    a1.user_id,
                    a.device_number,
                    a.cust_sex,
                    a.age_range,
                    a.top1_home_province,
                    a.top1_home_city,
                    a.factory_desc,
                    a.cost_level,
                    row_number() over(partition by device_number order by user_id) rn
                from 
                    (select * from user_info_sample where pt_days='201906') a,
                    (select * from zba_dwa.dwa_v_m_cus_nm_user_info where month_id='201906') a1
                where a.device_number=a1.device_number) b 
            where b.rn=1;

            insert overwrite table user_action_context_out partition (pt_days=${day})
            select  binaryconversion(murmurhash(case when t1.user_id is not null then t1.user_id else '1000000000' end)) as irt,
                    t1.cust_sex,
                    t1.age_range,
                    t1.top1_home_province,
                    t1.top1_home_city,
                    t1.factory_desc,
                    t1.cost_level,
                    t.tt_con,
                    t.tx_con
            from  
                (select  pt_days,
                         device_number,
                         concat_ws('$',collect_set(concat_ws(':',con1,con3))) tt_con,
                         model  tx_con 
                from  user_action_context_d  a
                where pt_days = ${day}
                    and model in ('toutiao_article','tencent_news_article','tencent_news_video') 
                    and length(trim(con1))>0
                group by pt_days,device_number,model 
                union all  
                select  pt_days, 
                        device_number, 
                        concat_ws('$',collect_set(con1)) tt_con, 
                        'ximalaya' tx_con 
                        from  
                        (select pt_days, 
                                device_number, 
                                con1, 
                                floor(cast(con5 as bigint)/180) ceil 
                    from  tmp_user_con_out_t1 a
                    where pt_days = ${day}
                      and length(trim(con1))>0 
                    group by pt_days,device_number,con1,floor(cast(con5 as bigint)/180)) a 
                        group by pt_days,device_number) t
            right join tmp_tt_con_userinfo t1
            on t.device_number=t1.device_number;
"
      else 
            hive -e"
            use lf_airui_pro;
            set mapreduce.job.queuename=ia_dzh_airui;
            set mapreduce.map.memory.mb = 4096;
            set mapreduce.reduce.memory.mb=4096;
            set mapreduce.map.java.opts=-Xmx3480m;
            set mapreduce.reduce.java.opts=-Xmx3480m;
            set mapred.max.split.size=300000000;
            set mapred.min.split.size.per.node=300000000; 
            set mapred.min.split.size.per.rack=300000000;
            insert overwrite table user_action_context_out partition (pt_days=${day})
            select  binaryconversion(murmurhash(case when t1.user_id is not null then t1.user_id else '1000000000' end)) as irt,
                    t1.cust_sex,
                    t1.age_range,
                    t1.top1_home_province,
                    t1.top1_home_city,
                    t1.factory_desc,
                    t1.cost_level,
                    t.tt_con,
                    t.tx_con
            from  
                (select  pt_days,
                         device_number,
                         concat_ws('$',collect_set(concat_ws(':',con1,con3))) tt_con,
                         model  tx_con 
                from  user_action_context_d  a
                where pt_days = ${day}
                    and model in ('toutiao_article','tencent_news_article','tencent_news_video') 
                    and length(trim(con1))>0
                group by pt_days,device_number,model 
                union all  
                select  pt_days, 
                        device_number, 
                        concat_ws('$',collect_set(con1)) tt_con, 
                        'ximalaya' tx_con 
                        from  
                        (select pt_days, 
                                device_number, 
                                con1, 
                                floor(cast(con5 as bigint)/180) ceil 
                    from  tmp_user_con_out_t1 a
                    where pt_days = ${day}
                      and length(trim(con1))>0 
                    group by pt_days,device_number,con1,floor(cast(con5 as bigint)/180)) a 
                        group by pt_days,device_number) t
            right join tmp_tt_con_userinfo t1
            on t.device_number=t1.device_number;"
      fi 
done


