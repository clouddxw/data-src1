source ~/.bash_profile;
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8;
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



drop table if exists tmp_user_app_yy_t2;
create table tmp_user_app_yy_t2 as
select t.*,
       t1.device_number device_number1
    from 
       (select  pt_days,
                appname,
                device_number,
                pv
       from 
        user_action_app_d
      where pt_days<='20190531'
      union all
      select    pt_days,
                appname,
                device_number,
                pv
       from 
        user_action_app_d
      where pt_days>='20190601' and pt_days<='20190631'
        and appname not in ('qq_music','wangyi_music')
      union all
      select  pt_days,
              case when url_host  in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com') then 'qq_music'
                    when url_host in ('interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com') then 'wangyi_music'
                    else null
              end appname,
              device_number,
              count(*) pv 
      from user_action_tag_d 
      where pt_days>='20190601' and pt_days<='20190631'
      and url_host in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com','interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com')
        group by 
            pt_days,
            device_number,
            case when url_host  in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com') then 'qq_music'
                  when url_host in ('interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com') then 'wangyi_music'
                  else null
            end) t
     left join 
        (select device_number 
         from zba_dwa.dwa_v_m_cus_nm_user_info b  
         where b.month_id='201905'
         and b.product_class not in ('90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')
            group by device_number
         ) t1
      on t.device_number=t1.device_number;



drop table if exists tmp_user_app_yy_t3;
create table tmp_user_app_yy_t3 as
select        t.*,
              t1.device_number device_number1
                from 
                 (select * from  (select pt_days,
                                    appname,
                                    device_number,
                                    pv,
                                    row_number() over(partition by appname,device_number order by pt_days asc) rn  from  tmp_user_app_yy_t2 where pt_days >= '20190301')  t 
                            where t.rn=1) t
                left join 
                  (select device_number 
                    from zba_dwa.dwa_v_m_cus_nm_user_info b  
                     where b.month_id='201905'
                       and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                            '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                            '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                            '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')
                      group by device_number
                  ) t1
              on t.device_number=t1.device_number
              and t.rn=1;


drop table if exists tmp_user_app_yy_t1;
create table tmp_user_app_yy_t1 as
     select     pt_days,
                appname,
                device_number,
                active_days,
                pv
         from user_action_app_m where pt_days<='201905'
    union all
    select      '201906' pt_days,
                appname,
                device_number,
                count(distinct pt_days) active_days,
                sum(pv) pv
       from tmp_user_app_yy_t2
			 where pt_days>='20190601' and pt_days<='20190631'
      group by  appname,
                device_number;

select '-- -- -*******************************************************************************************************************-- -- -';

select '月份' month_id,
       '各家app永久新增' model,
       'App' appname,
       '日期' pt_days,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select '201906' month_id,
      '各家app永久新增' model,
       appname,
       pt_days,
       count(distinct device_number) uv,
       count(distinct device_number1) uv1
 from  
      tmp_user_app_yy_t3
 where  pt_days>='20190601'
group by pt_days,
         appname
order by appname,
         pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';


select '月份' month_id,
       '新闻各app新增来源' model,
       '统计App' tarapp,
       '来源App' srcapp,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select '201906' month_id,
       '新闻各app新增来源' model,
        tarapp,
        srcapp,
        count(distinct device_number) uv,
        count(distinct device_number1) uv1
 from 
(select b.pt_days,
        b.device_number,
        b.device_number1,
        b.tarapp,
        b1.appname srcapp,
        datediff(concat_ws('-',substr(b.pt_days,1,4),substr(b.pt_days,5,2),substr(b.pt_days,7,2)),concat_ws('-',substr(b1.pt_days,1,4),substr(b1.pt_days,5,2),substr(b1.pt_days,7,2))) diff
  from 
      (select device_number,
              device_number1,
              pt_days,
              appname tarapp
          from 
            tmp_user_app_yy_t3
          where pt_days>='20190601' and pt_days<='20190631' 
            and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')) b 
left join 
      (select
                device_number,
                pt_days,
                appname
            from  tmp_user_app_yy_t2
            where pt_days >= '20190301'
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
            )  b1
on  b.device_number=b1.device_number) c
where c.diff>0 and c.diff<=90
group by tarapp,
         srcapp;






select '月份' month_id,
       '音乐各app新增来源' model,
       '统计App' tarapp,
       '来源App' srcapp,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select '201906' month_id,
       '音乐各app新增来源' model,
        tarapp,
        srcapp,
        count(distinct device_number) uv,
        count(distinct device_number1) uv1
 from 
(select b.pt_days,
        b.device_number,
        b.device_number1,
        b.tarapp,
        b1.appname srcapp,
        datediff(concat_ws('-',substr(b.pt_days,1,4),substr(b.pt_days,5,2),substr(b.pt_days,7,2)),concat_ws('-',substr(b1.pt_days,1,4),substr(b1.pt_days,5,2),substr(b1.pt_days,7,2))) diff
  from 
      (select device_number,
              device_number1,
              pt_days,
              appname tarapp
          from 
            tmp_user_app_yy_t3
          where pt_days>='20190601' and pt_days<='20190631' 
            and appname in ('qq_music','kuwo','kugou','wangyi_music')) b 
left join 
      (select
                device_number,
                pt_days,
                appname
            from  tmp_user_app_yy_t2
            where pt_days >= '20190301'
             and appname in ('qq_music','kuwo','kugou','wangyi_music')       
            )  b1
on  b.device_number=b1.device_number) c
where c.diff>0 and c.diff<=90
group by tarapp,
         srcapp;



select
       '各app201905新增-201906留存' model,
       'App' appname,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select
       '各app201905新增-201906留存' model,
       b.appname,
       count(distinct b.device_number) uv,
       count(distinct b.device_number1) uv1
 from 
(select device_number,
        device_number1,
        appname 
     from 
       tmp_user_app_yy_t3
    where pt_days>='20190501' and pt_days<='20190531' 
  group by device_number,
           device_number1,
           appname) b 
  where exists 
    (select * 
         from 
       (select
                device_number,
                appname
            from  tmp_user_app_yy_t1
            where pt_days = '201906'      
            )  b1
        where  b.device_number=b1.device_number
          and  b.appname=b1.appname) 
group by b.appname;

select '-- -- -*******************************************************************************************************************-- -- -';

select
       '各app201904新增-201905,06留存' model,
       'App' appname,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select
       '各app201904新增-201905,06留存' model,
       b.appname,
       count(distinct b.device_number) uv,
       count(distinct b.device_number1) uv1
 from 
(select device_number,
        device_number1,
        appname 
     from 
       tmp_user_app_yy_t3
    where pt_days>='20190401' and pt_days<='20190431' 
  group by device_number,
           device_number1,
           appname) b 
  where exists 
    (select * 
         from 
       (select
                device_number,
                appname,
                count(distinct pt_days) am
            from  tmp_user_app_yy_t1
            where pt_days in ('201905','201906') 
          group by device_number,
                   appname    
            )  b1
        where  b.device_number=b1.device_number
          and  b.appname=b1.appname
          and  b1.am=2) 
group by b.appname;

drop table if exists tmp_user_app_yy_t1;
drop table if exists tmp_user_app_yy_t2;
drop table if exists tmp_user_app_yy_t3;


"
