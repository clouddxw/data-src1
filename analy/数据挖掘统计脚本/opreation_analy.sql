
-- -- 天表加工
-- drop table if exists tmp_user_app_yy_d;
-- create table tmp_user_app_yy_d as
-- select t.*,
--        t1.device_number device_number1
--     from
--      (select  pt_days,
--               appname,
--               device_number,
--               pv
--        from
--         user_action_app_d
--       where pt_days<='20190531'
--       union all
--       select    pt_days,
--                 appname,
--                 device_number,
--                 pv
--        from
--         user_action_app_d
--       where pt_days>='20190601' and pt_days<='20190731'
--         and appname not in ('qq_music','wangyi_music')
--       union all
--       select  pt_days,
--               case when url_host  in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com') then 'qq_music'
--                     when url_host in ('interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com') then 'wangyi_music'
--                     else null
--               end appname,
--               device_number,
--               count(*) pv
--       from user_action_tag_d
--       where pt_days>='20190601' and pt_days<='20190731'
--       and url_host in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com','interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com')
--         group by
--             pt_days,
--             device_number,
--             case when url_host  in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com') then 'qq_music'
--                   when url_host in ('interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com') then 'wangyi_music'
--                   else null
--             end)  t
--      left join
--         (select device_number
--          from zba_dwa.dwa_v_m_cus_nm_user_info b
--          where b.month_id='201905'
--          and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
--                                 '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
--                                 '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
--                                 '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')
--            group by device_number
--          ) t1
--       on t.device_number=t1.device_number;



drop table if exists tmp_user_app_yy_d;
create table tmp_user_app_yy_d as
select t.*,
       t1.device_number device_number1
    from
        user_action_app_d t
     left join
        (select device_number
         from zba_dwa.dwa_v_m_cus_nm_user_info b
         where b.month_id='201907'
         and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')
           group by device_number
         ) t1
      on t.device_number=t1.device_number;


-- 月表加工
drop table if exists tmp_user_app_yy_m;
create table tmp_user_app_yy_m as
select substr(pt_days,0,6) pt_days,
       device_number,
       device_number1,
       appname,
       count(distinct pt_days) active_days,
       sum(pv) pv
    from  tmp_user_app_yy_d
   where pt_days>='20190301' and pt_days<='20190831'
     group by
       substr(pt_days,0,6),
       device_number,
       device_number1,
       appname;


-- 新增用户加工
drop table if exists tmp_user_app_yy_new;
create table tmp_user_app_yy_new as
select * from
          (select pt_days,
                  appname,
                  device_number,
                  device_number1,
                  pv,
                  row_number() over(partition by appname,device_number order by pt_days asc) rn  from  tmp_user_app_yy_d where pt_days >= '20190301')  t
where t.rn=1;

-- 流失用户加工
drop table if exists  tmp_user_app_yy_loss;
create table tmp_user_app_yy_loss AS
select t.*,
       t1.device_number1
  from user_action_app_loss_d t
 left JOIN
    (select device_number1 from tmp_user_app_yy_m group by device_number1) t1
  on t.device_number=t1.device_number1;


select '-- -- -*******************************************************************************************************************-- -- -';

-- 各个APP的日活
select 'app日活' model,
       'pt_days' pt_days,
       'App' appname,
       'uv' uv,
       'uv-剔除流量卡' uv1;
select 'app日活' model,
        pt_days,
        appname,
        count(distinct device_number) uv,
        count(distinct device_number1) uv1
    from tmp_user_app_yy_d
    where pt_days>='20190801' and pt_days<='20190831'
    group by pt_days,appname;

select '-- -- -*******************************************************************************************************************-- -- -';
-- 行业日活
select '行业日活' model,
       'pt_days' pt_days,
       '新闻行业uv' uv1,
       '新闻行业uv-剔除流量卡' uv2,
       '音乐行业uv' uv3,
       '音乐行业uv-剔除流量卡' uv4;
select  '行业日活' model,
        pt_days,
        count(distinct(case when appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') then device_number else null end)) uv1,
        count(distinct(case when appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') then device_number1 else null end)) uv2,
        count(distinct(case when appname in ('qq_music','kuwo','kugou','wangyi_music') then device_number else null end)) uv3,
        count(distinct(case when appname in ('qq_music','kuwo','kugou','wangyi_music') then device_number1 else null end)) uv4
      from  tmp_user_app_yy_d
    where pt_days>='20190801' and pt_days<='20190831'
    group by pt_days;


select '-- -- -*******************************************************************************************************************-- -- -';

-- 各个APP的月活
select '月份' month_id,
       'app月活' model,
       'App' appname,
       'uv' uv,
       'uv-剔除流量卡' uv1;
select '201908' month_id,
       'app月活' model,
        appname,
        count(distinct device_number) uv,
        count(distinct device_number1) uv1
    from tmp_user_app_yy_m
    where pt_days='201908'
    group by appname;

select '-- -- -*******************************************************************************************************************-- -- -';


-- 行业月活
select '月份' month_id,
       '行业月活' model,
       '新闻行业uv' uv1,
       '新闻行业uv-剔除流量卡' uv2,
       '音乐行业uv' uv3,
       '音乐行业uv-剔除流量卡' uv4;
select '201908' month_id,
       '行业月活' model,
        count(distinct(case when appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') then device_number else null end)) uv1,
        count(distinct(case when appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') then device_number1 else null end)) uv2,
        count(distinct(case when appname in ('qq_music','kuwo','kugou','wangyi_music') then device_number else null end)) uv3,
        count(distinct(case when appname in ('qq_music','kuwo','kugou','wangyi_music') then device_number1 else null end)) uv4
      from  tmp_user_app_yy_m
     where pt_days='201908';

select '-- -- -*******************************************************************************************************************-- -- -';

-- 各家新闻独占
select '201908' month_id,
       '各家新闻独占' model,
       top1_app,
       count(*)
    from
      (select device_number,
            count(distinct appname) cnt,
            max(case when rn=1 then appname else null end) top1_app
        from
       (select  a.*,
                row_number() over(partition by device_number order by pv desc) rn
          from
              (select appname,
                      device_number,
                      pv
                 from  tmp_user_app_yy_m
                 where pt_days = '201908'
                   and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')
                  ) a) a
      group by device_number) b
  where cnt=1
  group by top1_app;


select '-- -- -*******************************************************************************************************************-- -- -';

-- 新闻两两重合
select  '201908' month_id,
        '新闻两两重合' model,
        'qq&&toutiao' qq_toutiao,
        'qq&&wangyi' qq_wangyi,
        'qq&&kuaibao' qq_kuaibao,
        'qq&&sina' qq_sina,
        'qq&&ifeng' qq_ifeng,
        'qq&&sohu' qq_sohu,
        'toutiao&&wangyi' toutiao_wangyi,
        'toutiao&&kuaibao' toutiao_kuaibao,
        'toutiao&&sina' toutiao_sina,
        'toutiao&&ifeng' toutiao_ifeng,
        'toutiao&&sohu' toutiao_sohu,
        'wangyi&&kuaibao' wangyi_kuaibao,
        'wangyi&&sina' wangyi_sina,
        'wangyi&&ifeng' wangyi_ifeng,
        'wangyi&&sohu' wangyi_sohu,
        'kuaibao&&sina' kuaibao_sina,
        'kuaibao&&ifeng' kuaibao_ifeng,
        'kuaibao&&sohu' kuaibao_sohu,
        'sina&&ifeng' sina_ifeng,
        'sina&&sohu' sina_sohu,
        'ifeng&&sohu' ifeng_sohu
union  all
select
      '201908' month_id,
      '新闻两两重合' model,
      sum(case  when qq_flg='1' and tt_flg='1' then 1 else 0 end) qq_toutiao ,
      sum(case  when qq_flg='1' and wy_flg='1' then 1 else 0 end) qq_wangyi ,
      sum(case  when qq_flg='1' and kb_flg='1' then 1 else 0 end) qq_kuaibao ,
      sum(case  when qq_flg='1' and sina_flg='1' then 1 else 0 end) qq_sina ,
      sum(case  when qq_flg='1' and if_flg='1' then 1 else 0 end) qq_ifeng ,
      sum(case  when qq_flg='1' and sh_flg='1' then 1 else 0 end) qq_sohu ,
      sum(case  when tt_flg='1' and wy_flg='1' then 1 else 0 end) toutiao_wangyi ,
      sum(case  when tt_flg='1' and kb_flg='1' then 1 else 0 end) toutiao_kuaibao ,
      sum(case  when tt_flg='1' and sina_flg='1' then 1 else 0 end) toutiao_sina ,
      sum(case  when tt_flg='1' and if_flg='1' then 1 else 0 end) toutiao_ifeng ,
      sum(case  when tt_flg='1' and sh_flg='1' then 1 else 0 end) toutiao_sohu ,
      sum(case  when wy_flg='1' and kb_flg='1' then 1 else 0 end) wangyi_kuaibao ,
      sum(case  when wy_flg='1' and sina_flg='1' then 1 else 0 end) wangyi_sina ,
      sum(case  when wy_flg='1' and if_flg='1' then 1 else 0 end) wangyi_ifeng ,
      sum(case  when wy_flg='1' and sh_flg='1' then 1 else 0 end) wangyi_sohu ,
      sum(case  when kb_flg='1' and sina_flg='1' then 1 else 0 end) kuaibao_sina ,
      sum(case  when kb_flg='1' and if_flg='1' then 1 else 0 end) kuaibao_ifeng ,
      sum(case  when kb_flg='1' and sh_flg='1' then 1 else 0 end) kuaibao_sohu ,
      sum(case  when sina_flg='1' and if_flg='1' then 1 else 0 end) sina_ifeng ,
      sum(case  when sina_flg='1' and sh_flg='1' then 1 else 0 end) sina_sohu ,
      sum(case  when if_flg='1' and sh_flg='1' then 1 else 0 end) ifeng_sohu
  from
      (select device_number,
              count(distinct appname) cnt,
              max(case when appname='tencent_news' then 1 else 0 end) qq_flg,
              max(case when appname='kuaibao' then 1 else 0 end) kb_flg,
              max(case when appname='wangyinews' then 1 else 0 end) wy_flg,
              max(case when appname='toutiao' then 1 else 0 end) tt_flg,
              max(case when appname='sina_news' then 1 else 0 end) sina_flg,
              max(case when appname='ifeng' then 1 else 0 end) if_flg,
              max(case when appname='sohu_news' then 1 else 0 end) sh_flg
          from
          (select     appname,
                      device_number,
                      pv
                 from  tmp_user_app_yy_m
                 where pt_days = '201908'
                  and  appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')
                  ) a
        group by device_number) b
where cnt>=2;

select '-- -- -*******************************************************************************************************************-- -- -';

-- 新闻重合用户偏好
select '201908' month_id,
       '新闻APP重合用户偏好' model,
        top1_app,
        count(*) uv
from
    (select device_number,
          count(distinct appname) cnt,
          max(case when rn=1 then appname end) top1_app
        from
        (select a.*,
                row_number() over(partition by device_number order by pv desc) rn
          from
              (select appname,
                      device_number,
                      pv
                 from  tmp_user_app_yy_m
                 where pt_days = '201908'
                   and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')
                  ) a) b
    group by  device_number) c
    where cnt>=2
group by top1_app;


select '-- -- -*******************************************************************************************************************-- -- -';

select '月份' month_id,
       '各家app永久新增' model,
       'App' appname,
       '日期' pt_days,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select '201908' month_id,
      '各家app永久新增' model,
       appname,
       pt_days,
       count(distinct device_number) uv,
       count(distinct device_number1) uv1
 from
      tmp_user_app_yy_new
 where  pt_days>='20190801'
group by pt_days,
         appname
order by appname,
         pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

select '月份' month_id,
       '各app流失用户' model,
       'App' appname,
       '日期' pt_days,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select    '201908' month_id,
          '各app流失用户' model,
           a.appname,
           a.pt_days,
           count(distinct a.device_number) uv,
           count(distinct a.device_number1) uv1
      from  tmp_user_app_yy_loss a
      where pt_days>='20190801' and pt_days<='20190831'
      group by pt_days,
              appname;

select '-- -- -*******************************************************************************************************************-- -- -';

select '月份' month_id,
       '各家app留存' model,
       'App' appname,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select    '201908' month_id,
          '各家app留存' model,
          a.appname,
          count(distinct a.device_number) uv,
          count(distinct a.device_number1) uv1
      from  tmp_user_app_yy_m a
      where a.pt_days='201907'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')
        and exists
          (select * from
              (select
                      device_number,
                      appname
                  from  tmp_user_app_yy_m
                  where pt_days='201908'
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')
                  ) a1
              where a.appname=a1.appname and a.device_number=a1.device_number)
        group by a.appname;

select '-- -- -*******************************************************************************************************************-- -- -';

select '月份' month_id,
       '新闻各app新增来源' model,
       '统计App' tarapp,
       '来源App' srcapp,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select '201908' month_id,
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
            tmp_user_app_yy_new
          where pt_days>='20190801' and pt_days<='20190831'
            and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')) b
left join
      (select
                device_number,
                pt_days,
                appname
            from  tmp_user_app_yy_m
            where pt_days >= '20190301'
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')
            )  b1
on  b.device_number=b1.device_number) c
where c.diff>0 and c.diff<=90
group by tarapp,
         srcapp;


select '201908' month_id,
       '新闻各app自然新增' model,
        tarapp,
        count(distinct device_number) uv,
        count(distinct device_number1) uv1
from
  (select
          device_number,
          device_number1,
          tarapp,
          max(case when c.diff>1 and c.diff<=88 then 1 else 0 end) flg
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
              tmp_user_app_yy_new
            where pt_days>='20190801' and pt_days<='20190831'
              and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')) b
  left join
        (select
                  device_number,
                  pt_days,
                  appname
              from  tmp_user_app_yy_d
              where pt_days >= '20190301'
              and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')
              )  b1
  on  b.device_number=b1.device_number) c
  group  by
              device_number,
              device_number1,
              tarapp) d
where flg=0
group by tarapp;

select '-- -- -*******************************************************************************************************************-- -- -';

select
       '各app201907新增-201908留存' model,
       'App' appname,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select
       '各app201907新增-201908留存' model,
       b.appname,
       count(distinct b.device_number) uv,
       count(distinct b.device_number1) uv1
 from
(select device_number,
        device_number1,
        appname
     from
       tmp_user_app_yy_new
    where pt_days>='20190701' and pt_days<='20190731'
  group by device_number,
           device_number1,
           appname) b
  where exists
    (select *
         from
       (select
                device_number,
                appname
            from  tmp_user_app_yy_m
            where pt_days = '201908'
            )  b1
        where  b.device_number=b1.device_number
          and  b.appname=b1.appname)
group by b.appname;

select '-- -- -*******************************************************************************************************************-- -- -';

select
       '各app201906新增-201907,08留存' model,
       'App' appname,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select
       '各app201906新增-201907,08留存' model,
       b.appname,
       count(distinct b.device_number) uv,
       count(distinct b.device_number1) uv1
 from
(select device_number,
        device_number1,
        appname
     from
       tmp_user_app_yy_new
    where pt_days>='20190601' and pt_days<='20190631'
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
            from  tmp_user_app_yy_m
            where pt_days in ('201907','201908')
          group by device_number,
                   appname
            )  b1
        where  b.device_number=b1.device_number
          and  b.appname=b1.appname
          and  b1.am=2)
group by b.appname;

select '-- -- -*******************************************************************************************************************-- -- -';

select '201908' month_id,
       '新闻各app流失去向' model,
       '统计App' tarapp,
       '流向App' srcapp,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select '201908' month_id,
      '新闻各app流失去向' model,
       srcapp,
       tarapp,
       count(distinct device_number) uv,
       count(distinct device_number1) uv1
 from
  (select   b.pt_days,
            b.device_number,
            b.appname srcapp,
            b1.appname tarapp,
            b1.device_number1 device_number1,
            datediff(concat_ws('-',substr(b.pt_days,1,4),substr(b.pt_days,5,2),substr(b.pt_days,7,2)),concat_ws('-',substr(b1.pt_days,1,4),substr(b1.pt_days,5,2),substr(b1.pt_days,7,2))) diff
      from
      (select
            a.pt_days,
            a.device_number,
            a.appname
        from  tmp_user_app_yy_loss a
        where a.pt_days>='20190801' and a.pt_days<='20190831'
          and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')) b
  left join
          (select
                  pt_days,
                  device_number,
                  device_number1,
                  appname
              from  tmp_user_app_yy_d
              where appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng'))  b1
   on  b.device_number=b1.device_number
  ) c
where c.diff>=0 and c.diff<89
group by tarapp,
         srcapp;


 select '201908' month_id,
       '新闻各app自然流失' model,
        srcapp,
        count(distinct device_number) uv,
        count(distinct device_number1) uv1
 from
     (select
             device_number,
             srcapp,
             device_number1,
             max(case when c.diff>=1 and c.diff<88 then 1 else 0 end) flg
     from
     (select     b.pt_days,
                 b.device_number,
                 b.appname srcapp,
                 b1.appname tarapp,
                 b1.device_number1 device_number1,
                 datediff(concat_ws('-',substr(b.pt_days,1,4),substr(b.pt_days,5,2),substr(b.pt_days,7,2)),concat_ws('-',substr(b1.pt_days,1,4),substr(b1.pt_days,5,2),substr(b1.pt_days,7,2))) diff
         from
         (select
                 a.pt_days,
                 a.device_number,
                 a.appname
             from  tmp_user_app_yy_loss a
             where a.pt_days>='20190801' and a.pt_days<='20190831'
             and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')) b
     left join
             (select
                     pt_days,
                     device_number,
                     device_number1,
                     appname
                 from  tmp_user_app_yy_d
                 where appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng'))  b1
     on  b.device_number=b1.device_number
     ) c
     group by
             device_number,
             srcapp,
             device_number1) d
 where flg=0
 group by srcapp;


select '-- -- -*******************************************************************************************************************-- -- -';

-- -各家音乐APP独占
select '201908' month_id,
       '各家音乐APP独占' model,
       top1_app,
       count(*)
    from
      (select device_number,
            count(distinct appname) cnt,
            max(case when rn=1 then appname else null end) top1_app
        from
        (select a.*,
                row_number() over(partition by device_number order by pv desc) rn
            from
            (select appname,
                      device_number,
                      pv
                  from  tmp_user_app_yy_m
                where  pt_days='201908'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')
                  ) a ) a
      group by device_number) b
  where cnt=1
  group by top1_app;


select '-- -- -*******************************************************************************************************************-- -- -';
-- 音乐APP两两重合
select  '201908' month_id,
        '音乐APP两两重合' model,
        'qq&&kuwo' qq_kuwo ,
        'qq&&wangyi' qq_wangyi ,
        'qq&&kugou' qq_kugou ,
        'kuwo&&wangyi' kuwo_wangyi ,
        'kuwo&&kugou' kuwo_kugou ,
        'wangyi&&kugou' wangyi_kugou;
select '201908' month_id,
      '音乐APP两两重合' model,
      sum(case  when qq_flg='1' and kw_flg='1' then 1 else 0 end) qq_kuwo ,
      sum(case  when qq_flg='1' and wy_flg='1' then 1 else 0 end) qq_wangyi ,
      sum(case  when qq_flg='1' and kg_flg='1' then 1 else 0 end) qq_kugou ,
      sum(case  when kw_flg='1' and wy_flg='1' then 1 else 0 end) kuwo_wangyi ,
      sum(case  when kw_flg='1' and kg_flg='1' then 1 else 0 end) kuwo_kugou ,
      sum(case  when wy_flg='1' and kg_flg='1' then 1 else 0 end) wangyi_kugou
  from
      (select device_number,
              count(distinct appname) cnt,
              max(case when appname='qq_music' then 1 else 0 end) qq_flg,
              max(case when appname='kuwo' then 1 else 0 end) kw_flg,
              max(case when appname='wangyi_music' then 1 else 0 end) wy_flg,
              max(case when appname='kugou' then 1 else 0 end) kg_flg
          from
            (select   appname,
                      device_number,
                      pv
                  from  tmp_user_app_yy_m
                where  pt_days='201908'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')
                  ) a
        group by device_number) b
where cnt>=2;

select '-- -- -*******************************************************************************************************************-- -- -';
-- 音乐重合用户偏好
select '201908' month_id,
       '音乐APP重合用户偏好' model,
        top1_app,
        count(*) uv
from
    (select device_number,
          count(distinct appname) cnt,
          max(case when rn=1 then appname end) top1_app
        from
        (select a.*,
                row_number() over(partition by device_number order by pv desc) rn
          from
              (select appname,
                      device_number,
                      pv
                 from  tmp_user_app_yy_m
                 where pt_days = '201908'
                   and appname in ('qq_music','kuwo','kugou','wangyi_music')
                  ) a) b
    group by  device_number) c
    where cnt>=2
group by top1_app;



select '-- -- -*******************************************************************************************************************-- -- -';


select '月份' month_id,
       '音乐各app新增来源' model,
       '统计App' tarapp,
       '来源App' srcapp,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select '201908' month_id,
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
            tmp_user_app_yy_new
          where pt_days>='20190801' and pt_days<='20190831'
            and appname in ('qq_music','kuwo','kugou','wangyi_music')) b
left join
      (select
                device_number,
                pt_days,
                appname
            from  tmp_user_app_yy_d
            where pt_days >= '20190301'
             and appname in ('qq_music','kuwo','kugou','wangyi_music')
            )  b1
on  b.device_number=b1.device_number) c
where c.diff>0 and c.diff<=90
group by tarapp,
         srcapp;

select '201908' month_id,
       '音乐各app自然新增' model,
        tarapp,
        count(distinct device_number) uv,
        count(distinct device_number1) uv1
from
(select
        device_number,
        device_number1,
        tarapp,
        max(case when c.diff>1 and c.diff<=88 then 1 else 0 end) flg
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
            tmp_user_app_yy_new
          where pt_days>='20190801' and pt_days<='20190831'
            and appname in ('qq_music','kuwo','kugou','wangyi_music')) b
left join
      (select
                device_number,
                pt_days,
                appname
            from  tmp_user_app_yy_d
            where pt_days >= '20190301'
             and appname in ('qq_music','kuwo','kugou','wangyi_music')
            )  b1
on  b.device_number=b1.device_number) c
group  by
            device_number,
            device_number1,
            tarapp) d
where flg=0
group by tarapp;

select '-- -- -*******************************************************************************************************************-- -- -';

select '201908' month_id,
       '音乐各app流失去向' model,
       '统计App' tarapp,
       '流向App' srcapp,
       'uv' uv,
       'uv_剔除流量卡' uv1;
select '201908' month_id,
      '音乐各app流失去向' model,
       srcapp,
       tarapp,
       count(distinct device_number) uv,
       count(distinct device_number1) uv1
 from
  (select   b.pt_days,
            b.device_number,
            b.appname srcapp,
            b1.appname tarapp,
            b1.device_number1 device_number1,
            datediff(concat_ws('-',substr(b.pt_days,1,4),substr(b.pt_days,5,2),substr(b.pt_days,7,2)),concat_ws('-',substr(b1.pt_days,1,4),substr(b1.pt_days,5,2),substr(b1.pt_days,7,2))) diff
      from
      (select
            a.pt_days,
            a.device_number,
            a.appname
        from  tmp_user_app_yy_loss a
        where a.pt_days>='20190801' and a.pt_days<='20190831'
          and a.appname in ('qq_music','kuwo','kugou','wangyi_music')) b
  left join
          (select
                  pt_days,
                  device_number,
                  device_number1,
                  appname
              from  tmp_user_app_yy_d
              where appname in ('qq_music','kuwo','kugou','wangyi_music'))  b1
   on  b.device_number=b1.device_number
  ) c
where c.diff>=0 and c.diff<89
group by tarapp,
         srcapp;


 select '201908' month_id,
       '音乐各app自然流失' model,
        srcapp,
        count(distinct device_number) uv,
        count(distinct device_number1) uv1
 from
     (select
             device_number,
             srcapp,
             device_number1,
             max(case when c.diff>=1 and c.diff<88 then 1 else 0 end) flg
     from
     (select     b.pt_days,
                 b.device_number,
                 b.appname srcapp,
                 b1.appname tarapp,
                 b1.device_number1 device_number1,
                 datediff(concat_ws('-',substr(b.pt_days,1,4),substr(b.pt_days,5,2),substr(b.pt_days,7,2)),concat_ws('-',substr(b1.pt_days,1,4),substr(b1.pt_days,5,2),substr(b1.pt_days,7,2))) diff
         from
         (select
                 a.pt_days,
                 a.device_number,
                 a.appname
             from  tmp_user_app_yy_loss a
             where a.pt_days>='20190801' and a.pt_days<='20190831'
             and a.appname in ('qq_music','kuwo','kugou','wangyi_music')) b
     left join
             (select
                     pt_days,
                     device_number,
                     device_number1,
                     appname
                 from  tmp_user_app_yy_d
                 where appname in ('qq_music','kuwo','kugou','wangyi_music'))  b1
     on  b.device_number=b1.device_number
     ) c
     group by
             device_number,
             srcapp,
             device_number1) d
 where flg=0
 group by srcapp;

select '-- -- -*******************************************************************************************************************-- -- -';


select  '忠诚用户统计' model,
        '201908' month_id,
        'app' appname,
        '活跃2天及以上用户量' cnt2,
        '活跃3天及以上用户量' cnt3,
        '活跃4天及以上用户量' cnt4,
        '活跃5天及以上用户量' cnt5
;
select '忠诚用户统计' model,
      '201908' month_id,
       appname,
       count(distinct(case when cast(ad_m1 as int)>=2 and cast(ad_m2 as int)>=2 then device_number else null end)) cnt2,
       count(distinct(case when cast(ad_m1 as int)>=3 and cast(ad_m2 as int)>=3 then device_number else null end)) cnt3,
       count(distinct(case when cast(ad_m1 as int)>=4 and cast(ad_m2 as int)>=4 then device_number else null end)) cnt4,
       count(distinct(case when cast(ad_m1 as int)>=5 and cast(ad_m2 as int)>=5 then device_number else null end)) cnt5
  from
  (select
        appname,
        device_number,
        max(case when pt_days='201907' then active_days else 0 end) ad_m1,
        max(case when pt_days='201908' then active_days else 0 end) ad_m2
      from tmp_user_app_yy_m
      where  pt_days in ('201907','201908')
      group by appname,
              device_number) a
  group by appname;



select '-- -- -*******************************************************************************************************************-- -- -';



select '月份' month_id,
       '新闻行业流失分析' model,
       'uv_行业内竞品流失' uv0,
       'uv_行业流失' uv1,
       'uv_流向腾讯新闻' uv2,
       'uv_流向头条' uv3,
       'uv_流向快报' uv4,
       'uv_流向网易新闻' uv5,
       'uv_流向新浪新闻' uv6,
       'uv_流向搜狐新闻' uv7,
       'uv_流向凤凰新闻' uv8,
       'uv_qq->^tt' uv9,
       'uv_^qq->tt' uv10,
       'uv_tt->^qq' uv11,
       'uv_^tt->qq' uv12,
       'uv_wangyi->^sina' uv13,
       'uv_^wangyi->sina' uv14,
       'uv_sina->^wangyi' uv15,
       'uv_^sina->wangyi' uv16,
       'uv_wangyi->^sohu' uv17,
       'uv_^wangyi->sohu' uv18,
       'uv_sohu->^wangyi' uv19,
       'uv_^sohu->wangyi' uv20
;
select '201908' month_id,
       '新闻行业流失分析' model,
        count(distinct(case when tarapp is not null then device_number else null end)) uv0,
        count(distinct(case when tarapp is null then device_number else null end)) uv1,
        count(distinct(case when tarapp='tencent_news' then device_number1 else null end)) uv2,
        count(distinct(case when tarapp='toutiao' then device_number1 else null end)) uv3,
        count(distinct(case when tarapp='kuaibao' then device_number1 else null end)) uv4,
        count(distinct(case when tarapp='wangyinews' then device_number1 else null end)) uv5,
        count(distinct(case when tarapp='sina_news' then device_number1 else null end)) uv6,
        count(distinct(case when tarapp='sohu_news' then device_number1 else null end)) uv7,
        count(distinct(case when tarapp='ifeng' then device_number1 else null end)) uv8,
        count(distinct(case when srcapp='tencent_news' and tarapp<>'toutiao' then device_number1 else null end)) uv9,
        count(distinct(case when srcapp<>'tencent_news' and tarapp='toutiao' then device_number1 else null end)) uv10,
        count(distinct(case when srcapp='toutiao' and tarapp<>'tencent_news' then device_number1 else null end)) uv11,
        count(distinct(case when srcapp<>'toutiao' and tarapp='tencent_news' then device_number1 else null end)) uv12,
        count(distinct(case when srcapp='wangyinews' and tarapp<>'sina_news' then device_number1 else null end)) uv13,
        count(distinct(case when srcapp<>'wangyinews' and tarapp='sina_news' then device_number1 else null end)) uv14,
        count(distinct(case when srcapp='sina_news' and tarapp<>'wangyinews' then device_number1 else null end)) uv15,
        count(distinct(case when srcapp<>'sina_news' and tarapp='wangyinews' then device_number1 else null end)) uv16,
        count(distinct(case when srcapp='wangyinews' and tarapp<>'sohu_news' then device_number1 else null end)) uv17,
        count(distinct(case when srcapp<>'wangyinews' and tarapp='sohu_news' then device_number1 else null end)) uv18,
        count(distinct(case when srcapp='sohu_news' and tarapp<>'wangyinews' then device_number1 else null end)) uv19,
        count(distinct(case when srcapp<>'sohu_news' and tarapp='wangyinews' then device_number1 else null end)) uv20
from
  (select   b.pt_days,
            b.device_number,
            b.appname srcapp,
            b1.appname tarapp,
            b1.device_number device_number1,
            datediff(concat_ws('-',substr(b.pt_days,1,4),substr(b.pt_days,5,2),substr(b.pt_days,7,2)),concat_ws('-',substr(b1.pt_days,1,4),substr(b1.pt_days,5,2),substr(b1.pt_days,7,2))) diff
      from
      (select
            a.pt_days,
            a.device_number,
            a.appname
        from  tmp_user_app_yy_loss a
        where a.pt_days>='20190801' and a.pt_days<='20190831'
          and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')) b
 left join
          (select
                  pt_days,
                  device_number,
                  appname
              from  tmp_user_app_yy_d
              where appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng'))  b1
   on  b.device_number=b1.device_number
  ) c
where c.diff>=0 and c.diff<90;






-- -Top200 App
select '手百TOP200APP' model,
       t.prod_name,
       count(distinct t.device_number) uv,
       sum(visit_cnt) pv,
       sum(visit_dura) dur
from
    zb_dwa.dwa_m_ia_basic_user_app t,
    (select device_number from  user_action_app_d where pt_days>='20190801' and pt_days<='20190831' and appname='mobbaidu' group by device_number) t1
 where t.device_number=t1.device_number
   and t.month_id='201908'
group by t.prod_name;

select '-- -- -*******************************************************************************************************************-- -- -';


-- -- 联通全量Top200App
select '联通全量Top200App' model,
       t.prod_name,
       count(distinct t.device_number) uv,
       sum(visit_cnt) pv,
       sum(visit_dura) dur
from
    zb_dwa.dwa_m_ia_basic_user_app t
 where t.month_id='201908'
 group by t.prod_name
 ;




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
                    where pt_days>='20190701' and pt_days<='20190831'
                    and instr(url_host,'ximalaya.com')>0
                    group BY
                        substr(pt_days,0,6),
                        device_number) a) b
        left join
            (select * from tmp_user_app_yy_m  where appname='ximalaya' and pt_days>='201907') b1
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




drop table if exists tmp_user_app_yy_m;
drop table if exists tmp_user_app_yy_d;
drop table if exists tmp_user_app_yy_new;
drop table if exists tmp_user_app_yy_loss;
drop table if exists tmp_xima_model_t1;
