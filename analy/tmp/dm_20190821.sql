-- 天表加工
drop table if exists tmp_user_app_yy_d;
create table tmp_user_app_yy_d as
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
      where pt_days>='20190601' and pt_days<='20190731'
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
      where pt_days>='20190601' and pt_days<='20190731'
      and url_host in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com','interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com')
        group by
            pt_days,
            device_number,
            case when url_host  in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com') then 'qq_music'
                  when url_host in ('interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com') then 'wangyi_music'
                  else null
            end)  t
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
      on t.device_number=t1.device_number;



-- 流失用户加工
drop table if exists  tmp_user_app_yy_loss;
create table tmp_user_app_yy_loss AS
select t.*,
       t1.device_number1
  from user_action_app_loss_d t
 left JOIN
    (select device_number1 from tmp_user_app_yy_m group by device_number1) t1
  on t.device_number=t1.device_number1;


select '201907' month_id,
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
            where a.pt_days>='20190701' and a.pt_days<='20190731'
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



select '201907' month_id,
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
            where a.pt_days>='20190701' and a.pt_days<='20190731'
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
select '201907' month_id,
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
        where a.pt_days>='20190701' and a.pt_days<='20190731'
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
