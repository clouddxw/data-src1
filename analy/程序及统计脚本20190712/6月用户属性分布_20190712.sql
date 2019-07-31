

drop table if exists tmp_user_app_profile_t1;
create table tmp_user_app_profile_t1 as
select t.*,
       t1.device_number device_number1
   from 
    (select  device_number,
            max(case when appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') then 1 else 0 end) tag1,
            max(case when appname = 'tencent_news' then 1 else 0 end) tag2,
            max(case when appname = 'toutiao' then 1 else 0 end) tag3,
            max(case when appname = 'kuaibao' then 1 else 0 end) tag4,
            max(case when appname = 'wangyinews' then 1 else 0 end) tag5,
            max(case when appname = 'sina_news' then 1 else 0 end) tag6,
            max(case when appname = 'sohu_news' then 1 else 0 end) tag7,
            max(case when appname = 'ifeng' then 1 else 0 end) tag8,
            max(case when appname in ('qq_music','kuwo','kugou','wangyi_music') then 1 else 0 end) tag9, 
            max(case when appname = 'qq_music' then 1 else 0 end) tag10, 
            max(case when appname = 'kuwo' then 1 else 0 end) tag11,
            max(case when appname = 'kugou' then 1 else 0 end) tag12,
            max(case when appname = 'wangyi_music' then 1 else 0 end) tag13,
            max(case when appname = 'uc' then 1 else 0 end) tag14,
            max(case when appname = 'mobbaidu' then 1 else 0 end) tag15,
               max(case when appname = 'ximalaya' then 1 else 0 end) tag16
    from (select '201906' pt_days,
                 appname,
                 device_number,
                 count(distinct pt_days) active_days,
                 sum(pv) pv
         from  user_action_app_d
         where pt_days>='20190601' and pt_days<='20190631'
         group by appname,
                  device_number) a  where pt_days='201906' 
    group by device_number) t
left join 
    (select device_number 
        from zba_dwa.dwa_v_m_cus_nm_user_info b  
        where b.month_id='201905'
        and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                            '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                            '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                            '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')
        group by device_number) t1
on t.device_number=t1.device_number;


--  drop table if exists tmp_user_app_profile_t3;
--  create table tmp_user_app_profile_t3 as
--  select device_number
--   from 
--      (select 
--          pt_days,
--          hh,
--          device_number,  
--          max(case when url_host='bdbus-turbonet.baidu.com' then 1 else 0 end) tag1,
--          max(case when url_host='hmma.baidu.com' then 1 else 0 end) tag2,
--          max(case when url_host='mbd.baidu.com' then 1 else 0 end) tag3
--      from user_action_tag_d
--      where pt_days>='20190601' and pt_days<='20190631'
--      and  url_host in ('bdbus-turbonet.baidu.com','hmma.baidu.com','mbd.baidu.com')
--      group by pt_days,
--              hh,
--              device_number)  a
--  where tag1=1 and tag2=1 and tag3=1 
--  group by device_number;


drop table if exists tmp_user_app_profile_t4;
create table tmp_user_app_profile_t4 as
select * from 
    (select t.*,
        row_number() over(partition by device_number order by age_range desc) rn
    from user_info_sample t  where pt_days='201905') a
where rn=1;



select  '各app' app,
        '常驻省份分布' model,
        '省份' top1_home_province,
        'uv_新闻行业' uv1,
        'uv_腾讯新闻' uv2,
        'uv_头条' uv3,
        'uv_快报' uv4,
        'uv_网易新闻' uv5,
        'uv_新浪新闻' uv6,
        'uv_搜狐新闻' uv7,
        'uv_凤凰新闻' uv8,
        'uv_音乐行业' uv9,
        'uv_qq音乐' uv10,
        'uv_酷我' uv11,
        'uv_酷狗' uv12,
        'uv_网易云音乐' uv13,
        'uv_uc' uv14,
        'uv_手机百度' uv15,
          'uv_喜马拉雅' uv16,
        'uv_新闻行业-- 剔除流量卡' uv_1,
        'uv_腾讯新闻-- 剔除流量卡' uv_2,
        'uv_头条-- 剔除流量卡' uv_3,
        'uv_快报-- 剔除流量卡' uv_4,
        'uv_网易新闻-- 剔除流量卡' uv_5,
        'uv_新浪新闻-- 剔除流量卡' uv_6,
        'uv_搜狐新闻-- 剔除流量卡' uv_7,
        'uv_凤凰新闻-- 剔除流量卡' uv_8,
        'uv_音乐行业-- 剔除流量卡' uv_9,
        'uv_qq音乐-- 剔除流量卡' uv_10,
        'uv_酷我-- 剔除流量卡' uv_11,
        'uv_酷狗-- 剔除流量卡' uv_12,
        'uv_网易云音乐-- 剔除流量卡' uv_13,
        'uv_uc-- 剔除流量卡' uv_14,
        'uv_手机百度-- 剔除流量卡' uv_15,
          'uv_喜马拉雅-- 剔除流量卡' uv_16
union all
select  '各app' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        sum(case when tag1=1 then 1 else 0 end) uv1,
        sum(case when tag2=1 then 1 else 0 end) uv2,
        sum(case when tag3=1 then 1 else 0 end) uv3,
        sum(case when tag4=1 then 1 else 0 end) uv4,
        sum(case when tag5=1 then 1 else 0 end) uv5,
        sum(case when tag6=1 then 1 else 0 end) uv6,
        sum(case when tag7=1 then 1 else 0 end) uv7,
        sum(case when tag8=1 then 1 else 0 end) uv8,
        sum(case when tag9=1 then 1 else 0 end) uv9,
        sum(case when tag10=1 then 1 else 0 end) uv10,
        sum(case when tag11=1 then 1 else 0 end) uv11,
        sum(case when tag12=1 then 1 else 0 end) uv12,
        sum(case when tag13=1 then 1 else 0 end) uv13,
        sum(case when tag14=1 then 1 else 0 end) uv14,
        sum(case when tag15=1 then 1 else 0 end) uv15,
        sum(case when tag16=1 then 1 else 0 end) uv16,
        sum(case when tag1=1 and device_number1 is not null then  1 else 0 end) uv_1,
        sum(case when tag2=1 and device_number1 is not null then  1 else 0 end) uv_2,
        sum(case when tag3=1 and device_number1 is not null then  1 else 0 end) uv_3,
        sum(case when tag4=1 and device_number1 is not null then  1 else 0 end) uv_4,
        sum(case when tag5=1 and device_number1 is not null then  1 else 0 end) uv_5,
        sum(case when tag6=1 and device_number1 is not null then  1 else 0 end) uv_6,
        sum(case when tag7=1 and device_number1 is not null then  1 else 0 end) uv_7,
        sum(case when tag8=1 and device_number1 is not null then  1 else 0 end) uv_8,
        sum(case when tag9=1 and device_number1 is not null then  1 else 0 end) uv_9,
        sum(case when tag10=1 and device_number1 is not null then  1 else 0 end) uv_10,
        sum(case when tag11=1 and device_number1 is not null then  1 else 0 end) uv_11,
        sum(case when tag12=1 and device_number1 is not null then  1 else 0 end) uv_12,
        sum(case when tag13=1 and device_number1 is not null then  1 else 0 end) uv_13,
        sum(case when tag14=1 and device_number1 is not null then  1 else 0 end) uv_14,
        sum(case when tag15=1 and device_number1 is not null then  1 else 0 end) uv_15,
        sum(case when tag16=1 and device_number1 is not null then  1 else 0 end) uv_16
    from 
        tmp_user_app_profile_t1 t
     left join 
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '各app' app,
        '常驻城市分布' model,
        '城市' top1_home_city,
        'uv_新闻行业' uv1,
        'uv_腾讯新闻' uv2,
        'uv_头条' uv3,
        'uv_快报' uv4,
        'uv_网易新闻' uv5,
        'uv_新浪新闻' uv6,
        'uv_搜狐新闻' uv7,
        'uv_凤凰新闻' uv8,
        'uv_音乐行业' uv9,
        'uv_qq音乐' uv10,
        'uv_酷我' uv11,
        'uv_酷狗' uv12,
        'uv_网易云音乐' uv13,
        'uv_uc' uv14,
        'uv_手机百度' uv15,
          'uv_喜马拉雅' uv16,
        'uv_新闻行业-- 剔除流量卡' uv_1,
        'uv_腾讯新闻-- 剔除流量卡' uv_2,
        'uv_头条-- 剔除流量卡' uv_3,
        'uv_快报-- 剔除流量卡' uv_4,
        'uv_网易新闻-- 剔除流量卡' uv_5,
        'uv_新浪新闻-- 剔除流量卡' uv_6,
        'uv_搜狐新闻-- 剔除流量卡' uv_7,
        'uv_凤凰新闻-- 剔除流量卡' uv_8,
        'uv_音乐行业-- 剔除流量卡' uv_9,
        'uv_qq音乐-- 剔除流量卡' uv_10,
        'uv_酷我-- 剔除流量卡' uv_11,
        'uv_酷狗-- 剔除流量卡' uv_12,
        'uv_网易云音乐-- 剔除流量卡' uv_13,
        'uv_uc-- 剔除流量卡' uv_14,
        'uv_手机百度-- 剔除流量卡' uv_15,
          'uv_喜马拉雅-- 剔除流量卡' uv_16
union all
select  '各app' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        sum(case when tag1=1 then 1 else 0 end) uv1,
        sum(case when tag2=1 then 1 else 0 end) uv2,
        sum(case when tag3=1 then 1 else 0 end) uv3,
        sum(case when tag4=1 then 1 else 0 end) uv4,
        sum(case when tag5=1 then 1 else 0 end) uv5,
        sum(case when tag6=1 then 1 else 0 end) uv6,
        sum(case when tag7=1 then 1 else 0 end) uv7,
        sum(case when tag8=1 then 1 else 0 end) uv8,
        sum(case when tag9=1 then 1 else 0 end) uv9,
        sum(case when tag10=1 then 1 else 0 end) uv10,
        sum(case when tag11=1 then 1 else 0 end) uv11,
        sum(case when tag12=1 then 1 else 0 end) uv12,
        sum(case when tag13=1 then 1 else 0 end) uv13,
        sum(case when tag14=1 then 1 else 0 end) uv14,
        sum(case when tag15=1 then 1 else 0 end) uv15,
        sum(case when tag16=1 then 1 else 0 end) uv16,
        sum(case when tag1=1 and device_number1 is not null then  1 else 0 end) uv_1,
        sum(case when tag2=1 and device_number1 is not null then  1 else 0 end) uv_2,
        sum(case when tag3=1 and device_number1 is not null then  1 else 0 end) uv_3,
        sum(case when tag4=1 and device_number1 is not null then  1 else 0 end) uv_4,
        sum(case when tag5=1 and device_number1 is not null then  1 else 0 end) uv_5,
        sum(case when tag6=1 and device_number1 is not null then  1 else 0 end) uv_6,
        sum(case when tag7=1 and device_number1 is not null then  1 else 0 end) uv_7,
        sum(case when tag8=1 and device_number1 is not null then  1 else 0 end) uv_8,
        sum(case when tag9=1 and device_number1 is not null then  1 else 0 end) uv_9,
        sum(case when tag10=1 and device_number1 is not null then  1 else 0 end) uv_10,
        sum(case when tag11=1 and device_number1 is not null then  1 else 0 end) uv_11,
        sum(case when tag12=1 and device_number1 is not null then  1 else 0 end) uv_12,
        sum(case when tag13=1 and device_number1 is not null then  1 else 0 end) uv_13,
        sum(case when tag14=1 and device_number1 is not null then  1 else 0 end) uv_14,
        sum(case when tag15=1 and device_number1 is not null then  1 else 0 end) uv_15,
        sum(case when tag16=1 and device_number1 is not null then  1 else 0 end) uv_16
    from 
        tmp_user_app_profile_t1 t
     left join 
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '各app' app,
        '品牌分布' model,
        '品牌' factory_desc,
        'uv_新闻行业' uv1,
        'uv_腾讯新闻' uv2,
        'uv_头条' uv3,
        'uv_快报' uv4,
        'uv_网易新闻' uv5,
        'uv_新浪新闻' uv6,
        'uv_搜狐新闻' uv7,
        'uv_凤凰新闻' uv8,
        'uv_音乐行业' uv9,
        'uv_qq音乐' uv10,
        'uv_酷我' uv11,
        'uv_酷狗' uv12,
        'uv_网易云音乐' uv13,
        'uv_uc' uv14,
        'uv_手机百度' uv15,
          'uv_喜马拉雅' uv16,
        'uv_新闻行业-- 剔除流量卡' uv_1,
        'uv_腾讯新闻-- 剔除流量卡' uv_2,
        'uv_头条-- 剔除流量卡' uv_3,
        'uv_快报-- 剔除流量卡' uv_4,
        'uv_网易新闻-- 剔除流量卡' uv_5,
        'uv_新浪新闻-- 剔除流量卡' uv_6,
        'uv_搜狐新闻-- 剔除流量卡' uv_7,
        'uv_凤凰新闻-- 剔除流量卡' uv_8,
        'uv_音乐行业-- 剔除流量卡' uv_9,
        'uv_qq音乐-- 剔除流量卡' uv_10,
        'uv_酷我-- 剔除流量卡' uv_11,
        'uv_酷狗-- 剔除流量卡' uv_12,
        'uv_网易云音乐-- 剔除流量卡' uv_13,
        'uv_uc-- 剔除流量卡' uv_14,
        'uv_手机百度-- 剔除流量卡' uv_15,
          'uv_喜马拉雅-- 剔除流量卡' uv_16
union all
select  '各app' app,
        '品牌分布' model,
        t1.factory_desc,
        sum(case when tag1=1 then 1 else 0 end) uv1,
        sum(case when tag2=1 then 1 else 0 end) uv2,
        sum(case when tag3=1 then 1 else 0 end) uv3,
        sum(case when tag4=1 then 1 else 0 end) uv4,
        sum(case when tag5=1 then 1 else 0 end) uv5,
        sum(case when tag6=1 then 1 else 0 end) uv6,
        sum(case when tag7=1 then 1 else 0 end) uv7,
        sum(case when tag8=1 then 1 else 0 end) uv8,
        sum(case when tag9=1 then 1 else 0 end) uv9,
        sum(case when tag10=1 then 1 else 0 end) uv10,
        sum(case when tag11=1 then 1 else 0 end) uv11,
        sum(case when tag12=1 then 1 else 0 end) uv12,
        sum(case when tag13=1 then 1 else 0 end) uv13,
        sum(case when tag14=1 then 1 else 0 end) uv14,
        sum(case when tag15=1 then 1 else 0 end) uv15,
        sum(case when tag16=1 then 1 else 0 end) uv16,
        sum(case when tag1=1 and device_number1 is not null then  1 else 0 end) uv_1,
        sum(case when tag2=1 and device_number1 is not null then  1 else 0 end) uv_2,
        sum(case when tag3=1 and device_number1 is not null then  1 else 0 end) uv_3,
        sum(case when tag4=1 and device_number1 is not null then  1 else 0 end) uv_4,
        sum(case when tag5=1 and device_number1 is not null then  1 else 0 end) uv_5,
        sum(case when tag6=1 and device_number1 is not null then  1 else 0 end) uv_6,
        sum(case when tag7=1 and device_number1 is not null then  1 else 0 end) uv_7,
        sum(case when tag8=1 and device_number1 is not null then  1 else 0 end) uv_8,
        sum(case when tag9=1 and device_number1 is not null then  1 else 0 end) uv_9,
        sum(case when tag10=1 and device_number1 is not null then  1 else 0 end) uv_10,
        sum(case when tag11=1 and device_number1 is not null then  1 else 0 end) uv_11,
        sum(case when tag12=1 and device_number1 is not null then  1 else 0 end) uv_12,
        sum(case when tag13=1 and device_number1 is not null then  1 else 0 end) uv_13,
        sum(case when tag14=1 and device_number1 is not null then  1 else 0 end) uv_14,
        sum(case when tag15=1 and device_number1 is not null then  1 else 0 end) uv_15,
        sum(case when tag16=1 and device_number1 is not null then  1 else 0 end) uv_16
    from 
        tmp_user_app_profile_t1 t
     left join 
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '各app' app,
        '证件归属地分布' model,
        '归属地' cert_prov,
        'uv_新闻行业' uv1,
        'uv_腾讯新闻' uv2,
        'uv_头条' uv3,
        'uv_快报' uv4,
        'uv_网易新闻' uv5,
        'uv_新浪新闻' uv6,
        'uv_搜狐新闻' uv7,
        'uv_凤凰新闻' uv8,
        'uv_音乐行业' uv9,
        'uv_qq音乐' uv10,
        'uv_酷我' uv11,
        'uv_酷狗' uv12,
        'uv_网易云音乐' uv13,
        'uv_uc' uv14,
        'uv_手机百度' uv15,
          'uv_喜马拉雅' uv16,
        'uv_新闻行业-- 剔除流量卡' uv_1,
        'uv_腾讯新闻-- 剔除流量卡' uv_2,
        'uv_头条-- 剔除流量卡' uv_3,
        'uv_快报-- 剔除流量卡' uv_4,
        'uv_网易新闻-- 剔除流量卡' uv_5,
        'uv_新浪新闻-- 剔除流量卡' uv_6,
        'uv_搜狐新闻-- 剔除流量卡' uv_7,
        'uv_凤凰新闻-- 剔除流量卡' uv_8,
        'uv_音乐行业-- 剔除流量卡' uv_9,
        'uv_qq音乐-- 剔除流量卡' uv_10,
        'uv_酷我-- 剔除流量卡' uv_11,
        'uv_酷狗-- 剔除流量卡' uv_12,
        'uv_网易云音乐-- 剔除流量卡' uv_13,
        'uv_uc-- 剔除流量卡' uv_14,
        'uv_手机百度-- 剔除流量卡' uv_15,
          'uv_喜马拉雅-- 剔除流量卡' uv_16
union all
select  '各app' app,
        '证件归属地分布' model,
        t1.cert_prov,
        sum(case when tag1=1 then 1 else 0 end) uv1,
        sum(case when tag2=1 then 1 else 0 end) uv2,
        sum(case when tag3=1 then 1 else 0 end) uv3,
        sum(case when tag4=1 then 1 else 0 end) uv4,
        sum(case when tag5=1 then 1 else 0 end) uv5,
        sum(case when tag6=1 then 1 else 0 end) uv6,
        sum(case when tag7=1 then 1 else 0 end) uv7,
        sum(case when tag8=1 then 1 else 0 end) uv8,
        sum(case when tag9=1 then 1 else 0 end) uv9,
        sum(case when tag10=1 then 1 else 0 end) uv10,
        sum(case when tag11=1 then 1 else 0 end) uv11,
        sum(case when tag12=1 then 1 else 0 end) uv12,
        sum(case when tag13=1 then 1 else 0 end) uv13,
        sum(case when tag14=1 then 1 else 0 end) uv14,
        sum(case when tag15=1 then 1 else 0 end) uv15,
        sum(case when tag16=1 then 1 else 0 end) uv16,
        sum(case when tag1=1 and device_number1 is not null then  1 else 0 end) uv_1,
        sum(case when tag2=1 and device_number1 is not null then  1 else 0 end) uv_2,
        sum(case when tag3=1 and device_number1 is not null then  1 else 0 end) uv_3,
        sum(case when tag4=1 and device_number1 is not null then  1 else 0 end) uv_4,
        sum(case when tag5=1 and device_number1 is not null then  1 else 0 end) uv_5,
        sum(case when tag6=1 and device_number1 is not null then  1 else 0 end) uv_6,
        sum(case when tag7=1 and device_number1 is not null then  1 else 0 end) uv_7,
        sum(case when tag8=1 and device_number1 is not null then  1 else 0 end) uv_8,
        sum(case when tag9=1 and device_number1 is not null then  1 else 0 end) uv_9,
        sum(case when tag10=1 and device_number1 is not null then  1 else 0 end) uv_10,
        sum(case when tag11=1 and device_number1 is not null then  1 else 0 end) uv_11,
        sum(case when tag12=1 and device_number1 is not null then  1 else 0 end) uv_12,
        sum(case when tag13=1 and device_number1 is not null then  1 else 0 end) uv_13,
        sum(case when tag14=1 and device_number1 is not null then  1 else 0 end) uv_14,
        sum(case when tag15=1 and device_number1 is not null then  1 else 0 end) uv_15,
        sum(case when tag16=1 and device_number1 is not null then  1 else 0 end) uv_16
    from 
        tmp_user_app_profile_t1 t
     left join 
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '各app' app,
        '性别分布' model,
        '性别' cust_sex,
        'uv_新闻行业' uv1,
        'uv_腾讯新闻' uv2,
        'uv_头条' uv3,
        'uv_快报' uv4,
        'uv_网易新闻' uv5,
        'uv_新浪新闻' uv6,
        'uv_搜狐新闻' uv7,
        'uv_凤凰新闻' uv8,
        'uv_音乐行业' uv9,
        'uv_qq音乐' uv10,
        'uv_酷我' uv11,
        'uv_酷狗' uv12,
        'uv_网易云音乐' uv13,
        'uv_uc' uv14,
        'uv_手机百度' uv15,
          'uv_喜马拉雅' uv16,
        'uv_新闻行业-- 剔除流量卡' uv_1,
        'uv_腾讯新闻-- 剔除流量卡' uv_2,
        'uv_头条-- 剔除流量卡' uv_3,
        'uv_快报-- 剔除流量卡' uv_4,
        'uv_网易新闻-- 剔除流量卡' uv_5,
        'uv_新浪新闻-- 剔除流量卡' uv_6,
        'uv_搜狐新闻-- 剔除流量卡' uv_7,
        'uv_凤凰新闻-- 剔除流量卡' uv_8,
        'uv_音乐行业-- 剔除流量卡' uv_9,
        'uv_qq音乐-- 剔除流量卡' uv_10,
        'uv_酷我-- 剔除流量卡' uv_11,
        'uv_酷狗-- 剔除流量卡' uv_12,
        'uv_网易云音乐-- 剔除流量卡' uv_13,
        'uv_uc-- 剔除流量卡' uv_14,
        'uv_手机百度-- 剔除流量卡' uv_15,
          'uv_喜马拉雅-- 剔除流量卡' uv_16
union all
select  '各app' app,
        '性别分布' model,
        t1.cust_sex,
        sum(case when tag1=1 then 1 else 0 end) uv1,
        sum(case when tag2=1 then 1 else 0 end) uv2,
        sum(case when tag3=1 then 1 else 0 end) uv3,
        sum(case when tag4=1 then 1 else 0 end) uv4,
        sum(case when tag5=1 then 1 else 0 end) uv5,
        sum(case when tag6=1 then 1 else 0 end) uv6,
        sum(case when tag7=1 then 1 else 0 end) uv7,
        sum(case when tag8=1 then 1 else 0 end) uv8,
        sum(case when tag9=1 then 1 else 0 end) uv9,
        sum(case when tag10=1 then 1 else 0 end) uv10,
        sum(case when tag11=1 then 1 else 0 end) uv11,
        sum(case when tag12=1 then 1 else 0 end) uv12,
        sum(case when tag13=1 then 1 else 0 end) uv13,
        sum(case when tag14=1 then 1 else 0 end) uv14,
        sum(case when tag15=1 then 1 else 0 end) uv15,
        sum(case when tag16=1 then 1 else 0 end) uv16,
        sum(case when tag1=1 and device_number1 is not null then  1 else 0 end) uv_1,
        sum(case when tag2=1 and device_number1 is not null then  1 else 0 end) uv_2,
        sum(case when tag3=1 and device_number1 is not null then  1 else 0 end) uv_3,
        sum(case when tag4=1 and device_number1 is not null then  1 else 0 end) uv_4,
        sum(case when tag5=1 and device_number1 is not null then  1 else 0 end) uv_5,
        sum(case when tag6=1 and device_number1 is not null then  1 else 0 end) uv_6,
        sum(case when tag7=1 and device_number1 is not null then  1 else 0 end) uv_7,
        sum(case when tag8=1 and device_number1 is not null then  1 else 0 end) uv_8,
        sum(case when tag9=1 and device_number1 is not null then  1 else 0 end) uv_9,
        sum(case when tag10=1 and device_number1 is not null then  1 else 0 end) uv_10,
        sum(case when tag11=1 and device_number1 is not null then  1 else 0 end) uv_11,
        sum(case when tag12=1 and device_number1 is not null then  1 else 0 end) uv_12,
        sum(case when tag13=1 and device_number1 is not null then  1 else 0 end) uv_13,
        sum(case when tag14=1 and device_number1 is not null then  1 else 0 end) uv_14,
        sum(case when tag15=1 and device_number1 is not null then  1 else 0 end) uv_15,
        sum(case when tag16=1 and device_number1 is not null then  1 else 0 end) uv_16
    from 
        tmp_user_app_profile_t1 t
     left join 
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '各app' app,
        '消费水平分布' model,
        '消费水平' cost_level,
        'uv_新闻行业' uv1,
        'uv_腾讯新闻' uv2,
        'uv_头条' uv3,
        'uv_快报' uv4,
        'uv_网易新闻' uv5,
        'uv_新浪新闻' uv6,
        'uv_搜狐新闻' uv7,
        'uv_凤凰新闻' uv8,
        'uv_音乐行业' uv9,
        'uv_qq音乐' uv10,
        'uv_酷我' uv11,
        'uv_酷狗' uv12,
        'uv_网易云音乐' uv13,
        'uv_uc' uv14,
        'uv_手机百度' uv15,
          'uv_喜马拉雅' uv16,
        'uv_新闻行业-- 剔除流量卡' uv_1,
        'uv_腾讯新闻-- 剔除流量卡' uv_2,
        'uv_头条-- 剔除流量卡' uv_3,
        'uv_快报-- 剔除流量卡' uv_4,
        'uv_网易新闻-- 剔除流量卡' uv_5,
        'uv_新浪新闻-- 剔除流量卡' uv_6,
        'uv_搜狐新闻-- 剔除流量卡' uv_7,
        'uv_凤凰新闻-- 剔除流量卡' uv_8,
        'uv_音乐行业-- 剔除流量卡' uv_9,
        'uv_qq音乐-- 剔除流量卡' uv_10,
        'uv_酷我-- 剔除流量卡' uv_11,
        'uv_酷狗-- 剔除流量卡' uv_12,
        'uv_网易云音乐-- 剔除流量卡' uv_13,
        'uv_uc-- 剔除流量卡' uv_14,
        'uv_手机百度-- 剔除流量卡' uv_15,
          'uv_喜马拉雅-- 剔除流量卡' uv_16
union all
select  '各app' app,
        '消费水平分布' model,
        t1.cost_level,
        sum(case when tag1=1 then 1 else 0 end) uv1,
        sum(case when tag2=1 then 1 else 0 end) uv2,
        sum(case when tag3=1 then 1 else 0 end) uv3,
        sum(case when tag4=1 then 1 else 0 end) uv4,
        sum(case when tag5=1 then 1 else 0 end) uv5,
        sum(case when tag6=1 then 1 else 0 end) uv6,
        sum(case when tag7=1 then 1 else 0 end) uv7,
        sum(case when tag8=1 then 1 else 0 end) uv8,
        sum(case when tag9=1 then 1 else 0 end) uv9,
        sum(case when tag10=1 then 1 else 0 end) uv10,
        sum(case when tag11=1 then 1 else 0 end) uv11,
        sum(case when tag12=1 then 1 else 0 end) uv12,
        sum(case when tag13=1 then 1 else 0 end) uv13,
        sum(case when tag14=1 then 1 else 0 end) uv14,
        sum(case when tag15=1 then 1 else 0 end) uv15,
        sum(case when tag16=1 then 1 else 0 end) uv16,
        sum(case when tag1=1 and device_number1 is not null then  1 else 0 end) uv_1,
        sum(case when tag2=1 and device_number1 is not null then  1 else 0 end) uv_2,
        sum(case when tag3=1 and device_number1 is not null then  1 else 0 end) uv_3,
        sum(case when tag4=1 and device_number1 is not null then  1 else 0 end) uv_4,
        sum(case when tag5=1 and device_number1 is not null then  1 else 0 end) uv_5,
        sum(case when tag6=1 and device_number1 is not null then  1 else 0 end) uv_6,
        sum(case when tag7=1 and device_number1 is not null then  1 else 0 end) uv_7,
        sum(case when tag8=1 and device_number1 is not null then  1 else 0 end) uv_8,
        sum(case when tag9=1 and device_number1 is not null then  1 else 0 end) uv_9,
        sum(case when tag10=1 and device_number1 is not null then  1 else 0 end) uv_10,
        sum(case when tag11=1 and device_number1 is not null then  1 else 0 end) uv_11,
        sum(case when tag12=1 and device_number1 is not null then  1 else 0 end) uv_12,
        sum(case when tag13=1 and device_number1 is not null then  1 else 0 end) uv_13,
        sum(case when tag14=1 and device_number1 is not null then  1 else 0 end) uv_14,
        sum(case when tag15=1 and device_number1 is not null then  1 else 0 end) uv_15,
        sum(case when tag16=1 and device_number1 is not null then  1 else 0 end) uv_16
    from 
        tmp_user_app_profile_t1 t
     left join 
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cost_level;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '各app' app,
        '年龄分布' model,
        '年龄' age_range,
        'uv_新闻行业' uv1,
        'uv_腾讯新闻' uv2,
        'uv_头条' uv3,
        'uv_快报' uv4,
        'uv_网易新闻' uv5,
        'uv_新浪新闻' uv6,
        'uv_搜狐新闻' uv7,
        'uv_凤凰新闻' uv8,
        'uv_音乐行业' uv9,
        'uv_qq音乐' uv10,
        'uv_酷我' uv11,
        'uv_酷狗' uv12,
        'uv_网易云音乐' uv13,
        'uv_uc' uv14,
        'uv_手机百度' uv15,
          'uv_喜马拉雅' uv16,
        'uv_新闻行业-- 剔除流量卡' uv_1,
        'uv_腾讯新闻-- 剔除流量卡' uv_2,
        'uv_头条-- 剔除流量卡' uv_3,
        'uv_快报-- 剔除流量卡' uv_4,
        'uv_网易新闻-- 剔除流量卡' uv_5,
        'uv_新浪新闻-- 剔除流量卡' uv_6,
        'uv_搜狐新闻-- 剔除流量卡' uv_7,
        'uv_凤凰新闻-- 剔除流量卡' uv_8,
        'uv_音乐行业-- 剔除流量卡' uv_9,
        'uv_qq音乐-- 剔除流量卡' uv_10,
        'uv_酷我-- 剔除流量卡' uv_11,
        'uv_酷狗-- 剔除流量卡' uv_12,
        'uv_网易云音乐-- 剔除流量卡' uv_13,
        'uv_uc-- 剔除流量卡' uv_14,
        'uv_手机百度-- 剔除流量卡' uv_15,
          'uv_喜马拉雅-- 剔除流量卡' uv_16
union all
select  '各app' app,
        '年龄分布' model,
        t1.age_range,
        sum(case when tag1=1 then 1 else 0 end) uv1,
        sum(case when tag2=1 then 1 else 0 end) uv2,
        sum(case when tag3=1 then 1 else 0 end) uv3,
        sum(case when tag4=1 then 1 else 0 end) uv4,
        sum(case when tag5=1 then 1 else 0 end) uv5,
        sum(case when tag6=1 then 1 else 0 end) uv6,
        sum(case when tag7=1 then 1 else 0 end) uv7,
        sum(case when tag8=1 then 1 else 0 end) uv8,
        sum(case when tag9=1 then 1 else 0 end) uv9,
        sum(case when tag10=1 then 1 else 0 end) uv10,
        sum(case when tag11=1 then 1 else 0 end) uv11,
        sum(case when tag12=1 then 1 else 0 end) uv12,
        sum(case when tag13=1 then 1 else 0 end) uv13,
        sum(case when tag14=1 then 1 else 0 end) uv14,
        sum(case when tag15=1 then 1 else 0 end) uv15,
        sum(case when tag16=1 then 1 else 0 end) uv16,
        sum(case when tag1=1 and device_number1 is not null then  1 else 0 end) uv_1,
        sum(case when tag2=1 and device_number1 is not null then  1 else 0 end) uv_2,
        sum(case when tag3=1 and device_number1 is not null then  1 else 0 end) uv_3,
        sum(case when tag4=1 and device_number1 is not null then  1 else 0 end) uv_4,
        sum(case when tag5=1 and device_number1 is not null then  1 else 0 end) uv_5,
        sum(case when tag6=1 and device_number1 is not null then  1 else 0 end) uv_6,
        sum(case when tag7=1 and device_number1 is not null then  1 else 0 end) uv_7,
        sum(case when tag8=1 and device_number1 is not null then  1 else 0 end) uv_8,
        sum(case when tag9=1 and device_number1 is not null then  1 else 0 end) uv_9,
        sum(case when tag10=1 and device_number1 is not null then  1 else 0 end) uv_10,
        sum(case when tag11=1 and device_number1 is not null then  1 else 0 end) uv_11,
        sum(case when tag12=1 and device_number1 is not null then  1 else 0 end) uv_12,
        sum(case when tag13=1 and device_number1 is not null then  1 else 0 end) uv_13,
        sum(case when tag14=1 and device_number1 is not null then  1 else 0 end) uv_14,
        sum(case when tag15=1 and device_number1 is not null then  1 else 0 end) uv_15,
        sum(case when tag16=1 and device_number1 is not null then  1 else 0 end) uv_16
    from 
        tmp_user_app_profile_t1 t
     left join 
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.age_range;




drop table if exists tmp_user_app_profile_t1;
drop table if exists tmp_user_app_profile_t4;
--  drop table if exists tmp_user_app_profile_t2;
--  drop table if exists tmp_user_app_profile_t3;



