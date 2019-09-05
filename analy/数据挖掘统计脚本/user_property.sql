

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
      from (select '201908' pt_days,
                   appname,
                   device_number,
                   count(distinct pt_days) active_days,
                   sum(pv) pv
           from  user_action_app_d t
           where pt_days>='20190801' and pt_days<='20190831'
           group by appname,
                    device_number) a
      group by device_number
     ) t
left join
    (select device_number
        from zba_dwa.dwa_v_m_cus_nm_user_info b
        where b.month_id='201907'
        and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                            '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                            '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                            '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')
        group by device_number) t1
on t.device_number=t1.device_number;











drop table if exists tmp_user_app_profile_t2;
create table tmp_user_app_profile_t2 as
select
        device_number,
        max(case when url_host in ('feed.baidu.com','ext.baidu.com') or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag1,
        max(case when url_host rlike '^(sp)([0-9]*)(\.baidu.com)$' then 1 else 0 end) tag2,
        max(case when url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag3,
        max(case when url_host='boxnovel.baidu.com' then 1 else 0 end) tag4,
        max(case when url_host='voice.baidu.com' and actiontag>3  then 1 else 0 end) tag5,
        max(case when instr(url_host,'.uc.cn')>0 and instr(urltag,'book')>0 then 1 else 0 end) tag6,
        max(case when url_host='iflow.uczzd.cn'  then 1 else 0 end) tag7,
        max(case when url_host='iflow.uczzd.cn' and instr(urltag,'video')>0 then 1 else 0 end) tag8,
        max(case when url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') then 1 else 0 end) tag9,
        max(case when instr(urltag,'app_name=news_article')>0 and (instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0)  then 1 else 0 end) tag10,
        max(case when instr(urltag,'app_name=news_article')>0 and instr(urltag,'keyword')>0 then 1 else 0 end) tag11,
        max(case when instr(urltag,'app_name=news_article')>0 and (instr(urltag,'category=video')>0 or instr(urltag,'action=GetPlayInfo')>0)  then 1 else 0 end) tag12,
        max(case when instr(urltag,'app_name=news_article')>0 and instr(urltag,'answer/detail')>0 then 1 else 0 end) tag13,
        max(case when instr(urltag,'app_name=news_article')>0 and (instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=hotsoon_video_detail_draw')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0)  then 1 else 0 end) tag14,
        max(case when instr(urltag,'app_name=news_article')>0 and (instr(urltag,'video/play')>0 or instr(urltag,'video/app')>0) and instr(urltag,'from_category=__all__')>0 then 1 else 0 end) tag15,
        max(case when instr(urltag,'app_name=news_article')>0 and  instr(urltag,'user/profile')>0 then 1 else 0 end) tag16,
        max(case when instr(urltag,'app_name=news_article_lite')>0 and (instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0)  then 1 else 0 end) tag17,
        max(case when instr(urltag,'app_name=news_article_lite')>0 and instr(urltag,'keyword')>0 then 1 else 0 end) tag18,
        max(case when instr(urltag,'app_name=news_article_lite')>0 and (instr(urltag,'category=video')>0 or instr(urltag,'action=GetPlayInfo')>0)  then 1 else 0 end) tag19,
        max(case when instr(urltag,'app_name=news_article_lite')>0 and instr(urltag,'answer/detail')>0 then 1 else 0 end) tag20,
        max(case when instr(urltag,'app_name=news_article_lite')>0 and (instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=hotsoon_video_detail_draw')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0)  then 1 else 0 end) tag21,
        max(case when instr(urltag,'app_name=news_article_lite')>0 and (instr(urltag,'video/play')>0 or instr(urltag,'video/app')>0) and instr(urltag,'from_category=__all__')>0 then 1 else 0 end) tag22,
        max(case when instr(urltag,'app_name=news_article_lite')>0 and  instr(urltag,'user/profile')>0 then 1 else 0 end) tag23,
        max(case when instr(url_host,'.kuwo.cn')>0 and instr(urltag,'isVip=1')>0 then 1 else 0 end) tag24,
        max(case when (url_host in ('ip2.kugou.com','newsongretry.kugou.com','ip.kugou.com') and instr(urltag,'vip_type=6')>0 )
                       or (url_host='ads.service.kugou.com' and instr(urltag,'isVip=1')>0 ) then 1 else 0 end) tag25,
        max(case when url_host='interface.music.163.com' and instr(urltag,'enhance/download')>0 then 1 else 0 end) tag26,
        max(case when instr(url_host,'ximalaya.com')>0 and (instr(urltag,'vipCategory')>0 or instr(urltag,'vipStatus=1')>0 or instr(urltag,'vipStatus=2')>0 or instr(urltag,'vipStatus=3')>0) then 1 else 0 end) tag27
    from  user_action_tag_d
    where  pt_days>='20190801' and pt_days<='20190831'
    group by device_number;



-- drop table if exists tmp_user_app_profile_t3;
-- create table tmp_user_app_profile_t3 as
-- select
--         device_number,
--         substr(pt_days,0,6) pt_days,
--         max(case when instr(url_host,'.kuwo.cn')>0 and instr(urltag,'isVip=1')>0 then 1 else 0 end) tag1,
--         max(case when (url_host in ('ip2.kugou.com','newsongretry.kugou.com','ip.kugou.com') and instr(urltag,'vip_type=6')>0 )
--                        or (url_host='ads.service.kugou.com' and instr(urltag,'isVip=1')>0 ) then 1 else 0 end) tag2,
--         max(case when url_host='interface.music.163.com' and instr(urltag,'enhance/download')>0 then 1 else 0 end) tag3,
--         max(case when instr(url_host,'ximalaya.com')>0 and (instr(urltag,'vipCategory')>0 or instr(urltag,'vipStatus=1')>0 or instr(urltag,'vipStatus=2')>0 or instr(urltag,'vipStatus=3')>0) then 1 else 0 end) tag4
--     from  user_action_tag_d
--     where  pt_days>='20190401' and pt_days<='20190531'
--     group by device_number,
--              substr(pt_days,0,6);



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
--      where pt_days>='20190701' and pt_days<='20190731'
--      and  url_host in ('bdbus-turbonet.baidu.com','hmma.baidu.com','mbd.baidu.com')
--      group by pt_days,
--              hh,
--              device_number)  a
--  where tag1=1 and tag2=1 and tag3=1
--  group by device_number;

drop table if exists tmp_user_app_profile_t3;
create table tmp_user_app_profile_t3 as
select device_number from user_action_context_d
where pt_days>='20190801' and pt_days<='20190831'
    and model='toutiao_article'
group by device_number;


drop table if exists tmp_user_app_profile_t4;
create table tmp_user_app_profile_t4 as
select * from
    (select t.*,
        row_number() over(partition by device_number order by age_range desc) rn
    from user_info_sample t  where pt_days='201907') a
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
          'uv_喜马拉雅-- 剔除流量卡' uv_16;
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
          'uv_喜马拉雅-- 剔除流量卡' uv_16;
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
          'uv_喜马拉雅-- 剔除流量卡' uv_16;
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
          'uv_喜马拉雅-- 剔除流量卡' uv_16;
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
          'uv_喜马拉雅-- 剔除流量卡' uv_16;
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
          'uv_喜马拉雅-- 剔除流量卡' uv_16;
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
        'uv_喜马拉雅-- 剔除流量卡' uv_16;
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

select '-- -- -*******************************************************************************************************************-- -- -';

--  -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -头条内容点击属性分布-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --


select  '头条内容点击' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from
        tmp_user_app_profile_t3 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '头条内容点击' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from
       tmp_user_app_profile_t3  t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;
select '-- -- -*******************************************************************************************************************-- -- -';

select  '头条内容点击' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from
      tmp_user_app_profile_t3 t
    left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '头条内容点击' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from
      tmp_user_app_profile_t3 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '头条内容点击' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from
        tmp_user_app_profile_t3 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '头条内容点击' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from
        tmp_user_app_profile_t3 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cost_level;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '头条内容点击' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from
        tmp_user_app_profile_t3 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.age_range;

select '-- -- -*******************************************************************************************************************-- -- -';
-- -- -- -- -- -- -- -- -- -- -- -- -- -头条内容点击pv分布-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

select '头条内容点击pv分布' model,
       pv_range,
       count(*) uv
     from
    (select   device_number,
                case when count(*)=1 then '1'
                    when count(*)>1 and count(*)<=5 then '(1-5]'
                    when count(*)>5 and count(*)<=10 then '(5-10]'
                    when count(*)>10 and count(*)<=20 then '(10-20]'
                    when count(*)>20 then '(20,&]'
                end pv_range
            from user_action_context_d
         where pt_days>='20190801' and pt_days<='20190831'
            and model='toutiao_article'
        group by device_number) t
    group by pv_range;

select '-- -- -*******************************************************************************************************************-- -- -';

--  -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 信息流模块属性分布-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

select  '信息流模块' app,
        '常驻省份分布' model,
        '省份' top1_home_province,
        'uv_手百信息流-ext' uv1,
        'uv_手百搜索' uv2,
        'uv_手百视频' uv3,
        'uv_手百小说' uv4,
        'uv_手百语音' uv5,
        'uv_uc小说' uv6,
        'uv_uc-feed流' uv7,
        'uv_uc视频点击' uv8,
        'uv_uc-搜索合计' uv9,
        'uv_头条-点击caaa' uv10,
        'uv_头条-搜索' uv11,
        'uv_头条-短视频合并ca' uv12,
        'uv_头条-问答s' uv13,
        'uv_头条-小视频合并ccvg' uv14,
        'uv_头条-主TL视频点击' uv15,
        'uv_头条-我的页面' uv16,
        'uv_头条极速版-点击caaa' uv17,
        'uv_头条极速版-搜索' uv18,
        'uv_头条极速版-短视频合并ca' uv19,
        'uv_头条极速版-问答s' uv20,
        'uv_头条极速版-小视频合并ccvg' uv21,
        'uv_头条极速版-主TL视频点击' uv22,
        'uv_头条极速版-我的页面' uv23,
        'uv_酷我VIP' uv24,
        'uv_酷狗VIP' uv25,
        'uv_网易云音乐VIP' uv26,
        'uv_喜马拉雅VIP' uv27
;
select  '信息流模块' app,
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
        sum(case when tag17=1 then 1 else 0 end) uv17,
        sum(case when tag18=1 then 1 else 0 end) uv18,
        sum(case when tag19=1 then 1 else 0 end) uv19,
        sum(case when tag20=1 then 1 else 0 end) uv20,
        sum(case when tag21=1 then 1 else 0 end) uv21,
        sum(case when tag22=1 then 1 else 0 end) uv22,
        sum(case when tag23=1 then 1 else 0 end) uv23,
        sum(case when tag24=1 then 1 else 0 end) uv24,
        sum(case when tag25=1 then 1 else 0 end) uv25,
        sum(case when tag26=1 then 1 else 0 end) uv26,
        sum(case when tag27=1 then 1 else 0 end) uv27
from
       tmp_user_app_profile_t2 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '信息流模块' app,
        '常驻城市分布' model,
        '城市' top1_home_city,
        'uv_手百信息流-ext' uv1,
        'uv_手百搜索' uv2,
        'uv_手百视频' uv3,
        'uv_手百小说' uv4,
        'uv_手百语音' uv5,
        'uv_uc小说' uv6,
        'uv_uc-feed流' uv7,
        'uv_uc视频点击' uv8,
        'uv_uc-搜索合计' uv9,
        'uv_头条-点击caaa' uv10,
        'uv_头条-搜索' uv11,
        'uv_头条-短视频合并ca' uv12,
        'uv_头条-问答s' uv13,
        'uv_头条-小视频合并ccvg' uv14,
        'uv_头条-主TL视频点击' uv15,
        'uv_头条-我的页面' uv16,
        'uv_头条极速版-点击caaa' uv17,
        'uv_头条极速版-搜索' uv18,
        'uv_头条极速版-短视频合并ca' uv19,
        'uv_头条极速版-问答s' uv20,
        'uv_头条极速版-小视频合并ccvg' uv21,
        'uv_头条极速版-主TL视频点击' uv22,
        'uv_头条极速版-我的页面' uv23,
        'uv_酷我VIP' uv24,
        'uv_酷狗VIP' uv25,
        'uv_网易云音乐VIP' uv26,
        'uv_喜马拉雅VIP' uv27
;
select  '信息流模块' app,
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
        sum(case when tag17=1 then 1 else 0 end) uv17,
        sum(case when tag18=1 then 1 else 0 end) uv18,
        sum(case when tag19=1 then 1 else 0 end) uv19,
        sum(case when tag20=1 then 1 else 0 end) uv20,
        sum(case when tag21=1 then 1 else 0 end) uv21,
        sum(case when tag22=1 then 1 else 0 end) uv22,
        sum(case when tag23=1 then 1 else 0 end) uv23,
        sum(case when tag24=1 then 1 else 0 end) uv24,
        sum(case when tag25=1 then 1 else 0 end) uv25,
        sum(case when tag26=1 then 1 else 0 end) uv26,
        sum(case when tag27=1 then 1 else 0 end) uv27
from
        tmp_user_app_profile_t2 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;

select '-- -- -*******************************************************************************************************************-- -- -';


select  '信息流模块' app,
        '品牌分布' model,
        '品牌' factory_desc,
        'uv_手百信息流-ext' uv1,
        'uv_手百搜索' uv2,
        'uv_手百视频' uv3,
        'uv_手百小说' uv4,
        'uv_手百语音' uv5,
        'uv_uc小说' uv6,
        'uv_uc-feed流' uv7,
        'uv_uc视频点击' uv8,
        'uv_uc-搜索合计' uv9,
        'uv_头条-点击caaa' uv10,
        'uv_头条-搜索' uv11,
        'uv_头条-短视频合并ca' uv12,
        'uv_头条-问答s' uv13,
        'uv_头条-小视频合并ccvg' uv14,
        'uv_头条-主TL视频点击' uv15,
        'uv_头条-我的页面' uv16,
        'uv_头条极速版-点击caaa' uv17,
        'uv_头条极速版-搜索' uv18,
        'uv_头条极速版-短视频合并ca' uv19,
        'uv_头条极速版-问答s' uv20,
        'uv_头条极速版-小视频合并ccvg' uv21,
        'uv_头条极速版-主TL视频点击' uv22,
        'uv_头条极速版-我的页面' uv23,
        'uv_酷我VIP' uv24,
        'uv_酷狗VIP' uv25,
        'uv_网易云音乐VIP' uv26,
        'uv_喜马拉雅VIP' uv27
;
select  '信息流模块' app,
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
        sum(case when tag17=1 then 1 else 0 end) uv17,
        sum(case when tag18=1 then 1 else 0 end) uv18,
        sum(case when tag19=1 then 1 else 0 end) uv19,
        sum(case when tag20=1 then 1 else 0 end) uv20,
        sum(case when tag21=1 then 1 else 0 end) uv21,
        sum(case when tag22=1 then 1 else 0 end) uv22,
        sum(case when tag23=1 then 1 else 0 end) uv23,
        sum(case when tag24=1 then 1 else 0 end) uv24,
        sum(case when tag25=1 then 1 else 0 end) uv25,
        sum(case when tag26=1 then 1 else 0 end) uv26,
        sum(case when tag27=1 then 1 else 0 end) uv27
from
        tmp_user_app_profile_t2 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '信息流模块' app,
        '证件归属地分布' model,
        '归属地' cert_prov,
        'uv_手百信息流-ext' uv1,
        'uv_手百搜索' uv2,
        'uv_手百视频' uv3,
        'uv_手百小说' uv4,
        'uv_手百语音' uv5,
        'uv_uc小说' uv6,
        'uv_uc-feed流' uv7,
        'uv_uc视频点击' uv8,
        'uv_uc-搜索合计' uv9,
        'uv_头条-点击caaa' uv10,
        'uv_头条-搜索' uv11,
        'uv_头条-短视频合并ca' uv12,
        'uv_头条-问答s' uv13,
        'uv_头条-小视频合并ccvg' uv14,
        'uv_头条-主TL视频点击' uv15,
        'uv_头条-我的页面' uv16,
        'uv_头条极速版-点击caaa' uv17,
        'uv_头条极速版-搜索' uv18,
        'uv_头条极速版-短视频合并ca' uv19,
        'uv_头条极速版-问答s' uv20,
        'uv_头条极速版-小视频合并ccvg' uv21,
        'uv_头条极速版-主TL视频点击' uv22,
        'uv_头条极速版-我的页面' uv23,
        'uv_酷我VIP' uv24,
        'uv_酷狗VIP' uv25,
        'uv_网易云音乐VIP' uv26,
        'uv_喜马拉雅VIP' uv27
;
select  '信息流模块' app,
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
        sum(case when tag17=1 then 1 else 0 end) uv17,
        sum(case when tag18=1 then 1 else 0 end) uv18,
        sum(case when tag19=1 then 1 else 0 end) uv19,
        sum(case when tag20=1 then 1 else 0 end) uv20,
        sum(case when tag21=1 then 1 else 0 end) uv21,
        sum(case when tag22=1 then 1 else 0 end) uv22,
        sum(case when tag23=1 then 1 else 0 end) uv23,
        sum(case when tag24=1 then 1 else 0 end) uv24,
        sum(case when tag25=1 then 1 else 0 end) uv25,
        sum(case when tag26=1 then 1 else 0 end) uv26,
        sum(case when tag27=1 then 1 else 0 end) uv27
from
        tmp_user_app_profile_t2 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '信息流模块' app,
        '性别分布' model,
        '性别' cust_sex,
        'uv_手百信息流-ext' uv1,
        'uv_手百搜索' uv2,
        'uv_手百视频' uv3,
        'uv_手百小说' uv4,
        'uv_手百语音' uv5,
        'uv_uc小说' uv6,
        'uv_uc-feed流' uv7,
        'uv_uc视频点击' uv8,
        'uv_uc-搜索合计' uv9,
        'uv_头条-点击caaa' uv10,
        'uv_头条-搜索' uv11,
        'uv_头条-短视频合并ca' uv12,
        'uv_头条-问答s' uv13,
        'uv_头条-小视频合并ccvg' uv14,
        'uv_头条-主TL视频点击' uv15,
        'uv_头条-我的页面' uv16,
        'uv_头条极速版-点击caaa' uv17,
        'uv_头条极速版-搜索' uv18,
        'uv_头条极速版-短视频合并ca' uv19,
        'uv_头条极速版-问答s' uv20,
        'uv_头条极速版-小视频合并ccvg' uv21,
        'uv_头条极速版-主TL视频点击' uv22,
        'uv_头条极速版-我的页面' uv23,
        'uv_酷我VIP' uv24,
        'uv_酷狗VIP' uv25,
        'uv_网易云音乐VIP' uv26,
        'uv_喜马拉雅VIP' uv27
;
select  '信息流模块' app,
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
        sum(case when tag17=1 then 1 else 0 end) uv17,
        sum(case when tag18=1 then 1 else 0 end) uv18,
        sum(case when tag19=1 then 1 else 0 end) uv19,
        sum(case when tag20=1 then 1 else 0 end) uv20,
        sum(case when tag21=1 then 1 else 0 end) uv21,
        sum(case when tag22=1 then 1 else 0 end) uv22,
        sum(case when tag23=1 then 1 else 0 end) uv23,
        sum(case when tag24=1 then 1 else 0 end) uv24,
        sum(case when tag25=1 then 1 else 0 end) uv25,
        sum(case when tag26=1 then 1 else 0 end) uv26,
        sum(case when tag27=1 then 1 else 0 end) uv27
from
        tmp_user_app_profile_t2 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '信息流模块' app,
        '消费水平分布' model,
        '消费水平' cost_level,
        'uv_手百信息流-ext' uv1,
        'uv_手百搜索' uv2,
        'uv_手百视频' uv3,
        'uv_手百小说' uv4,
        'uv_手百语音' uv5,
        'uv_uc小说' uv6,
        'uv_uc-feed流' uv7,
        'uv_uc视频点击' uv8,
        'uv_uc-搜索合计' uv9,
        'uv_头条-点击caaa' uv10,
        'uv_头条-搜索' uv11,
        'uv_头条-短视频合并ca' uv12,
        'uv_头条-问答s' uv13,
        'uv_头条-小视频合并ccvg' uv14,
        'uv_头条-主TL视频点击' uv15,
        'uv_头条-我的页面' uv16,
        'uv_头条极速版-点击caaa' uv17,
        'uv_头条极速版-搜索' uv18,
        'uv_头条极速版-短视频合并ca' uv19,
        'uv_头条极速版-问答s' uv20,
        'uv_头条极速版-小视频合并ccvg' uv21,
        'uv_头条极速版-主TL视频点击' uv22,
        'uv_头条极速版-我的页面' uv23,
        'uv_酷我VIP' uv24,
        'uv_酷狗VIP' uv25,
        'uv_网易云音乐VIP' uv26,
        'uv_喜马拉雅VIP' uv27
;
select  '信息流模块' app,
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
        sum(case when tag17=1 then 1 else 0 end) uv17,
        sum(case when tag18=1 then 1 else 0 end) uv18,
        sum(case when tag19=1 then 1 else 0 end) uv19,
        sum(case when tag20=1 then 1 else 0 end) uv20,
        sum(case when tag21=1 then 1 else 0 end) uv21,
        sum(case when tag22=1 then 1 else 0 end) uv22,
        sum(case when tag23=1 then 1 else 0 end) uv23,
        sum(case when tag24=1 then 1 else 0 end) uv24,
        sum(case when tag25=1 then 1 else 0 end) uv25,
        sum(case when tag26=1 then 1 else 0 end) uv26,
        sum(case when tag27=1 then 1 else 0 end) uv27
from
        tmp_user_app_profile_t2 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.cost_level;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '信息流模块' app,
        '年龄分布' model,
        '年龄' age_range,
        'uv_手百信息流-ext' uv1,
        'uv_手百搜索' uv2,
        'uv_手百视频' uv3,
        'uv_手百小说' uv4,
        'uv_手百语音' uv5,
        'uv_uc小说' uv6,
        'uv_uc-feed流' uv7,
        'uv_uc视频点击' uv8,
        'uv_uc-搜索合计' uv9,
        'uv_头条-点击caaa' uv10,
        'uv_头条-搜索' uv11,
        'uv_头条-短视频合并ca' uv12,
        'uv_头条-问答s' uv13,
        'uv_头条-小视频合并ccvg' uv14,
        'uv_头条-主TL视频点击' uv15,
        'uv_头条-我的页面' uv16,
        'uv_头条极速版-点击caaa' uv17,
        'uv_头条极速版-搜索' uv18,
        'uv_头条极速版-短视频合并ca' uv19,
        'uv_头条极速版-问答s' uv20,
        'uv_头条极速版-小视频合并ccvg' uv21,
        'uv_头条极速版-主TL视频点击' uv22,
        'uv_头条极速版-我的页面' uv23,
        'uv_酷我VIP' uv24,
        'uv_酷狗VIP' uv25,
        'uv_网易云音乐VIP' uv26,
        'uv_喜马拉雅VIP' uv27
;
select  '信息流模块' app,
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
        sum(case when tag17=1 then 1 else 0 end) uv17,
        sum(case when tag18=1 then 1 else 0 end) uv18,
        sum(case when tag19=1 then 1 else 0 end) uv19,
        sum(case when tag20=1 then 1 else 0 end) uv20,
        sum(case when tag21=1 then 1 else 0 end) uv21,
        sum(case when tag22=1 then 1 else 0 end) uv22,
        sum(case when tag23=1 then 1 else 0 end) uv23,
        sum(case when tag24=1 then 1 else 0 end) uv24,
        sum(case when tag25=1 then 1 else 0 end) uv25,
        sum(case when tag26=1 then 1 else 0 end) uv26,
        sum(case when tag27=1 then 1 else 0 end) uv27
from
        tmp_user_app_profile_t2 t
     left join
        tmp_user_app_profile_t4 t1
    on t.device_number=t1.device_number
    group by t1.age_range;

select '-- -- -*******************************************************************************************************************-- -- -';

--  select  '手百小程序上下文' app,
--          '常驻省份分布' model,
--          t1.top1_home_province,
--          count(*)
--      from
--          tmp_user_app_profile_t3 t
--       left join
--          tmp_user_app_profile_t4 t1
--      on t.device_number=t1.device_number
--      group by t1.top1_home_province;



--  select  '手百小程序上下文' app,
--          '常驻城市分布' model,
--          t1.top1_home_city,
--          count(*)
--      from
--          tmp_user_app_profile_t3 t
--       left join
--          tmp_user_app_profile_t4 t1
--      on t.device_number=t1.device_number
--      group by t1.top1_home_city;

--  select  '手百小程序上下文' app,
--          '品牌分布' model,
--          t1.factory_desc,
--          count(*)
--      from
--          tmp_user_app_profile_t3 t
--       left join
--          tmp_user_app_profile_t4 t1
--      on t.device_number=t1.device_number
--      group by t1.factory_desc;

--  select  '手百小程序上下文' app,
--          '证件归属地分布' model,
--          t1.cert_prov,
--          count(*)
--      from
--          tmp_user_app_profile_t3 t
--       left join
--          tmp_user_app_profile_t4 t1
--      on t.device_number=t1.device_number
--      group by t1.cert_prov;

--  select  '手百小程序上下文' app,
--          '性别分布' model,
--          t1.cust_sex,
--          count(*)
--      from
--          tmp_user_app_profile_t3 t
--       left join
--          tmp_user_app_profile_t4 t1
--      on t.device_number=t1.device_number
--      group by t1.cust_sex;


--  select  '手百小程序上下文' app,
--          '消费水平分布' model,
--          t1.cost_level,
--          count(*)
--      from
--          tmp_user_app_profile_t3 t
--       left join
--          tmp_user_app_profile_t4 t1
--      on t.device_number=t1.device_number
--      group by t1.cost_level;


--  select  '手百小程序上下文' app,
--          '年龄分布' model,
--          t1.age_range,
--          count(*)
--      from
--          tmp_user_app_profile_t3 t
--       left join
--          tmp_user_app_profile_t4 t1
--      on t.device_number=t1.device_number
--      group by t1.age_range;


drop table if exists tmp_user_app_profile_t1;
drop table if exists tmp_user_app_profile_t2;
drop table if exists tmp_user_app_profile_t3;
drop table if exists tmp_user_app_profile_t4;
