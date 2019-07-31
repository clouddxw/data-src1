select '头条未加密部分各模块周统计1' model,
       '周id' weekid,
       'uv_头条-搜索' uv1,
       'uv_头条点击caaa' uv2,
       'pv_头条-搜索' pv1,
       'pv_头条点击caaa' pv2
union all
select '头条未加密部分各模块周统计1' model,
        weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd'))) weekid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2
   from 
    (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag1,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag2
          from  user_action_tag_d
          where  pt_days>='20190304' and pt_days<'20190401'
            and  instr(url_host,'.snssdk.com')>0
        group by pt_days,
                 device_number,
                 hh) a
      group by weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd')));


select '头条未加密部分各模块周统计1' model,
        weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd'))) weekid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2
   from 
    (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag1,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag2
          from  user_action_tag_d
          where  pt_days>='20190401' and pt_days<'20190506'
            and  instr(url_host,'.snssdk.com')>0
        group by pt_days,
                 device_number,
                 hh) a
      group by weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd')));




select '头条未加密部分各模块月统计1' model,
       '月份' monthid,
       'uv_头条-搜索' uv1,
       'uv_头条点击caaa' uv2,
       'pv_头条-搜索' pv1,
       'pv_头条点击caaa' pv2
union all
select '头条未加密部分各模块月统计1' model,
       '201903' monthid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2
   from 
    (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag1,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag2
          from  user_action_tag_d
          where  pt_days>='20190301' and pt_days<'20190401'
            and  instr(url_host,'.snssdk.com')>0
        group by pt_days,
                 device_number,
                 hh) a;

select '头条未加密部分各模块月统计1' model,
       '201904' monthid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2
   from 
    (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag1,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag2
          from  user_action_tag_d
          where  pt_days>='20190401' and pt_days<'20190501'
            and  instr(url_host,'.snssdk.com')>0
        group by pt_days,
                 device_number,
                 hh) a;









select  '手机百度各模块周统计' model,
        '周id' weekid,
        'uv_信息流-feed&f0' uv1,
        'uv_信息流-ext' uv3,
        'uv_搜索' uv4,
        'uv_视频' uv5,
        'uv_百度小说' uv6,
        'uv_语音' uv7,
        'uv_小程序hm' uv8,
        'uv_手百小程序smart' uv9,
        'uv_手百小程序上下文' uv10,
        'uv_手百小程序bdbus' uv11,
        'pv_信息流-feed&f0' pv1,
        'pv_信息流-ext' pv3,
        'pv_搜索' pv4,
        'pv_视频' pv5,
        'pv_百度小说' pv6,
        'pv_语音' pv7,
        'pv_小程序hm' pv8,
        'pv_手百小程序smart' pv9,
        'pv_手百小程序上下文' pv10,
        'pv_手百小程序bdbus' pv11
union all
select '手机百度各模块周统计' model,
        weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd'))) weekid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 and tag11=1 and tag12=1 then device_number else null end)) uv10,
        count(distinct(case when tag10=1 then device_number else null end)) uv11,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 and tag11=1 and tag12=1 then 1 else 0 end) pv10,
        sum(case when tag10=1 then 1 else 0 end) pv11       
from 
        (select  pt_days,
                device_number,
                hh,
                max(case when url_host = 'feed.baidu.com' or url_host like 'f%.baidu.com' or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag1,
                max(case when url_host in ('feed.baidu.com','ext.baidu.com') or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag3,
                max(case when url_host rlike '^(sp)([0-9]*)(\.baidu.com)$' then 1 else 0 end) tag4,
                max(case when url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag5,
                max(case when url_host='boxnovel.baidu.com' then 1 else 0 end) tag6,
                max(case when url_host='voice.baidu.com' and actiontag>3  then 1 else 0 end) tag7,
                max(case when url_host='hmma.baidu.com' and actiontag>3 then 1 else 0 end) tag8,
                max(case when instr(url_host,'.smartapps.cn')>0 then 1 else 0 end) tag9,
                max(case when url_host='bdbus-turbonet.baidu.com' then 1 else 0 end) tag10,
                max(case when url_host='hmma.baidu.com' then 1 else 0 end) tag11,
                max(case when url_host='mbd.baidu.com' then 1 else 0 end) tag12
        from  user_action_tag_d
        where pt_days>='20190304' and pt_days<'20190401'  
        and (instr(url_host,'baidu.com')>0 or instr(url_host,'bdstatic.com')>0 or instr(url_host,'.smartapps.cn')>0 )
                group BY
                pt_days,
                device_number,
                hh) a
group by weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd')));


select '手机百度各模块周统计' model,
        weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd'))) weekid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 and tag11=1 and tag12=1 then device_number else null end)) uv10,
        count(distinct(case when tag10=1 then device_number else null end)) uv11,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 and tag11=1 and tag12=1 then 1 else 0 end) pv10,
        sum(case when tag10=1 then 1 else 0 end) pv11       
from 
        (select pt_days,
                device_number,
                hh,
                max(case when url_host = 'feed.baidu.com' or url_host like 'f%.baidu.com' or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag1,
                max(case when url_host in ('feed.baidu.com','ext.baidu.com') or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag3,
                max(case when url_host rlike '^(sp)([0-9]*)(\.baidu.com)$' then 1 else 0 end) tag4,
                max(case when url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag5,
                max(case when url_host='boxnovel.baidu.com' then 1 else 0 end) tag6,
                max(case when url_host='voice.baidu.com' and actiontag>3  then 1 else 0 end) tag7,
                max(case when url_host='hmma.baidu.com' and actiontag>3 then 1 else 0 end) tag8,
                max(case when instr(url_host,'.smartapps.cn')>0 then 1 else 0 end) tag9,
                max(case when url_host='bdbus-turbonet.baidu.com' then 1 else 0 end) tag10,
                max(case when url_host='hmma.baidu.com' then 1 else 0 end) tag11,
                max(case when url_host='mbd.baidu.com' then 1 else 0 end) tag12
        from  user_action_tag_d
          where  pt_days>='20190401' and pt_days<'20190506'
            and (instr(url_host,'baidu.com')>0 or instr(url_host,'bdstatic.com')>0 or instr(url_host,'.smartapps.cn')>0 )
           group BY
                pt_days,
                device_number,
                hh) a
group by weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd')));




select  '手机百度各模块月统计' model,
        '月份' monthid,
        'uv_信息流-feed&f0' uv1,
        'uv_信息流-ext' uv3,
        'uv_搜索' uv4,
        'uv_视频' uv5,
        'uv_百度小说' uv6,
        'uv_语音' uv7,
        'uv_小程序hm' uv8,
        'uv_手百小程序smart' uv9,
        'uv_手百小程序上下文' uv10,
        'uv_手百小程序bdbus' uv11,
        'pv_信息流-feed&f0' pv1,
        'pv_信息流-ext' pv3,
        'pv_搜索' pv4,
        'pv_视频' pv5,
        'pv_百度小说' pv6,
        'pv_语音' pv7,
        'pv_小程序hm' pv8,
        'pv_手百小程序smart' pv9,
        'pv_手百小程序上下文' pv10,
        'pv_手百小程序bdbus' pv11
union all
select '手机百度各模块月统计' model,
       '201903' monthid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 and tag11=1 and tag12=1 then device_number else null end)) uv10,
        count(distinct(case when tag10=1 then device_number else null end)) uv11,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 and tag11=1 and tag12=1 then 1 else 0 end) pv10,
        sum(case when tag10=1 then 1 else 0 end) pv11       
from 
        (select  pt_days,
                device_number,
                hh,
                max(case when url_host = 'feed.baidu.com' or url_host like 'f%.baidu.com' or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag1,
                max(case when url_host in ('feed.baidu.com','ext.baidu.com') or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag3,
                max(case when url_host rlike '^(sp)([0-9]*)(\.baidu.com)$' then 1 else 0 end) tag4,
                max(case when url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag5,
                max(case when url_host='boxnovel.baidu.com' then 1 else 0 end) tag6,
                max(case when url_host='voice.baidu.com' and actiontag>3  then 1 else 0 end) tag7,
                max(case when url_host='hmma.baidu.com' and actiontag>3 then 1 else 0 end) tag8,
                max(case when instr(url_host,'.smartapps.cn')>0 then 1 else 0 end) tag9,
                max(case when url_host='bdbus-turbonet.baidu.com' then 1 else 0 end) tag10,
                max(case when url_host='hmma.baidu.com' then 1 else 0 end) tag11,
                max(case when url_host='mbd.baidu.com' then 1 else 0 end) tag12
        from  user_action_tag_d
        where pt_days>='20190301' and pt_days<'20190401'  
        and (instr(url_host,'baidu.com')>0 or instr(url_host,'bdstatic.com')>0 or instr(url_host,'.smartapps.cn')>0 )
                group BY
                pt_days,
                device_number,
                hh) a;

select '手机百度各模块月统计' model,
        '201904' monthid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 and tag11=1 and tag12=1 then device_number else null end)) uv10,
        count(distinct(case when tag10=1 then device_number else null end)) uv11,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 and tag11=1 and tag12=1 then 1 else 0 end) pv10,
        sum(case when tag10=1 then 1 else 0 end) pv11       
from 
        (select  pt_days,
                device_number,
                hh,
                max(case when url_host = 'feed.baidu.com' or url_host like 'f%.baidu.com' or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag1,
                max(case when url_host in ('feed.baidu.com','ext.baidu.com') or url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag3,
                max(case when url_host rlike '^(sp)([0-9]*)(\.baidu.com)$' then 1 else 0 end) tag4,
                max(case when url_host rlike '^(vd)([0-9]*)(\.bdstatic.com)$' then 1 else 0 end) tag5,
                max(case when url_host='boxnovel.baidu.com' then 1 else 0 end) tag6,
                max(case when url_host='voice.baidu.com' and actiontag>3  then 1 else 0 end) tag7,
                max(case when url_host='hmma.baidu.com' and actiontag>3 then 1 else 0 end) tag8,
                max(case when instr(url_host,'.smartapps.cn')>0 then 1 else 0 end) tag9,
                max(case when url_host='bdbus-turbonet.baidu.com' then 1 else 0 end) tag10,
                max(case when url_host='hmma.baidu.com' then 1 else 0 end) tag11,
                max(case when url_host='mbd.baidu.com' then 1 else 0 end) tag12
        from  user_action_tag_d
          where  pt_days>='20190401' and pt_days<'20190501'
            and (instr(url_host,'baidu.com')>0 or instr(url_host,'bdstatic.com')>0 or instr(url_host,'.smartapps.cn')>0 )
           group BY
                pt_days,
                device_number,
                hh) a;


select  'UC各模块周统计' model,
        '周id' weekid,
        'uv_UC-feed流' uv1,
        'uv_小说book' uv2,
        'uv_小说shuqi' uv3,
        'uv_UC-小说fiction' uv4,
        'uv_视频点击' uv5,
        'uv_搜索' uv6,
        'uv_小视频' uv7,
        'uv_UMS视频' uv8,
        'uv_搜索input' uv9,
        'uv_搜索合计' uv10,       
        'pv_UC-feed流' pv1,
        'pv_小说book' pv2,
        'pv_小说shuqi' pv3,
        'pv_UC-小说fiction' pv4,
        'pv_视频点击' pv5,
        'pv_搜索' pv6,
        'pv_小视频' pv7,
        'pv_UMS视频' pv8,
        'pv_搜索input' pv9,
        'pv_搜索合计' pv10              
union all
select 'UC各模块周统计' model,
        weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd'))) weekid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 then device_number else null end)) uv10,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 then 1 else 0 end) pv10
    from 
        (select pt_days,
                device_number,
                hh,
                max(case when url_host = 'iflow.uczzd.cn'  then 1 else 0 end) tag1,
                max(case when url_host='track.uc.cn' and instr(urltag,'book')>0 then 1 else 0 end) tag2,
                max(case when instr(url_host,'shuqireader.com')>0 then 1 else 0 end) tag3,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'fiction')>0 then 1 else 0 end) tag4,
                max(case when url_host='iflow.uczzd.cn' and instr(urltag,'video')>0 then 1 else 0 end) tag5,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'search')>0 then 1 else 0 end) tag6,
                max(case when url_host='img.v.uc.cn'  then 1 else 0 end) tag7,
                max(case when url_host='video.ums.uc.cn' then 1 else 0 end) tag8,
                max(case when url_host='sugs.m.sm.cn' and instr(urltag,'ucinput')>0 then 1 else 0 end) tag9,
                max(case when url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') then 1 else 0 end) tag10
            from  user_action_tag_d
            where pt_days>='20190304' and pt_days<'20190401'  
              and (instr(url_host,'.uczzd.cn')>0 or instr(url_host,'.uc.cn')>0 or url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') or instr(url_host,'.shuqireader.com')>0)
              group BY
                pt_days,
                device_number,
                hh) a
        group by weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd')));


select 'UC各模块周统计' model,
        weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd'))) weekid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 then device_number else null end)) uv10,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 then 1 else 0 end) pv10
    from 
        (select pt_days,
                device_number,
                hh,
                max(case when url_host = 'iflow.uczzd.cn'  then 1 else 0 end) tag1,
                max(case when url_host='track.uc.cn' and instr(urltag,'book')>0 then 1 else 0 end) tag2,
                max(case when instr(url_host,'shuqireader.com')>0 then 1 else 0 end) tag3,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'fiction')>0 then 1 else 0 end) tag4,
                max(case when url_host='iflow.uczzd.cn' and instr(urltag,'video')>0 then 1 else 0 end) tag5,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'search')>0 then 1 else 0 end) tag6,
                max(case when url_host='img.v.uc.cn'  then 1 else 0 end) tag7,
                max(case when url_host='video.ums.uc.cn' then 1 else 0 end) tag8,
                max(case when url_host='sugs.m.sm.cn' and instr(urltag,'ucinput')>0 then 1 else 0 end) tag9,
                max(case when url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') then 1 else 0 end) tag10
            from  user_action_tag_d
            where pt_days>='20190401' and pt_days<'20190506'  
              and (instr(url_host,'.uczzd.cn')>0 or instr(url_host,'.uc.cn')>0 or url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') or instr(url_host,'.shuqireader.com')>0)
              group BY
                pt_days,
                device_number,
                hh) a
        group by weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd')));


select  'UC各模块月统计' model,
        '月份' monthid,
        'uv_UC-feed流' uv1,
        'uv_小说book' uv2,
        'uv_小说shuqi' uv3,
        'uv_UC-小说fiction' uv4,
        'uv_视频点击' uv5,
        'uv_搜索' uv6,
        'uv_小视频' uv7,
        'uv_UMS视频' uv8,
        'uv_搜索input' uv9,
        'uv_搜索合计' uv10,       
        'pv_UC-feed流' pv1,
        'pv_小说book' pv2,
        'pv_小说shuqi' pv3,
        'pv_UC-小说fiction' pv4,
        'pv_视频点击' pv5,
        'pv_搜索' pv6,
        'pv_小视频' pv7,
        'pv_UMS视频' pv8,
        'pv_搜索input' pv9,
        'pv_搜索合计' pv10              
union all
select  'UC各模块月统计' model,
        '201903' monthid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 then device_number else null end)) uv10,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 then 1 else 0 end) pv10
    from 
        (select pt_days,
                device_number,
                hh,
                max(case when url_host = 'iflow.uczzd.cn'  then 1 else 0 end) tag1,
                max(case when url_host='track.uc.cn' and instr(urltag,'book')>0 then 1 else 0 end) tag2,
                max(case when instr(url_host,'shuqireader.com')>0 then 1 else 0 end) tag3,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'fiction')>0 then 1 else 0 end) tag4,
                max(case when url_host='iflow.uczzd.cn' and instr(urltag,'video')>0 then 1 else 0 end) tag5,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'search')>0 then 1 else 0 end) tag6,
                max(case when url_host='img.v.uc.cn'  then 1 else 0 end) tag7,
                max(case when url_host='video.ums.uc.cn' then 1 else 0 end) tag8,
                max(case when url_host='sugs.m.sm.cn' and instr(urltag,'ucinput')>0 then 1 else 0 end) tag9,
                max(case when url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') then 1 else 0 end) tag10
            from  user_action_tag_d
            where pt_days>='20190301' and pt_days<'20190401'  
              and (instr(url_host,'.uczzd.cn')>0 or instr(url_host,'.uc.cn')>0 or url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') or instr(url_host,'.shuqireader.com')>0)
              group BY
                pt_days,
                device_number,
                hh) a;

select  'UC各模块月统计' model,
        '201904' monthid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 then device_number else null end)) uv10,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 then 1 else 0 end) pv10
    from 
        (select pt_days,
                device_number,
                hh,
                max(case when url_host = 'iflow.uczzd.cn'  then 1 else 0 end) tag1,
                max(case when url_host='track.uc.cn' and instr(urltag,'book')>0 then 1 else 0 end) tag2,
                max(case when instr(url_host,'shuqireader.com')>0 then 1 else 0 end) tag3,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'fiction')>0 then 1 else 0 end) tag4,
                max(case when url_host='iflow.uczzd.cn' and instr(urltag,'video')>0 then 1 else 0 end) tag5,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'search')>0 then 1 else 0 end) tag6,
                max(case when url_host='img.v.uc.cn'  then 1 else 0 end) tag7,
                max(case when url_host='video.ums.uc.cn' then 1 else 0 end) tag8,
                max(case when url_host='sugs.m.sm.cn' and instr(urltag,'ucinput')>0 then 1 else 0 end) tag9,
                max(case when url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') then 1 else 0 end) tag10
            from  user_action_tag_d
            where pt_days>='20190401' and pt_days<'20190501'  
              and (instr(url_host,'.uczzd.cn')>0 or instr(url_host,'.uc.cn')>0 or url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') or instr(url_host,'.shuqireader.com')>0)
              group BY
                pt_days,
                device_number,
                hh) a;


select '头条未加密部分各模块周统计' model,
       '周ID' weekid,
       'uv_头条-搜索' uv1,
       'uv_头条-小程序' uv2,
       'uv_头条-短视频category' uv3,
       'uv_头条-短视频action' uv4,
       'uv_头条-短视频合并ca' uv5,
       'uv_头条-短视频openapi' uv6,
       'uv_头条问答s' uv7,
       'uv_头条-小视频合并' uv8,
       'uv_头条点击日活caaa' uv9,
       'uv_头条-小mcs' uv10,
       'uv_头条-microapp' uv11,
       'uv_头条-我的页面' uv12,
       'pv_头条-搜索' pv1,
       'pv_头条-小程序' pv2,
       'pv_头条-短视频category' pv3,
       'pv_头条-短视频action' pv4,
       'pv_头条-短视频合并ca' pv5,
       'pv_头条-短视频openapi' pv6,
       'pv_头条问答s' pv7,
       'pv_头条-小视频合并' pv8,
       'pv_头条点击日活caaa' pv9,
       'pv_头条-小mcs' pv10,
       'pv_头条-microapp' pv11,
       'pv_头条-我的页面' pv12
union all
select '头条未加密部分各模块周统计' model,
        weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd'))) weekid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 then device_number else null end)) uv10,
        count(distinct(case when tag11=1 then device_number else null end)) uv11,
        count(distinct(case when tag12=1 then device_number else null end)) uv12,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 then 1 else 0 end) pv10,
        sum(case when tag11=1 then 1 else 0 end) pv11,
        sum(case when tag12=1 then 1 else 0 end) pv12
   from 
      (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag1,
            max(case when url_host='developer.toutiao.com'  and actiontag>2 then 1 else 0 end) tag2,
            max(case when instr(urltag,'category=video')>0 then 1 else 0 end) tag3,
            max(case when instr(urltag,'action=GetPlayInfo')>0 then 1 else 0 end) tag4,
            max(case when instr(urltag,'category=video')>0 or instr(urltag,'action=GetPlayInfo')>0  then 1 else 0 end) tag5,
            max(case when instr(urltag,'video/openapi')>0 then 1 else 0 end) tag6,
            max(case when instr(urltag,'answer/detail')>0 then 1 else 0 end) tag7,
            max(case when instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0  then 1 else 0 end) tag8,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag9,
            max(case when url_host='mcs.snssdk.com' then 1 else 0 end) tag10,
            max(case when url_host='microapp.bytedance.com' then 1 else 0 end) tag11,
            max(case when instr(urltag,'user/profile')>0 then 1 else 0 end) tag12
          from  user_action_tag_d
          where  pt_days>='20190304' and pt_days<'20190401'
            and (url_host like '%.snssdk.com'
                  or url_host in ('developer.toutiao.com','microapp.bytedance.com'))
        group by pt_days,
                device_number,
                hh) a
      group by weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd')));


select '头条未加密部分各模块周统计' model,
        weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd'))) weekid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 then device_number else null end)) uv10,
        count(distinct(case when tag11=1 then device_number else null end)) uv11,
        count(distinct(case when tag12=1 then device_number else null end)) uv12,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 then 1 else 0 end) pv10,
        sum(case when tag11=1 then 1 else 0 end) pv11,
        sum(case when tag12=1 then 1 else 0 end) pv12
   from 
      (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag1,
            max(case when url_host='developer.toutiao.com'  and actiontag>2 then 1 else 0 end) tag2,
            max(case when instr(urltag,'category=video')>0 then 1 else 0 end) tag3,
            max(case when instr(urltag,'action=GetPlayInfo')>0 then 1 else 0 end) tag4,
            max(case when instr(urltag,'category=video')>0 or instr(urltag,'action=GetPlayInfo')>0  then 1 else 0 end) tag5,
            max(case when instr(urltag,'video/openapi')>0 then 1 else 0 end) tag6,
            max(case when instr(urltag,'answer/detail')>0 then 1 else 0 end) tag7,
            max(case when instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0  then 1 else 0 end) tag8,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag9,
            max(case when url_host='mcs.snssdk.com' then 1 else 0 end) tag10,
            max(case when url_host='microapp.bytedance.com' then 1 else 0 end) tag11,
            max(case when instr(urltag,'user/profile')>0 then 1 else 0 end) tag12
          from  user_action_tag_d
          where  pt_days>='20190401' and pt_days<'20190506'
            and (url_host like '%.snssdk.com'
                  or url_host in ('developer.toutiao.com','microapp.bytedance.com'))
        group by pt_days,
                device_number,
                hh) a
      group by weekofyear(to_date(from_unixtime(unix_timestamp(pt_days,'yyyymmdd'),'yyyy-mm-dd')));


select '头条未加密部分各模块月统计' model,
       '月份' monthid,
       'uv_头条-搜索' uv1,
       'uv_头条-小程序' uv2,
       'uv_头条-短视频category' uv3,
       'uv_头条-短视频action' uv4,
       'uv_头条-短视频合并ca' uv5,
       'uv_头条-短视频openapi' uv6,
       'uv_头条问答s' uv7,
       'uv_头条-小视频合并' uv8,
       'uv_头条点击日活caaa' uv9,
       'uv_头条-小mcs' uv10,
       'uv_头条-microapp' uv11,
       'uv_头条-我的页面' uv12,
       'pv_头条-搜索' pv1,
       'pv_头条-小程序' pv2,
       'pv_头条-短视频category' pv3,
       'pv_头条-短视频action' pv4,
       'pv_头条-短视频合并ca' pv5,
       'pv_头条-短视频openapi' pv6,
       'pv_头条问答s' pv7,
       'pv_头条-小视频合并' pv8,
       'pv_头条点击日活caaa' pv9,
       'pv_头条-小mcs' pv10,
       'pv_头条-microapp' pv11,
       'pv_头条-我的页面' pv12
union all
select '头条未加密部分各模块月统计-3月' model,
        '201903' monthid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 then device_number else null end)) uv10,
        count(distinct(case when tag11=1 then device_number else null end)) uv11,
        count(distinct(case when tag12=1 then device_number else null end)) uv12,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 then 1 else 0 end) pv10,
        sum(case when tag11=1 then 1 else 0 end) pv11,
        sum(case when tag12=1 then 1 else 0 end) pv12
   from 
      (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag1,
            max(case when url_host='developer.toutiao.com'  and actiontag>2 then 1 else 0 end) tag2,
            max(case when instr(urltag,'category=video')>0 then 1 else 0 end) tag3,
            max(case when instr(urltag,'action=GetPlayInfo')>0 then 1 else 0 end) tag4,
            max(case when instr(urltag,'category=video')>0 or instr(urltag,'action=GetPlayInfo')>0  then 1 else 0 end) tag5,
            max(case when instr(urltag,'video/openapi')>0 then 1 else 0 end) tag6,
            max(case when instr(urltag,'answer/detail')>0 then 1 else 0 end) tag7,
            max(case when instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0  then 1 else 0 end) tag8,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag9,
            max(case when url_host='mcs.snssdk.com' then 1 else 0 end) tag10,
            max(case when url_host='microapp.bytedance.com' then 1 else 0 end) tag11,
            max(case when instr(urltag,'user/profile')>0 then 1 else 0 end) tag12
          from  user_action_tag_d
          where  pt_days>='20190301' and pt_days<'20190401'
            and (url_host like '%.snssdk.com'
                  or url_host in ('developer.toutiao.com','microapp.bytedance.com'))
        group by pt_days,
                device_number,
                hh) a;

select '头条未加密部分各模块月统计' model,
       '201904' monthid,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1 then device_number else null end)) uv10,
        count(distinct(case when tag11=1 then device_number else null end)) uv11,
        count(distinct(case when tag12=1 then device_number else null end)) uv12,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7,
        sum(case when tag8=1 then 1 else 0 end) pv8,
        sum(case when tag9=1 then 1 else 0 end) pv9,
        sum(case when tag10=1 then 1 else 0 end) pv10,
        sum(case when tag11=1 then 1 else 0 end) pv11,
        sum(case when tag12=1 then 1 else 0 end) pv12
   from 
      (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag1,
            max(case when url_host='developer.toutiao.com'  and actiontag>2 then 1 else 0 end) tag2,
            max(case when instr(urltag,'category=video')>0 then 1 else 0 end) tag3,
            max(case when instr(urltag,'action=GetPlayInfo')>0 then 1 else 0 end) tag4,
            max(case when instr(urltag,'category=video')>0 or instr(urltag,'action=GetPlayInfo')>0  then 1 else 0 end) tag5,
            max(case when instr(urltag,'video/openapi')>0 then 1 else 0 end) tag6,
            max(case when instr(urltag,'answer/detail')>0 then 1 else 0 end) tag7,
            max(case when instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0  then 1 else 0 end) tag8,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag9,
            max(case when url_host='mcs.snssdk.com' then 1 else 0 end) tag10,
            max(case when url_host='microapp.bytedance.com' then 1 else 0 end) tag11,
            max(case when instr(urltag,'user/profile')>0 then 1 else 0 end) tag12
          from  user_action_tag_d
          where  pt_days>='20190401' and pt_days<'20190501'
            and (url_host like '%.snssdk.com'
                  or url_host in ('developer.toutiao.com','microapp.bytedance.com'))
        group by pt_days,
                 device_number,
                 hh) a;