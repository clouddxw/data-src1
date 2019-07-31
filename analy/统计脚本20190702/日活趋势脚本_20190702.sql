
select '各app日活数据' model,
       pt_days,
       appname,
       sum(pv) pv,
       count(distinct device_number) uv
 from  user_action_app_d 
 where pt_days>='20190610' and pt_days<'20190701'
group by pt_days,
         appname;
select '-- -- -*******************************************************************************************************************-- -- -';

select '各app日活数据-剔除流量套餐' model,
       pt_days,
       appname,
       sum(pv) pv,
       count(distinct t1.device_number) uv
   from 
        (select *
        from  user_action_app_d 
        where pt_days>='20190610' and pt_days<'20190701')  t1
     INNER join 
        (select device_number 
         from zba_dwa.dwa_v_m_cus_nm_user_info b  
         where b.month_id='201905'
         and b.product_class not in ('90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t2
 on t1.device_number=t2.device_number
group by pt_days,
         appname;

select '-- -- -*******************************************************************************************************************-- -- -';

select '新闻行业日活数据' model,
       pt_days,
       sum(pv) pv,
       count(distinct device_number)
      from  user_action_app_d
     where pt_days>='20190610' and pt_days<'20190701'
     and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')
     group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

select '新闻行业日活数据-剔除流量套餐' model,
       pt_days,
       sum(pv) pv,
       count(distinct t1.device_number)
   from 
        (select *
        from  user_action_app_d 
        where pt_days>='20190610' and pt_days<'20190701'
         and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng'))  t1
     INNER join 
        (select device_number 
         from zba_dwa.dwa_v_m_cus_nm_user_info b  
         where b.month_id='201905'
         and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t2
 on t1.device_number=t2.device_number
 group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

select '音乐行业日活数据' model,
       pt_days,
       sum(pv) pv,
       count(distinct device_number)
      from  user_action_app_d
     where pt_days>='20190610' and pt_days<'20190701'
       and appname in ('qq_music','kuwo','kugou','wangyi_music')
       group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

select 
        '酷我各模块统计' model,
        '日期' pt_days,
        'uv_酷我电台' uv1,
        'uv_酷我会员' uv2,
        'uv_酷我搜索' uv3,
        'pv_酷我电台' pv1,
        'pv_酷我会员' pv2,
        'pv_酷我搜索' pv3
union all
select '酷我各模块统计' model,
        pt_days,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3
    from 
        (select pt_days,
                device_number,
                hh,
                max(case when instr(urltag,'fm/category')>0 or instr(urltag,'get_jm_info')>0 or instr(urltag,'fmradio')>0 then 1 else 0 end) tag1,
                max(case when instr(urltag,'isVip=1')>0 then 1 else 0 end) tag2,
                max(case when url_host='search.kuwo.cn' then 1 else 0 end) tag3
            from  user_action_tag_d
            where pt_days>='20190610' and pt_days<'20190701' 
              and instr(url_host,'.kuwo.cn')>0
              group BY
                pt_days,
                device_number,
                hh) a
     group by pt_days;
      
select '-- -- -*******************************************************************************************************************-- -- -';


--  酷狗其他模块统计
select  '酷狗各模块统计' model,
        '日期' pt_days,
        'uv_电台' uv1,
        'uv_酷狗号' uv2,
        'uv_猜你喜欢' uv3,
        'uv_视频直播' uv4,
        'uv_K歌-KTV' uv5,
        'uv_K歌-直播' uv6,
        'uv_K歌-录唱' uv7,
        'uv_短视频' uv8,
        'uv_酷狗每日推' uv9,
        'uv_酷狗会员' uv10,
        'pv_电台' pv1,
        'pv_酷狗号' pv2,
        'pv_猜你喜欢' pv3,
        'pv_视频直播' pv4,
        'pv_K歌-KTV' pv5,
        'pv_K歌-直播' pv6,
        'pv_K歌-录唱' pv7,
        'pv_短视频' pv8,
        'pv_酷狗每日推' pv9,
        'pv_酷狗会员' pv10
union all
select '酷狗各模块统计' model,
        pt_days,
        count(distinct(case when tag1=1  then device_number else null end)) uv1,
        count(distinct(case when tag2=1  then device_number else null end)) uv2,
        count(distinct(case when tag3=1  then device_number else null end)) uv3,
        count(distinct(case when tag4=1  then device_number else null end)) uv4,
        count(distinct(case when tag5=1  then device_number else null end)) uv5,
        count(distinct(case when tag6=1  then device_number else null end)) uv6,
        count(distinct(case when tag7=1  then device_number else null end)) uv7,
        count(distinct(case when tag8=1  then device_number else null end)) uv8,
        count(distinct(case when tag9=1  then device_number else null end)) uv9,
        count(distinct(case when tag10=1  then device_number else null end)) uv10,
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
                max(case when url_host='fm.service.kugou.com' then 1 else 0 end) tag1,
                max(case when url_host='kuhaoapi.kugou.com' then 1 else 0 end) tag2,
                max(case when url_host='persnfm.service.kugou.com' and instr(urltag,'personal_recommend')>0 then 1 else 0 end) tag3,
                max(case when url_host='acshow.kugou.com' and (instr(urltag,'roomId')>0 or instr(urltag,'getEnterRoomInfo')>0) then 1 else 0 end) tag4,
                max(case when url_host='acsing.kugou.com' and instr(urltag,'ktv_room')>0 then 1 else 0 end) tag5,
                max(case when url_host='acsing.kugou.com' and instr(urltag,'liveroom')>0 then 1 else 0 end) tag6,
                max(case when url_host='acsing.kugou.com' and instr(urltag,'record.do')>0 then 1 else 0 end) tag7,
                max(case when url_host='acsing.kugou.com' and instr(urltag,'video/recordListen')>0 then 1 else 0 end) tag8,
                max(case when url_host='everydayrec.service.kugou.com' then 1 else 0 end) tag9,
                max(case when (url_host in ('ip2.kugou.com','newsongretry.kugou.com','ip.kugou.com') and instr(urltag,'vip_type=6')>0 ) 
                       or (url_host='ads.service.kugou.com' and instr(urltag,'isVip=1')>0 ) then 1 else 0 end) tag10
            from  user_action_tag_d
            where pt_days>='20190610' and pt_days<'20190701'
              and instr(url_host,'kugou.com')>0
              group BY
                pt_days,
                device_number,
                hh) a
        group by pt_days;


select '-- -- -*******************************************************************************************************************-- -- -';


select 
        '网易云各模块' app,
        '日期' pt_days,
        'uv_下载会员' uv1,
        'uv_商城store' uv2,
        'uv_每日推荐' uv3,
        'uv_查看评论comment' uv4,
        'uv_音乐云盘' uv5,
        'uv_我的消息-私信' uv6,
        'uv_我的消息-评论' uv7,
        'uv_我的消息-@我' uv8,
        'uv_我的消息-通知' uv9,
        'uv_评论detail' uv10,
        'uv_评论musiciansaid' uv11,
        'uv_关注列表' uv12,
        'uv_歌单-广场&推荐' uv13,
        'uv_发现-排行榜' uv14,
        'uv_发现-电台' uv15,
        'uv_发现-电台-付费精品' uv16,
        'uv_视频页卡' uv17,
        'uv_我的页卡' uv18,
        'uv_朋友页卡' uv19,
        'uv_我的-私人FM' uv20,
        'uv_我的收藏' uv21,
        'uv_我的电台' uv22,
        'uv_查看个人歌单' uv23,
        'uv_播放歌曲' uv24,
        'uv_喜欢歌曲' uv25,
        'uv_我的数字付费专辑' uv26,
        'uv_新碟上架latest' uv27,
        'uv_新碟上架new' uv28,
        'uv_听歌识曲' uv29,
        'uv_哼唱识曲' uv30,
        'uv_观看MV' uv31,
        'uv_签到' uv32,
        'uv_搜索' uv33,
        'pv_下载会员' pv1,
        'pv_商城store' pv2,
        'pv_每日推荐' pv3,
        'pv_查看评论comment' pv4,
        'pv_音乐云盘' pv5,
        'pv_我的消息-私信' pv6,
        'pv_我的消息-评论' pv7,
        'pv_我的消息-@我' pv8,
        'pv_我的消息-通知' pv9,
        'pv_评论detail' pv10,
        'pv_评论musiciansaid' pv11,
        'pv_关注列表' pv12,
        'pv_歌单-广场&推荐' pv13,
        'pv_发现-排行榜' pv14,
        'pv_发现-电台' pv15,
        'pv_发现-电台-付费精品' pv16,
        'pv_视频页卡' pv17,
        'pv_我的页卡' pv18,
        'pv_朋友页卡' pv19,
        'pv_我的-私人FM' pv20,
        'pv_我的收藏' pv21,
        'pv_我的电台' pv22,
        'pv_查看个人歌单' pv23,
        'pv_播放歌曲' pv24,
        'pv_喜欢歌曲' pv25,
        'pv_我的数字付费专辑' pv26,
        'pv_新碟上架latest' pv27,
        'pv_新碟上架new' pv28,
        'pv_听歌识曲' pv29,
        'pv_哼唱识曲' pv30,
        'pv_观看MV' pv31,
        'pv_签到' pv32,
        'pv_搜索' pv33
union all
select  '网易云各模块' app,
        pt_days,
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
        count(distinct(case when tag13=1 then device_number else null end)) uv13,
        count(distinct(case when tag14=1 then device_number else null end)) uv14,
        count(distinct(case when tag15=1 then device_number else null end)) uv15,
        count(distinct(case when tag16=1 then device_number else null end)) uv16,
        count(distinct(case when tag17=1 then device_number else null end)) uv17,
        count(distinct(case when tag18=1 then device_number else null end)) uv18,
        count(distinct(case when tag19=1 then device_number else null end)) uv19,
        count(distinct(case when tag20=1 then device_number else null end)) uv20,
        count(distinct(case when tag21=1 then device_number else null end)) uv21,
        count(distinct(case when tag22=1 then device_number else null end)) uv22,
        count(distinct(case when tag23=1 then device_number else null end)) uv23,
        count(distinct(case when tag24=1 then device_number else null end)) uv24,
        count(distinct(case when tag25=1 then device_number else null end)) uv25,
        count(distinct(case when tag26=1 then device_number else null end)) uv26,
        count(distinct(case when tag27=1 then device_number else null end)) uv27,
        count(distinct(case when tag28=1 then device_number else null end)) uv28,
        count(distinct(case when tag29=1 then device_number else null end)) uv29,
        count(distinct(case when tag30=1 then device_number else null end)) uv30,
        count(distinct(case when tag31=1 then device_number else null end)) uv31,
        count(distinct(case when tag32=1 then device_number else null end)) uv32,
        count(distinct(case when tag33=1 then device_number else null end)) uv33,
        sum(case when tag1=1 then 1 else 0 end)  pv1,
        sum(case when tag2=1 then 1 else 0 end)  pv2,
        sum(case when tag3=1 then 1 else 0 end)  pv3,
        sum(case when tag4=1 then 1 else 0 end)  pv4,
        sum(case when tag5=1 then 1 else 0 end)  pv5,
        sum(case when tag6=1 then 1 else 0 end)  pv6,
        sum(case when tag7=1 then 1 else 0 end)  pv7,
        sum(case when tag8=1 then 1 else 0 end)  pv8,
        sum(case when tag9=1 then 1 else 0 end)  pv9,
        sum(case when tag10=1 then 1 else 0 end)  pv10,
        sum(case when tag11=1 then 1 else 0 end)  pv11,
        sum(case when tag12=1 then 1 else 0 end)  pv12,
        sum(case when tag13=1 then 1 else 0 end)  pv13,
        sum(case when tag14=1 then 1 else 0 end)  pv14,
        sum(case when tag15=1 then 1 else 0 end)  pv15,
        sum(case when tag16=1 then 1 else 0 end)  pv16,
        sum(case when tag17=1 then 1 else 0 end)  pv17,
        sum(case when tag18=1 then 1 else 0 end)  pv18,
        sum(case when tag19=1 then 1 else 0 end)  pv19,
        sum(case when tag20=1 then 1 else 0 end)  pv20,
        sum(case when tag21=1 then 1 else 0 end)  pv21,
        sum(case when tag22=1 then 1 else 0 end)  pv22,
        sum(case when tag23=1 then 1 else 0 end)  pv23,
        sum(case when tag24=1 then 1 else 0 end)  pv24,
        sum(case when tag25=1 then 1 else 0 end)  pv25,
        sum(case when tag26=1 then 1 else 0 end)  pv26,
        sum(case when tag27=1 then 1 else 0 end)  pv27,
        sum(case when tag28=1 then 1 else 0 end)  pv28,
        sum(case when tag29=1 then 1 else 0 end)  pv29,
        sum(case when tag30=1 then 1 else 0 end)  pv30,
        sum(case when tag31=1 then 1 else 0 end)  pv31,
        sum(case when tag32=1 then 1 else 0 end)  pv32,
        sum(case when tag33=1 then 1 else 0 end)  pv33
    from 
        (select 
                pt_days,
                device_number,
                hh,
                max(case when instr(urltag,'enhance/download')>0 then 1 else 0 end)    tag1,
                max(case when instr(urltag,'store')>0 then 1 else 0 end)    tag2,
                max(case when instr(urltag,'recommend')>0 then 1 else 0 end)    tag3,
                max(case when instr(urltag,'comment')>0 then 1 else 0 end)    tag4,
                max(case when instr(urltag,'cloud/get')>0 then 1 else 0 end)    tag5,
                max(case when instr(urltag,'msg/private')>0 then 1 else 0 end)    tag6,
                max(case when instr(urltag,'user/comments')>0 then 1 else 0 end)    tag7,
                max(case when instr(urltag,'forwards')>0 then 1 else 0 end)    tag8,
                max(case when instr(urltag,'msg/notices')>0 then 1 else 0 end)    tag9,
                max(case when instr(urltag,'song/detail')>0 then 1 else 0 end)    tag10,
                max(case when instr(urltag,'comments/musiciansaid')>0 then 1 else 0 end)    tag11,
                max(case when instr(urltag,'getfollows')>0 then 1 else 0 end)    tag12,
                max(case when instr(urltag,'playlist/category')>0 then 1 else 0 end)    tag13,
                max(case when instr(urltag,'playlist/detail/dynamic')>0 then 1 else 0 end)    tag14,
                max(case when instr(urltag,'djradio')>0 then 1 else 0 end)    tag15,
                max(case when instr(urltag,'djradio/home/paygift')>0 then 1 else 0 end)    tag16,
                max(case when instr(urltag,'videotimeline')>0 then 1 else 0 end)    tag17,
                max(case when instr(urltag,'homepage')>0 then 1 else 0 end)    tag18,
                max(case when instr(urltag,'college/user/get')>0 then 1 else 0 end)    tag19,
                max(case when instr(urltag,'radio/get')>0 then 1 else 0 end)    tag20,
                max(case when instr(urltag,'sublist')>0 then 1 else 0 end)    tag21,
                max(case when instr(urltag,'my/radio')>0 then 1 else 0 end)    tag22,
                max(case when instr(urltag,'playlist/detail')>0 then 1 else 0 end)    tag23,
                max(case when instr(urltag,'mlivestream/entrance/playpage')>0 or instr(urltag,'songplay/entry')>0 or instr(urltag,'song/enhance/player')>0 then 1 else 0 end)    tag24,
                max(case when instr(urltag,'song/like')>0 then 1 else 0 end)    tag25,
                max(case when instr(urltag,'digitalAlbum/purchased')>0 then 1 else 0 end)    tag26,
                max(case when instr(urltag,'albumproduct/latest')>0 then 1 else 0 end)    tag27,
                max(case when instr(urltag,'new/albums')>0 then 1 else 0 end)    tag28,
                max(case when instr(urltag,'music/matcher')>0 then 1 else 0 end)    tag29,
                max(case when instr(urltag,'music/matcher/sing')>0 then 1 else 0 end)    tag30,
                max(case when instr(urltag,'play/mv')>0 then 1 else 0 end)    tag31,
                max(case when instr(urltag,'dailyTask')>0 then 1 else 0 end)    tag32,
                max(case when instr(urltag,'keyword')>0 then 1 else 0 end)    tag33
              from  user_action_tag_d
            where pt_days>='20190610' and pt_days<'20190701'
              and url_host='interface.music.163.com'
              group by pt_days,
                      device_number,
                      hh) a
          group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';


-- 喜马拉雅各模块统计
select  '喜马拉雅各模块统计' model,
        '日期' pt_days,
        'uv_会员Category' uv1,
        'uv_会员VIP2' uv2,
        'uv_会员VIP1&3' uv3,
        'uv_直播' uv4,
        'uv_充值' uv5,
        'uv_搜索' uv6,
        'uv_首页焦点图' uv7,
        'uv_VIP频道' uv8,
        'uv_每日必听' uv9,
        'uv_经典必听' uv10,
        'uv_精品' uv11,
        'uv_听头条' uv12,
        'uv_场景电台' uv13,
        'pv_会员Category' pv1,
        'pv_会员VIP2' pv2,
        'pv_会员VIP1&3' pv3,
        'pv_直播' pv4,
        'pv_充值' pv5,
        'pv_搜索' pv6,
        'pv_首页焦点图' pv7,
        'pv_VIP频道' pv8,
        'pv_每日必听' pv9,
        'pv_经典必听' pv10,
        'pv_精品' pv11,
        'pv_听头条' pv12,
        'pv_场景电台' pv13
union all
select '喜马拉雅各模块统计' model,
        pt_days,
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
        count(distinct(case when tag13=1 then device_number else null end)) uv13,
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
        sum(case when tag12=1 then 1 else 0 end) pv12,
        sum(case when tag13=1 then 1 else 0 end) pv13
    from 
        (select  pt_days,
                device_number,
                hh,
                max(case when instr(urltag,'vipCategory')>0 then 1 else 0 end) tag1,
                max(case when instr(urltag,'vipStatus=2')>0 then 1 else 0 end) tag2,
                max(case when instr(urltag,'vipStatus=1')>0 or instr(urltag,'vipStatus=3')>0 then 1 else 0 end) tag3,
                max(case when url_host ='liveroom.ximalaya.com' then 1 else 0 end) tag4,
                max(case when url_host ='mp.ximalaya.com' and instr(urltag,'recharge')>0 then 1 else 0 end) tag5,
                max(case when url_host ='searchwsa.ximalaya.com'  then 1 else 0 end) tag6,
                max(case when url_host ='ad.ximalaya.com' and instr(urltag,'positionName=focus%categoryId=-2')>0 then 1 else 0 end) tag7,
                max(case when url_host='mwsa.ximalaya.com' and instr(urltag,'vip%channel%categoryId=-8')>0 then 1 else 0 end) tag8,
                max(case when url_host='ifm.ximalaya.com' and instr(urltag,'daily')>0 then 1 else 0 end) tag9,
                max(case when instr(urltag,'AggregateRankListTabs')>0 then 1 else 0 end) tag10,
                max(case when instr(urltag,'recommends%categoryId=33')>0 then 1 else 0 end) tag11,
                max(case when instr(urltag,'topBuzz')>0 then 1 else 0 end) tag12,
                max(case when instr(urltag,'sceneId=')>0 then 1 else 0 end) tag13
            from  user_action_tag_d
            where pt_days>='20190610' and pt_days<'20190701'  
              and instr(url_host,'ximalaya.com')>0
              group BY
                pt_days,
                device_number,
                hh) a
   group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';


-- 喜马拉雅场景电台统计
select        '喜马拉雅场景电台统计' model,
               pt_days,
               regexp_extract(urltag,'(sceneId=-{0,1}[0-9]+)',1) id,
               count(*) pv,
               count(distinct device_number) uv
         from  user_action_tag_d
      where pt_days>='20190610' and pt_days<'20190701'
        and instr(url_host,'ximalaya.com')>0 and instr(urltag,'sceneId=')>0
        group by pt_days,
                 regexp_extract(urltag,'(sceneId=-{0,1}[0-9]+)',1);


select '-- -- -*******************************************************************************************************************-- -- -';

-- 喜马拉雅频道统计
select        '喜马拉雅频道统计' model,
               pt_days,
               regexp_extract(urltag,'(categoryId=-{0,1}[0-9]+)',1) id,
               count(*) pv,
               count(distinct device_number) uv
         from  user_action_tag_d
      where pt_days>='20190610' and pt_days<'20190701'
        and instr(url_host,'ximalaya.com')>0 and instr(urltag,'categoryId=')>0
        group by pt_days,
                 regexp_extract(urltag,'(categoryId=-{0,1}[0-9]+)',1);

select '-- -- -*******************************************************************************************************************-- -- -';

-- -喜马拉雅rankingListId排行
select        '喜马拉雅rankingListId排行' model,
               pt_days,
               regexp_extract(urltag,'(rankingListId=-{0,1}[0-9]+)',1) id,
               count(*) pv,
               count(distinct device_number) uv
         from  user_action_tag_d
      where pt_days>='20190610' and pt_days<'20190701'
        and instr(url_host,'ximalaya.com')>0 and instr(urltag,'rankingListId=')>0
        group by pt_days,
                 regexp_extract(urltag,'(rankingListId=-{0,1}[0-9]+)',1);

select '-- -- -*******************************************************************************************************************-- -- -';

-- 手机百度各模块统计
select  '手机百度各模块统计' model,
        '日期' pt_days,
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
select '手机百度各模块统计' model,
        pt_days,
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
                max(case when url_host='smartprogram.baidu.com' then 1 else 0 end) tag9,
                max(case when url_host='bdbus-turbonet.baidu.com' then 1 else 0 end) tag10,
                max(case when url_host='hmma.baidu.com' then 1 else 0 end) tag11,
                max(case when url_host='mbd.baidu.com' then 1 else 0 end) tag12
        from  user_action_tag_d
        where pt_days>='20190610' and pt_days<'20190701'  
                and (instr(url_host,'baidu.com')>0 or instr(url_host,'bdstatic.com')>0 or url_host='smartprogram.baidu.com' )
                group BY
                pt_days,
                device_number,
                hh) a
group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

select  'UC各模块统计' model,
        '日期' pt_days,
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
select 'UC各模块统计' model,
        pt_days,
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
                max(case when instr(url_host,'.uc.cn')>0 and instr(urltag,'book')>0 then 1 else 0 end) tag2,
                max(case when instr(url_host,'shuqireader.com')>0 then 1 else 0 end) tag3,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'fiction')>0 then 1 else 0 end) tag4,
                max(case when url_host='iflow.uczzd.cn' and instr(urltag,'video')>0 then 1 else 0 end) tag5,
                max(case when url_host='navi-user.uc.cn' and instr(urltag,'search')>0 then 1 else 0 end) tag6,
                max(case when url_host='img.v.uc.cn'  then 1 else 0 end) tag7,
                max(case when url_host='video.ums.uc.cn' then 1 else 0 end) tag8,
                max(case when url_host='sugs.m.sm.cn' and instr(urltag,'ucinput')>0 then 1 else 0 end) tag9,
                max(case when url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') then 1 else 0 end) tag10
            from  user_action_tag_d
            where pt_days>='20190610' and pt_days<'20190701'
              and (instr(url_host,'.uczzd.cn')>0 or instr(url_host,'.uc.cn')>0 or url_host in ('sugs.m.sm.cn','so.m.sm.cn','xm.sm.cn') or instr(url_host,'.shuqireader.com')>0)
              group BY
                pt_days,
                device_number,
                hh) a
        group by pt_days;


select '-- -- -*******************************************************************************************************************-- -- -';

-- 头条未加密部分各模块统计
select '头条未加密部分各模块统计' model,
       '日期' pt_days,
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
       'uv_主TL视频点击' uv13,
       'uv_微头条' uv14,
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
       'pv_头条-我的页面' pv12,
       'pv_主TL视频点击' pv13,
       'pv_微头条' pv14
union all
select '头条未加密部分各模块统计' model,
        pt_days,
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
        count(distinct(case when tag13=1 then device_number else null end)) uv13,
        count(distinct(case when tag14=1 then device_number else null end)) uv14,
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
        sum(case when tag12=1 then 1 else 0 end) pv12,
        sum(case when tag13=1 then 1 else 0 end) pv13,
        sum(case when tag14=1 then 1 else 0 end) pv14
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
            max(case when instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=hotsoon_video_detail_draw')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0  then 1 else 0 end) tag8,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag9,
            max(case when url_host='mcs.snssdk.com' then 1 else 0 end) tag10,
            max(case when url_host='microapp.bytedance.com' then 1 else 0 end) tag11,
            max(case when instr(urltag,'user/profile')>0 then 1 else 0 end) tag12,
            max(case when (instr(urltag,'video/play')>0 or instr(urltag,'video/app')>0) and instr(urltag,'from_category=__all__')>0 then 1 else 0 end) tag13,
            max(case when instr(urltag,'ugc/repost/')>0 then 1 else 0 end) tag14
          from  user_action_tag_d
          where pt_days>='20190515' and pt_days<'20190701'
            and ((url_host like '%.snssdk.com' and instr(urltag,'app_name=news_article')>0)
                  or url_host in ('developer.toutiao.com','microapp.bytedance.com'))
        group by pt_days,
                 device_number,
                 hh) a
      group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

-- -头条极速版
select  '头条极速版各模块统计' model,
        '日期' pt_days,
        'uv_点击caaa' uv1,
        'uv_搜索' uv2,
        'uv_短视频合并ca' uv3,
        'uv_问答s' uv4,
        'uv_主TL视频点击' uv5,
        'uv_小视频合并ccvg' uv6,
        'uv_我的页面' uv7,
        'pv_点击caaa' pv1,
        'pv_搜索' pv2,
        'pv_短视频合并ca' pv3,
        'pv_问答s' pv4,
        'pv_主TL视频点击' pv5,
        'pv_小视频合并ccvg' pv6,
        'pv_我的页面' pv7
union all
select  '头条极速版各模块统计' model,
        pt_days,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1 then device_number else null end)) uv7,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4,
        sum(case when tag5=1 then 1 else 0 end) pv5,
        sum(case when tag6=1 then 1 else 0 end) pv6,
        sum(case when tag7=1 then 1 else 0 end) pv7
  from 
      (select pt_days,
            device_number,
            hh,
            max(case when instr(urltag,'category=video')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 then 1 else 0 end) tag1,
            max(case when instr(urltag,'keyword')>0 then 1 else 0 end) tag2,
            max(case when instr(urltag,'category=video')>0 or instr(urltag,'action=GetPlayInfo')>0  then 1 else 0 end) tag3,
            max(case when instr(urltag,'answer/detail')>0 then 1 else 0 end) tag4,
            max(case when (instr(urltag,'video/play')>0 or instr(urltag,'video/app')>0) and instr(urltag,'from_category=__all__')>0 then 1 else 0 end) tag5,
            max(case when instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=hotsoon_video_detail_draw')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0  then 1 else 0 end) tag6,
            max(case when instr(urltag,'user/profile')>0 then 1 else 0 end) tag7
          from  user_action_tag_d
          where  pt_days>='20190610' and pt_days<'20190701'
            and ((url_host like '%.snssdk.com' and instr(urltag,'app_name=news_article_lite')>0)
                  or url_host in ('developer.toutiao.com','microapp.bytedance.com'))
        group by pt_days,
                 device_number,
                 hh) a
  group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

-- 抖音上传effect
select '抖音' app,
       '上传effect' model,
       pt_days,
       sum(case when actiontag>5 then 1 else 0 end) f5,
       sum(case when actiontag>6 then 1 else 0 end) f6,
       sum(case when actiontag>7 then 1 else 0 end) f7,
       sum(case when actiontag>8 then 1 else 0 end) f8
    from
(select pt_days,
        device_number,
        actiontag,
        row_number() over(partition by pt_days,device_number order by cast(actiontag as int) desc) rn
        from  user_action_tag_d
     where  pt_days>='20190610' and pt_days<'20190701'
       and  url_host = 'effect.snssdk.com') a
where a.rn=1
group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

-- 腾讯新闻内容频道分布
select 
   '201905' month_id,
  '腾讯新闻内容频道分布1' app,
  con2,
  count(*) pv,
  count(distinct con1) id_cnt,
  count(distinct device_number) uv
  from user_action_context_d
where pt_days>='20190601' and pt_days<'20190701'
  and model in ('tencent_news_article','tencent_news_video')
  and con2<>'201'
  group by con2;

select '-- -- -*******************************************************************************************************************-- -- -';

  select
    '201905' month_id,
    '腾讯新闻内容频道分布2' app,
    con3,
    count(*) pv,
    count(distinct con1) id_cnt,
    count(distinct device_number) uv
    from user_action_context_d
where pt_days>='20190601' and pt_days<'20190701'
    and model in ('tencent_news_article','tencent_news_video')
    and con2='201'
    group by con3;

select '-- -- -*******************************************************************************************************************-- -- -';

-- 腾讯内容分布统计
  select
    '201905' month_id,
    '腾讯文章pagefrom统计' app,
    con4,
    count(*) pv,
    count(distinct con1) id_cnt,
    count(distinct device_number) uv
    from user_action_context_d
where pt_days>='20190601' and pt_days<'20190701'
    and model='tencent_news_article'
    group by con4;

select '-- -- -*******************************************************************************************************************-- -- -';

-- 头条内容分布统计
  select
    '201905' month_id,
    '头条内容分布统计1' app,
    con3,
    count(*) pv,
    count(distinct con1) id_cnt,
    count(distinct device_number) uv
    from user_action_context_d
where pt_days>='20190601' and pt_days<'20190701'
    and model='toutiao_article'
    group by con3;

select '-- -- -*******************************************************************************************************************-- -- -';

-- 头条内容分布统计
  select
    '201905' month_id,
    '头条内容分布统计2' app,
    con4,
    count(*) pv,
    count(distinct con1) id_cnt,
    count(distinct device_number) uv
    from user_action_context_d
where pt_days>='20190601' and pt_days<'20190701'
    and model='toutiao_article'
    group by con4;

select '-- -- -*******************************************************************************************************************-- -- -';

-- 内容表分model统计
select '201905' month_id,
       '内容表统计',
        model,
        count(distinct con1) con_cnt,
        count(*) pv,
        count(distinct device_number) uv
    from user_action_context_d
where pt_days>='20190601' and pt_days<'20190701'
    group by model;

select '-- -- -*******************************************************************************************************************-- -- -';


select  '钉钉各模块统计' model,
        '日期' pt_days,
        'uv_钉钉启动' uv1,
        'uv_vip' uv2,
        'uv_h5-微应用' uv3,
        'uv_审批aflow' uv4,
        'uv_钉盘space' uv5,
        'uv_智能办公focus' uv6,
        'uv_电话会议' uv7,
        'uv_sh' uv8,
        'uv_perks-福利社' uv9,
        'uv_钉钉社区' uv10,
        'uv_钉钉管理后台OA' uv11,
        'uv_分享' uv12,
        'uv_im' uv13,
        'uv_csp' uv14,
        'uv_应用中心' uv15,
        'uv_钉邮' uv16,
        'uv_package-E' uv17,
        'uv_ding任务TMS' uv18,
        'uv_文件预览' uv19,
        'uv_钉钉直播/回放' uv20,
        'uv_钉钉-投票助手' uv21,
        'uv_钉钉社区' uv22,
        'uv_钉钉file' uv23,
        'uv_钉钉日志' uv24,
        'uv_HR登记' uv25,
        'uv_考勤' uv26,
        'uv_industry' uv27,
        'uv_staticDING' uv28,
        'uv_阿里邮箱' uv29,
        'uv_智能报表' uv30,
        'uv_小目标' uv31,
        'uv_预定' uv32,
        'uv_公告' uv33,
        'uv_登录login' uv34,
        'uv_钉钉qr' uv35,
        'pv_钉钉启动' pv1,
        'pv_vip' pv2,
        'pv_h5-微应用' pv3,
        'pv_审批aflow' pv4,
        'pv_钉盘space' pv5,
        'pv_智能办公focus' pv6,
        'pv_电话会议' pv7,
        'pv_sh' pv8,
        'pv_perks-福利社' pv9,
        'pv_钉钉社区' pv10,
        'pv_钉钉管理后台OA' pv11,
        'pv_分享' pv12,
        'pv_im' pv13,
        'pv_csp' pv14,
        'pv_应用中心' pv15,
        'pv_钉邮' pv16,
        'pv_package-E' pv17,
        'pv_ding任务TMS' pv18,
        'pv_文件预览' pv19,
        'pv_钉钉直播/回放' pv20,
        'pv_钉钉-投票助手' pv21,
        'pv_钉钉社区' pv22,
        'pv_钉钉file' pv23,
        'pv_钉钉日志' pv24,
        'pv_HR登记' pv25,
        'pv_考勤' pv26,
        'pv_industry' pv27,
        'pv_staticDING' pv28,
        'pv_阿里邮箱' pv29,
        'pv_智能报表' pv30,
        'pv_小目标' pv31,
        'pv_预定' pv32,
        'pv_公告' pv33,
        'pv_登录login' pv34,
        'pv_钉钉qr' pv35
 union all 
 select '钉钉各模块统计' model,
        pt_days,
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
        count(distinct(case when tag13=1 then device_number else null end)) uv13,
        count(distinct(case when tag14=1 then device_number else null end)) uv14,
        count(distinct(case when tag15=1 then device_number else null end)) uv15,
        count(distinct(case when tag16=1 then device_number else null end)) uv16,
        count(distinct(case when tag17=1 then device_number else null end)) uv17,
        count(distinct(case when tag18=1 then device_number else null end)) uv18,
        count(distinct(case when tag19=1 then device_number else null end)) uv19,
        count(distinct(case when tag20=1 then device_number else null end)) uv20,
        count(distinct(case when tag21=1 then device_number else null end)) uv21,
        count(distinct(case when tag22=1 then device_number else null end)) uv22,
        count(distinct(case when tag23=1 then device_number else null end)) uv23,
        count(distinct(case when tag24=1 then device_number else null end)) uv24,
        count(distinct(case when tag25=1 then device_number else null end)) uv25,
        count(distinct(case when tag26=1 then device_number else null end)) uv26,
        count(distinct(case when tag27=1 then device_number else null end)) uv27,
        count(distinct(case when tag28=1 then device_number else null end)) uv28,
        count(distinct(case when tag29=1 then device_number else null end)) uv29,
        count(distinct(case when tag30=1 then device_number else null end)) uv30,
        count(distinct(case when tag31=1 then device_number else null end)) uv31,
        count(distinct(case when tag32=1 then device_number else null end)) uv32,
        count(distinct(case when tag33=1 then device_number else null end)) uv33,
        count(distinct(case when tag34=1 then device_number else null end)) uv34,
        count(distinct(case when tag35=1 then device_number else null end)) uv35,
        sum(case when tag1=1 then 1 else 0 end)  pv1,
        sum(case when tag2=1 then 1 else 0 end)  pv2,
        sum(case when tag3=1 then 1 else 0 end)  pv3,
        sum(case when tag4=1 then 1 else 0 end)  pv4,
        sum(case when tag5=1 then 1 else 0 end)  pv5,
        sum(case when tag6=1 then 1 else 0 end)  pv6,
        sum(case when tag7=1 then 1 else 0 end)  pv7,
        sum(case when tag8=1 then 1 else 0 end)  pv8,
        sum(case when tag9=1 then 1 else 0 end)  pv9,
        sum(case when tag10=1 then 1 else 0 end)  pv10,
        sum(case when tag11=1 then 1 else 0 end)  pv11,
        sum(case when tag12=1 then 1 else 0 end)  pv12,
        sum(case when tag13=1 then 1 else 0 end)  pv13,
        sum(case when tag14=1 then 1 else 0 end)  pv14,
        sum(case when tag15=1 then 1 else 0 end)  pv15,
        sum(case when tag16=1 then 1 else 0 end)  pv16,
        sum(case when tag17=1 then 1 else 0 end)  pv17,
        sum(case when tag18=1 then 1 else 0 end)  pv18,
        sum(case when tag19=1 then 1 else 0 end)  pv19,
        sum(case when tag20=1 then 1 else 0 end)  pv20,
        sum(case when tag21=1 then 1 else 0 end)  pv21,
        sum(case when tag22=1 then 1 else 0 end)  pv22,
        sum(case when tag23=1 then 1 else 0 end)  pv23,
        sum(case when tag24=1 then 1 else 0 end)  pv24,
        sum(case when tag25=1 then 1 else 0 end)  pv25,
        sum(case when tag26=1 then 1 else 0 end)  pv26,
        sum(case when tag27=1 then 1 else 0 end)  pv27,
        sum(case when tag28=1 then 1 else 0 end)  pv28,
        sum(case when tag29=1 then 1 else 0 end)  pv29,
        sum(case when tag30=1 then 1 else 0 end)  pv30,
        sum(case when tag31=1 then 1 else 0 end)  pv31,
        sum(case when tag32=1 then 1 else 0 end)  pv32,
        sum(case when tag33=1 then 1 else 0 end)  pv33,
        sum(case when tag34=1 then 1 else 0 end)  pv34,
        sum(case when tag35=1 then 1 else 0 end)  pv35
   from 
       (select 
        pt_days,
        hh,
        device_number,
        max(case when instr(url_host,'.dingtalk.com')>0 then 1 else 0 end) tag1,
        max(case when url_host='vip.dingtalk.com' then 1 else 0 end) tag2,
        max(case when url_host='h5.dingtalk.com' then 1 else 0 end) tag3,
        max(case when url_host='aflow.dingtalk.com' then 1 else 0 end) tag4,
        max(case when url_host='space.dingtalk.com' then 1 else 0 end) tag5,
        max(case when url_host='focus.dingtalk.com' then 1 else 0 end) tag6,
        max(case when url_host='callapp.dingtalk.com' then 1 else 0 end) tag7,
        max(case when url_host='sh.trans.dingtalk.com' then 1 else 0 end) tag8,
        max(case when url_host='perks.dingtalk.com' then 1 else 0 end) tag9,
        max(case when url_host='club.dingding.xin' then 1 else 0 end) tag10,
        max(case when url_host in ('oa.dingtalk.com','oapi.dingtalk.com') then 1 else 0 end) tag11,
        max(case when url_host='wx.dingtalk.com' then 1 else 0 end) tag12,
        max(case when url_host='im.dingtalk.com' then 1 else 0 end) tag13,
        max(case when url_host='csp.dingtalk.com' then 1 else 0 end) tag14,
        max(case when url_host='appcenter.dingtalk.com' then 1 else 0 end) tag15,
        max(case when url_host in ('mail.dingtalk.com','mailhelp.dingtalk.com') then 1 else 0 end) tag16,
        max(case when url_host='package.dingtalk.com' then 1 else 0 end) tag17,
        max(case when url_host='tms.dingtalk.com' then 1 else 0 end) tag18,
        max(case when url_host in ('sh.preview.dingtalk.com','zjk.preview.dingtalk.com','sz.preview.dingtalk.com') then 1 else 0 end) tag19,
        max(case when url_host='dtliving.alicdn.com' then 1 else 0 end) tag20,
        max(case when url_host='vote.dingteam.com' then 1 else 0 end) tag21,
        max(case when url_host='club.dingding.xin' then 1 else 0 end) tag22,
        max(case when url_host='file.dingtalk.com' then 1 else 0 end) tag23,
        max(case when url_host='landray.dingtalkapps.com' then 1 else 0 end) tag24,
        max(case when url_host='hrmregister.dingtalk.com' then 1 else 0 end) tag25,
        max(case when url_host in ('attendance.dingtalk.com','attend.dingtalk.com') then 1 else 0 end) tag26,
        max(case when url_host='industry-fab.dingtalk.com' then 1 else 0 end) tag27,
        max(case when url_host='static.dingtalk.com' then 1 else 0 end) tag28,
        max(case when url_host in ('mail.mxhichina.com','imap.mxhichina.com','smtp.mxhichina.com') then 1 else 0 end) tag29,
        max(case when url_host='clouddata.dingtalkapps.com' then 1 else 0 end) tag30,
        max(case when url_host='small-target.dingtalk.com' then 1 else 0 end) tag31,
        max(case when url_host='reservation.dingtalk.com' then 1 else 0 end) tag32,
        max(case when url_host='app.dingtalk.com' then 1 else 0 end) tag33,
        max(case when url_host='login.dingtalk.com' then 1 else 0 end) tag34,
        max(case when url_host='qr.dingtalk.com' then 1 else 0 end) tag35
      from  user_action_tag_d
      where  pt_days>='20190610' and pt_days<'20190701'
        and (instr(url_host,'.dingtalk.com')>0 
             or instr(url_host,'.dingding.xin')>0
             or instr(url_host,'.mxhichina.com')>0 
             or instr(url_host,'.dingtalkapps.com')>0
             or url_host in ('dtliving.alicdn.com','vote.dingteam.com'))
        group by pt_days,
                 device_number,
                 hh) a
group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

select '拼多多各模块统计' model,
       '日期' pt_days,
       'uv_拼多多Dau' uv1,
       'uv_拼多多活动总触达' uv2,
       'uv_拼多多店铺触达' uv3,
       'pv_拼多多Dau' pv1,
       'pv_拼多多活动总触达' pv2,
       'pv_拼多多店铺触达' pv3
union all
select '拼多多各模块统计' model,
        pt_days,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        sum(case when tag1=1 then 1 else 0 end)  pv1,
        sum(case when tag2=1 then 1 else 0 end)  pv2,
        sum(case when tag3=1 then 1 else 0 end)  pv3
 from 
        (select  pt_days,
                device_number,
                hh,
                max(case when url_host in ('cmta.pinduoduo.com','api.pinduoduo.com','t00img.yangkeduo.com','t01img.yangkeduo.com','t05img.yangkeduo.com','t04img.yangkeduo.com','t14img.yangkeduo.com','t13img.yangkeduo.com','t16img.yangkeduo.com','pinduoduoimg.yangkeduo.com','images.pinduoduo.com') then 1 else 0 end) tag1,
                max(case when (url_host='t13img.yangkeduo.com' and instr(urltag,'cart')>0) or (url_host='pinduoduoimg.yangkeduo.com' and instr(urltag,'promotion')>0) then 1 else 0 end) tag2,
                max(case when url_host='t16img.yangkeduo.com' and instr(urltag,'pdd_ims')>0 then 1 else 0 end) tag3
        from  user_action_tag_d
        where  pt_days>='20190610' and pt_days<'20190701'
                and (instr(url_host,'.pinduoduo.com')>0 
                or instr(url_host,'.yangkeduo.com')>0)
                group by pt_days,
                        device_number,
                        hh) a
group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

-- 支付宝各模块统计
select    '支付宝各模块统计' model,
          '日期' pt_days,
          'uv_支付宝登陆' uv_1,
          'uv_支付宝财富模块' uv0,
          'uv_支付宝使用mdap' uv1,
          'uv_支付宝使用mcgw' uv2,
          'uv_支付宝使用mobilegw' uv3,
          'uv_支付宝-移动特权' uv4,
          'uv_支付宝-蘑菇租房' uv5,
          'uv_支付宝-怪兽充电' uv6,
          'uv_支付宝-哈喽单车' uv7,
          'uv_支付宝-饿了么入口' uv8,
          'uv_支付宝-天猫小程序入口' uv9,
          'uv_支付宝-优酷会员' uv10,
          'uv_支付宝-校园生活' uv11,
          'uv_支付宝-车主服务' uv12,
          'uv_支付宝-高德' uv13,
          'uv_支付宝-大麦网' uv14,
          'uv_支付宝-嘀嗒出行' uv15,
          'uv_支付宝-每日必抢/聚划算' uv16,
          'uv_支付宝-天猫商品点击' uv17,
          'uv_支付宝-淘宝商品点击' uv18,
          'uv_芝麻信用' uv19,
          'uv_支付宝-财富号' uv21,
          'uv_汽车报价' uv22,
          'uv_高德实时公交' uv23,
          'uv_寻证易' uv24,
          'uv_萌宠到家' uv25,
          'uv_移动花卡' uv26,
          'uv_社保查询' uv27,
          'uv_书旗小说' uv28,
          'uv_肯德基+' uv29,
          'uv_国家移民局' uv31,
          'uv_爱租机' uv32,
          'uv_哈罗出行' uv33,
          'uv_12306出行' uv34,
          'uv_麦当劳' uv35,
          'uv_交管12123' uv36,
          'uv_哇哈哈到家' uv37,
          'uv_ofo小黄车' uv38,
          'uv_股票财富号' uv40,
          'uv_adv财富号' uv41,
          'uv_私募财富号' uv42,
          'uv_天弘基金' uv43,
          'uv_render财富' uv44,
          'uv_支付宝小程序' uv45,
          'uv_最美证件照' uv46,
          'uv_全国社保计算器' uv47,
          'uv_工资个税计算器' uv48,
          'uv_浙江预约挂号' uv49,
          'uv_药急送' uv50,
          'uv_免费照片打印' uv51,
          'uv_人人租机' uv52,
          'uv_内啥' uv53,
          'uv_公立医院体检预约' uv54,
          'uv_齐天大圣' uv55,
          'uv_旧衣服回收' uv56,
          'uv_违章缴费' uv57,
          'uv_政府业务' uv58,
          'uv_魔盒citybox' uv59,
          'uv_衣二三' uv60,
          'uv_微车' uv61,
          'uv_优酷会员兑换' uv62,
          'uv_支付宝-客服' uv63,
          'uv_蚂蚁保险' uv64,
          'uv_余额宝' uv65,                
          'uv_支付宝登陆' pv_1,
          'pv_支付宝财富模块' pv0,
          'pv_支付宝使用mdap' pv1,
          'pv_支付宝使用mcgw' pv2,
          'pv_支付宝使用mobilegw' pv3,
          'pv_支付宝-移动特权' pv4,
          'pv_支付宝-蘑菇租房' pv5,
          'pv_支付宝-怪兽充电' pv6,
          'pv_支付宝-哈喽单车' pv7,
          'pv_支付宝-饿了么入口' pv8,
          'pv_支付宝-天猫小程序入口' pv9,
          'pv_支付宝-优酷会员' pv10,
          'pv_支付宝-校园生活' pv11,
          'pv_支付宝-车主服务' pv12,
          'pv_支付宝-高德' pv13,
          'pv_支付宝-大麦网' pv14,
          'pv_支付宝-嘀嗒出行' pv15,
          'pv_支付宝-每日必抢/聚划算' pv16,
          'pv_支付宝-天猫商品点击' pv17,
          'pv_支付宝-淘宝商品点击' pv18,
          'pv_芝麻信用' pv19,
          'pv_支付宝-财富号' pv21,
          'pv_汽车报价' pv22,
          'pv_高德实时公交' pv23,
          'pv_寻证易' pv24,
          'pv_萌宠到家' pv25,
          'pv_移动花卡' pv26,
          'pv_社保查询' pv27,
          'pv_书旗小说' pv28,
          'pv_肯德基+' pv29,
          'pv_国家移民局' pv31,
          'pv_爱租机' pv32,
          'pv_哈罗出行' pv33,
          'pv_12306出行' pv34,
          'pv_麦当劳' pv35,
          'pv_交管12123' pv36,
          'pv_哇哈哈到家' pv37,
          'pv_ofo小黄车' pv38,
          'pv_股票财富号' pv40,
          'pv_adv财富号' pv41,
          'pv_私募财富号' pv42,
          'pv_天弘基金' pv43,
          'pv_render财富' pv44,
          'pv_支付宝小程序' pv45,
          'pv_最美证件照' pv46,
          'pv_全国社保计算器' pv47,
          'pv_工资个税计算器' pv48,
          'pv_浙江预约挂号' pv49,
          'pv_药急送' pv50,
          'pv_免费照片打印' pv51,
          'pv_人人租机' pv52,
          'pv_内啥' pv53,
          'pv_公立医院体检预约' pv54,
          'pv_齐天大圣' pv55,
          'pv_旧衣服回收' pv56,
          'pv_违章缴费' pv57,
          'pv_政府业务' pv58,
          'pv_魔盒citybox' pv59,
          'pv_衣二三' pv60,
          'pv_微车' pv61,
          'pv_优酷会员兑换' pv62,
          'pv_支付宝-客服' pv63,
          'pv_蚂蚁保险' pv64,
          'pv_余额宝' pv65
union all
select  '支付宝各模块统计' model,
        pt_days,
        count(distinct(case when tag_login=1 then device_number else null end)) uv_1,
        count(distinct(case when tag_cf=1 then device_number else null end)) uv0,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1  and tag_login=1 then device_number else null end)) uv4,
        count(distinct(case when tag5=1  and tag_login=1 then device_number else null end)) uv5,
        count(distinct(case when tag6=1  and tag_login=1 then device_number else null end)) uv6,
        count(distinct(case when tag7=1  and tag_login=1 then device_number else null end)) uv7,
        count(distinct(case when tag8=1  and tag_login=1 then device_number else null end)) uv8,
        count(distinct(case when tag9=1  and tag_login=1 then device_number else null end)) uv9,
        count(distinct(case when tag10=1  and tag_login=1 then device_number else null end)) uv10,
        count(distinct(case when tag11=1  and tag_login=1 then device_number else null end)) uv11,
        count(distinct(case when tag12=1  and tag_login=1 then device_number else null end)) uv12,
        count(distinct(case when tag13=1  and tag_login=1 then device_number else null end)) uv13,
        count(distinct(case when tag14=1  and tag_login=1 then device_number else null end)) uv14,
        count(distinct(case when tag15=1  and tag_login=1 then device_number else null end)) uv15,
        count(distinct(case when tag16=1  and tag_login=1 then device_number else null end)) uv16,
        count(distinct(case when tag17=1  and tag_login=1 then device_number else null end)) uv17,
        count(distinct(case when tag18=1  and tag_login=1 then device_number else null end)) uv18,
        count(distinct(case when tag19=1  and tag_login=1 then device_number else null end)) uv19,
        count(distinct(case when tag21=1  and tag_login=1 then device_number else null end)) uv21,
        count(distinct(case when tag22=1  and tag_login=1 then device_number else null end)) uv22,
        count(distinct(case when tag23=1  and tag_login=1 then device_number else null end)) uv23,
        count(distinct(case when tag24=1  and tag_login=1 then device_number else null end)) uv24,
        count(distinct(case when tag25=1  and tag_login=1 then device_number else null end)) uv25,
        count(distinct(case when tag26=1  and tag_login=1 then device_number else null end)) uv26,
        count(distinct(case when tag27=1  and tag_login=1 then device_number else null end)) uv27,
        count(distinct(case when tag28=1  and tag_login=1 then device_number else null end)) uv28,
        count(distinct(case when tag29=1  and tag_login=1 then device_number else null end)) uv29,
        count(distinct(case when tag31=1  and tag_login=1 then device_number else null end)) uv31,
        count(distinct(case when tag32=1  and tag_login=1 then device_number else null end)) uv32,
        count(distinct(case when tag33=1  and tag_login=1 then device_number else null end)) uv33,
        count(distinct(case when tag34=1  and tag_login=1 then device_number else null end)) uv34,
        count(distinct(case when tag35=1  and tag_login=1 then device_number else null end)) uv35,
        count(distinct(case when tag36=1  and tag_login=1 then device_number else null end)) uv36,
        count(distinct(case when tag37=1  and tag_login=1 then device_number else null end)) uv37,
        count(distinct(case when tag38=1  and tag_login=1 then device_number else null end)) uv38,
        count(distinct(case when tag40=1  and tag_login=1 then device_number else null end)) uv40,
        count(distinct(case when tag41=1  and tag_login=1 then device_number else null end)) uv41,
        count(distinct(case when tag42=1  and tag_login=1 then device_number else null end)) uv42,
        count(distinct(case when tag43=1  and tag_login=1 then device_number else null end)) uv43,
        count(distinct(case when tag44=1  and tag_login=1 then device_number else null end)) uv44,
        count(distinct(case when tag45=1  and tag_login=1 then device_number else null end)) uv45,
        count(distinct(case when tag46=1  and tag_login=1 then device_number else null end)) uv46,
        count(distinct(case when tag47=1  and tag_login=1 then device_number else null end)) uv47,
        count(distinct(case when tag48=1  and tag_login=1 then device_number else null end)) uv48,
        count(distinct(case when tag49=1  and tag_login=1 then device_number else null end)) uv49,
        count(distinct(case when tag50=1  and tag_login=1 then device_number else null end)) uv50,
        count(distinct(case when tag51=1  and tag_login=1 then device_number else null end)) uv51,
        count(distinct(case when tag52=1  and tag_login=1 then device_number else null end)) uv52,
        count(distinct(case when tag53=1  and tag_login=1 then device_number else null end)) uv53,
        count(distinct(case when tag54=1  and tag_login=1 then device_number else null end)) uv54,
        count(distinct(case when tag55=1  and tag_login=1 then device_number else null end)) uv55,
        count(distinct(case when tag56=1  and tag_login=1 then device_number else null end)) uv56,
        count(distinct(case when tag57=1  and tag_login=1 then device_number else null end)) uv57,
        count(distinct(case when tag58=1  and tag_login=1 then device_number else null end)) uv58,
        count(distinct(case when tag59=1  and tag_login=1 then device_number else null end)) uv59,
        count(distinct(case when tag60=1  and tag_login=1 then device_number else null end)) uv60,
        count(distinct(case when tag61=1  and tag_login=1 then device_number else null end)) uv61,
        count(distinct(case when tag62=1  and tag_login=1 then device_number else null end)) uv62,
        count(distinct(case when tag63=1 then device_number else null end)) uv63,
        count(distinct(case when tag64=1 then device_number else null end)) uv64,
        count(distinct(case when tag65=1 then device_number else null end)) uv65,
        sum(case when tag_login=1 then 1 else 0 end) pv_1,
        sum(case when tag_cf=1 then 1 else 0 end) pv0,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1  and tag_login=1 then 1 else 0 end) pv4,
        sum(case when tag5=1  and tag_login=1 then 1 else 0 end) pv5,
        sum(case when tag6=1  and tag_login=1 then 1 else 0 end) pv6,
        sum(case when tag7=1  and tag_login=1 then 1 else 0 end) pv7,
        sum(case when tag8=1  and tag_login=1 then 1 else 0 end) pv8,
        sum(case when tag9=1  and tag_login=1 then 1 else 0 end) pv9,
        sum(case when tag10=1  and tag_login=1 then 1 else 0 end) pv10,
        sum(case when tag11=1  and tag_login=1 then 1 else 0 end) pv11,
        sum(case when tag12=1  and tag_login=1 then 1 else 0 end) pv12,
        sum(case when tag13=1  and tag_login=1 then 1 else 0 end) pv13,
        sum(case when tag14=1  and tag_login=1 then 1 else 0 end) pv14,
        sum(case when tag15=1  and tag_login=1 then 1 else 0 end) pv15,
        sum(case when tag16=1  and tag_login=1 then 1 else 0 end) pv16,
        sum(case when tag17=1  and tag_login=1 then 1 else 0 end) pv17,
        sum(case when tag18=1  and tag_login=1 then 1 else 0 end) pv18,
        sum(case when tag19=1  and tag_login=1 then 1 else 0 end) pv19,
        sum(case when tag21=1  and tag_login=1 then 1 else 0 end) pv21,
        sum(case when tag22=1  and tag_login=1 then 1 else 0 end) pv22,
        sum(case when tag23=1  and tag_login=1 then 1 else 0 end) pv23,
        sum(case when tag24=1  and tag_login=1 then 1 else 0 end) pv24,
        sum(case when tag25=1  and tag_login=1 then 1 else 0 end) pv25,
        sum(case when tag26=1  and tag_login=1 then 1 else 0 end) pv26,
        sum(case when tag27=1  and tag_login=1 then 1 else 0 end) pv27,
        sum(case when tag28=1  and tag_login=1 then 1 else 0 end) pv28,
        sum(case when tag29=1  and tag_login=1 then 1 else 0 end) pv29,
        sum(case when tag31=1  and tag_login=1 then 1 else 0 end) pv31,
        sum(case when tag32=1  and tag_login=1 then 1 else 0 end) pv32,
        sum(case when tag33=1  and tag_login=1 then 1 else 0 end) pv33,
        sum(case when tag34=1  and tag_login=1 then 1 else 0 end) pv34,
        sum(case when tag35=1  and tag_login=1 then 1 else 0 end) pv35,
        sum(case when tag36=1  and tag_login=1 then 1 else 0 end) pv36,
        sum(case when tag37=1  and tag_login=1 then 1 else 0 end) pv37,
        sum(case when tag38=1  and tag_login=1 then 1 else 0 end) pv38,
        sum(case when tag40=1  and tag_login=1 then 1 else 0 end) pv40,
        sum(case when tag41=1  and tag_login=1 then 1 else 0 end) pv41,
        sum(case when tag42=1  and tag_login=1 then 1 else 0 end) pv42,
        sum(case when tag43=1  and tag_login=1 then 1 else 0 end) pv43,
        sum(case when tag44=1  and tag_login=1 then 1 else 0 end) pv44,
        sum(case when tag45=1  and tag_login=1 then 1 else 0 end) pv45,
        sum(case when tag46=1  and tag_login=1 then 1 else 0 end) pv46,
        sum(case when tag47=1  and tag_login=1 then 1 else 0 end) pv47,
        sum(case when tag48=1  and tag_login=1 then 1 else 0 end) pv48,
        sum(case when tag49=1  and tag_login=1 then 1 else 0 end) pv49,
        sum(case when tag50=1  and tag_login=1 then 1 else 0 end) pv50,
        sum(case when tag51=1  and tag_login=1 then 1 else 0 end) pv51,
        sum(case when tag52=1  and tag_login=1 then 1 else 0 end) pv52,
        sum(case when tag53=1  and tag_login=1 then 1 else 0 end) pv53,
        sum(case when tag54=1  and tag_login=1 then 1 else 0 end) pv54,
        sum(case when tag55=1  and tag_login=1 then 1 else 0 end) pv55,
        sum(case when tag56=1  and tag_login=1 then 1 else 0 end) pv56,
        sum(case when tag57=1  and tag_login=1 then 1 else 0 end) pv57,
        sum(case when tag58=1  and tag_login=1 then 1 else 0 end) pv58,
        sum(case when tag59=1  and tag_login=1 then 1 else 0 end) pv59,
        sum(case when tag60=1  and tag_login=1 then 1 else 0 end) pv60,
        sum(case when tag61=1  and tag_login=1 then 1 else 0 end) pv61,
        sum(case when tag62=1  and tag_login=1 then 1 else 0 end) pv62,
        sum(case when tag63=1 then 1 else 0 end) pv63,
        sum(case when tag64=1 then 1 else 0 end) pv64,
        sum(case when tag65=1 then 1 else 0 end) pv65
    from ( select pt_days,
                  device_number,
                  hh,
                  max(case when instr(url_host,'antfortune.com')>0 then 1 else 0 end) tag_cf,
                  max(case when url_host='loggw.alipay.com' then 1 else 0 end) tag_login,
                  max(case when url_host='mdap.alipay.com' then 1 else 0 end) tag1,
                  max(case when url_host='mcgw.alipay.com' then 1 else 0 end) tag2,
                  max(case when url_host='mobilegw.alipay.com' then 1 else 0 end) tag3,
                  max(case when url_host='mfbizweb.19ego.cn' then 1 else 0 end) tag4,
                  max(case when url_host='api.mgzf.com' then 1 else 0 end) tag5,
                  max(case when url_host='ali.enmonster.com' then 1 else 0 end) tag6,
                  max(case when url_host='api.hellobike.com' then 1 else 0 end) tag7,
                  max(case when url_host='h5.ele.me' then 1 else 0 end) tag8,
                  max(case when url_host='pages.tmall.com' then 1 else 0 end) tag9,
                  max(case when url_host='h5.vip.youku.com' then 1 else 0 end) tag10,
                  max(case when url_host='campus.alipay-eco.com' then 1 else 0 end) tag11,
                  max(case when url_host='mycar-vbizplatform.alipay-eco.com' then 1 else 0 end) tag12,
                  max(case when url_host='mps.amap.com' then 1 else 0 end) tag13,
                  max(case when url_host='mtop.damai.cn' then 1 else 0 end) tag14,
                  max(case when url_host='apis.didapinche.com' then 1 else 0 end) tag15,
                  max(case when url_host='fragment.tmall.com' then 1 else 0 end) tag16,
                  max(case when url_host='s.click.tmall.com' then 1 else 0 end) tag17,
                  max(case when url_host='s.click.taobao.com' then 1 else 0 end) tag18,
                  max(case when instr(url_host,'zmxy.com')>0 then 1 else 0 end) tag19,
                  max(case when url_host='fundcaifuhao.antfortune.com' then 1 else 0 end) tag21,
                  max(case when url_host='magear.pangku.com' then 1 else 0 end) tag22,
                  max(case when url_host='aisle.amap.com' then 1 else 0 end) tag23,
                  max(case when url_host='easysearchcard.chuxingyouhui.com' then 1 else 0 end) tag24,
                  max(case when url_host='mcdj.qipinke.com' then 1 else 0 end) tag25,
                  max(case when url_host='huaka.cmicrwx.cn' then 1 else 0 end) tag26,
                  max(case when url_host='cbp.alipay-eco.com' then 1 else 0 end) tag27,
                  max(case when url_host='miniappweb.shuqireader.com' then 1 else 0 end) tag28,
                  max(case when url_host in ('orders.kfc.com.cn','imgorder.kfc.com.cn') then 1 else 0 end) tag29,
                  max(case when url_host='niaprod.alipay-eco.com' then 1 else 0 end) tag31,
                  max(case when url_host='app.woaizuji.com' then 1 else 0 end) tag32,
                  max(case when url_host='ubt.hellobike.com' then 1 else 0 end) tag33,
                  max(case when url_host='crnetzsjk.crnet.info' then 1 else 0 end) tag34,
                  max(case when url_host='imcd.jaxcx.com' then 1 else 0 end) tag35,
                  max(case when url_host='goveco.alipay.com' then 1 else 0 end) tag36,
                  max(case when url_host='www.mywhh.com' then 1 else 0 end) tag37,
                  max(case when url_host='scofo.ofo.so' then 1 else 0 end) tag38,
                  max(case when url_host='stockcaifuhao.antfortune.com' then 1 else 0 end) tag40,
                  max(case when url_host='advcaifuhao.antfortune.com' then 1 else 0 end) tag41,
                  max(case when url_host='pecaifuhao.antfortune.com' then 1 else 0 end) tag42,
                  max(case when url_host='cfh.tianhongjijin.com.cn' then 1 else 0 end) tag43,
                  max(case when url_host='render.antfortune.com' then 1 else 0 end) tag44,
                  max(case when url_host='mini.open.alipay.com' then 1 else 0 end) tag45,
                  max(case when url_host='apicall.id-photo-verify.com' then 1 else 0 end) tag46,
                  max(case when url_host='scalc-min.renliwo.com' then 1 else 0 end) tag47,
                  max(case when url_host='taxcal.renliwo.com' then 1 else 0 end) tag48,
                  max(case when url_host='ghws5.zj12580.cn' then 1 else 0 end) tag49,
                  max(case when url_host='api.zp315.cn' then 1 else 0 end) tag50,
                  max(case when url_host='api.songzhaopian.com' then 1 else 0 end) tag51,
                  max(case when url_host='m.rrzuji.com' then 1 else 0 end) tag52,
                  max(case when url_host='www.neisha.cc' then 1 else 0 end) tag53,
                  max(case when url_host='xcx.etong-online.com' then 1 else 0 end) tag54,
                  max(case when url_host='mys4s.cn' then 1 else 0 end) tag55,
                  max(case when url_host='lmf.gzluowang.com' then 1 else 0 end) tag56,
                  max(case when url_host='appgw.cmp520.com' then 1 else 0 end) tag57,
                  max(case when url_host='goveco.alipay.com' then 1 else 0 end) tag58,
                  max(case when url_host='cityboxapi.fruitday.com' then 1 else 0 end) tag59,
                  max(case when url_host='api.95vintage.com' then 1 else 0 end) tag60,
                  max(case when url_host='ai.wcar.net.cn' then 1 else 0 end) tag61,
                  max(case when url_host='sky.vip.youku.com' then 1 else 0 end) tag62,
                  max(case when url_host='csmobile.alipay.com' then 1 else 0 end) tag63,
                  max(case when url_host='bxcloudstore.alicdn.com' then 1 else 0 end) tag64,
                  max(case when url_host='yebprod.alipay.com' then 1 else 0 end) tag65
            from user_action_tag_d 
            where  pt_days>='20190610' and pt_days<'20190701'
              and  (url_host in ('mfbizweb.19ego.cn','api.mgzf.com','ali.enmonster.com','api.hellobike.com','h5.ele.me','pages.tmall.com','h5.vip.youku.com','mps.amap.com','mtop.damai.cn','apis.didapinche.com',
                        'fragment.tmall.com','s.click.tmall.com','s.click.taobao.com','magear.pangku.com','aisle.amap.com','easysearchcard.chuxingyouhui.com','mcdj.qipinke.com','huaka.cmicrwx.cn','miniappweb.shuqireader.com',
                        'orders.kfc.com.cn','imgorder.kfc.com.cn','app.woaizuji.com','ubt.hellobike.com','crnetzsjk.crnet.info','imcd.jaxcx.com','www.mywhh.com','scofo.ofo.so','cfh.tianhongjijin.com.cn',
                        'apicall.id-photo-verify.com','scalc-min.renliwo.com','taxcal.renliwo.com','ghws5.zj12580.cn','api.zp315.cn','api.songzhaopian.com','m.rrzuji.com','www.neisha.cc','xcx.etong-online.com',
                        'mys4s.cn','lmf.gzluowang.com','appgw.cmp520.com','cityboxapi.fruitday.com','api.95vintage.com','ai.wcar.net.cn','sky.vip.youku.com','bxcloudstore.alicdn.com') 
                    or instr(url_host,'.zmxy.com')>0
                    or instr(url_host,'.antfortune.com')>0
                    or instr(url_host,'.alipay.com')>0
                    or instr(url_host,'.alipay-eco.com')>0
                    )
                    group by pt_days,
                          device_number,
                          hh) a
    group by  pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';




-- 西瓜各个模块
select  '西瓜各个模块' model,
        '日期' pt_days,
        'uv_西瓜全量启动' uv1,
        'uv_西瓜未加密点击+拉取' uv2,
        'uv_西瓜未加密点击' uv3,
        'uv_西瓜搜索' uv4,
        'uv_西瓜小视频hotsoon' uv5,
        'uv_西瓜小视频tt' uv6,
        'uv_西瓜直播' uv7,
        'uv_西瓜推荐页卡' uv8,
        'uv_西瓜关注页卡' uv9,
        'uv_西瓜影视页卡' uv10,
        'uv_西瓜游戏页卡' uv11,
        'uv_西瓜社会页卡' uv12,
        'uv_西瓜NBA页卡' uv13,
        'uv_西瓜音乐页卡' uv14,
        'uv_西瓜综艺页卡' uv15,
        'uv_西瓜农人页卡' uv16,
        'uv_西瓜美食页卡' uv17,
        'uv_西瓜宠物页卡' uv18,
        'uv_西瓜体育页卡' uv19,
        'uv_西瓜懂车帝页卡' uv20,
        'pv_西瓜全量启动' pv1,
        'pv_西瓜未加密点击+拉取' pv2,
        'pv_西瓜未加密点击' pv3,
        'pv_西瓜搜索' pv4,
        'pv_西瓜小视频hotsoon' pv5,
        'pv_西瓜小视频tt' pv6,
        'pv_西瓜直播' pv7,
        'pv_西瓜推荐页卡' pv8,
        'pv_西瓜关注页卡' pv9,
        'pv_西瓜影视页卡' pv10,
        'pv_西瓜游戏页卡' pv11,
        'pv_西瓜社会页卡' pv12,
        'pv_西瓜NBA页卡' pv13,
        'pv_西瓜音乐页卡' pv14,
        'pv_西瓜综艺页卡' pv15,
        'pv_西瓜农人页卡' pv16,
        'pv_西瓜美食页卡' pv17,
        'pv_西瓜宠物页卡' pv18,
        'pv_西瓜体育页卡' pv19,
        'pv_西瓜懂车帝页卡' pv20
union all 
select '西瓜各个模块' model,
        pt_days,
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
        count(distinct(case when tag13=1 then device_number else null end)) uv13,
        count(distinct(case when tag14=1 then device_number else null end)) uv14,
        count(distinct(case when tag15=1 then device_number else null end)) uv15,
        count(distinct(case when tag16=1 then device_number else null end)) uv16,
        count(distinct(case when tag17=1 then device_number else null end)) uv17,
        count(distinct(case when tag18=1 then device_number else null end)) uv18,
        count(distinct(case when tag19=1 then device_number else null end)) uv19,
        count(distinct(case when tag20=1 then device_number else null end)) uv20,
        sum(case when tag1=1 then 1 else 0 end)  pv1,
        sum(case when tag2=1 then 1 else 0 end)  pv2,
        sum(case when tag3=1 then 1 else 0 end)  pv3,
        sum(case when tag4=1 then 1 else 0 end)  pv4,
        sum(case when tag5=1 then 1 else 0 end)  pv5,
        sum(case when tag6=1 then 1 else 0 end)  pv6,
        sum(case when tag7=1 then 1 else 0 end)  pv7,
        sum(case when tag8=1 then 1 else 0 end)  pv8,
        sum(case when tag9=1 then 1 else 0 end)  pv9,
        sum(case when tag10=1 then 1 else 0 end)  pv10,
        sum(case when tag11=1 then 1 else 0 end)  pv11,
        sum(case when tag12=1 then 1 else 0 end)  pv12,
        sum(case when tag13=1 then 1 else 0 end)  pv13,
        sum(case when tag14=1 then 1 else 0 end)  pv14,
        sum(case when tag15=1 then 1 else 0 end)  pv15,
        sum(case when tag16=1 then 1 else 0 end)  pv16,
        sum(case when tag17=1 then 1 else 0 end)  pv17,
        sum(case when tag18=1 then 1 else 0 end)  pv18,
        sum(case when tag19=1 then 1 else 0 end)  pv19,
        sum(case when tag20=1 then 1 else 0 end)  pv20
 from   
        (select pt_days,
                device_number,
                hh,
                max(case when instr(urltag,'app_name=video_article')>0 then 1 else 0 end) tag1,
                max(case when instr(urltag,'app_name=video_article')>0 and (instr(urltag,'vapp/action')>0 or instr(urltag,'vapp/danmaku')>0  or instr(urltag,'video/app/stream')>0)  then 1 else 0 end) tag2,
                max(case when instr(urltag,'app_name=video_article')>0 and (instr(urltag,'vapp/action')>0 or instr(urltag,'vapp/danmaku')>0) then 1 else 0 end) tag3,
                max(case when instr(urltag,'app_name=video_article')>0 and instr(urltag,'keyword')>0  then 1 else 0 end) tag4,
                max(case when instr(urltag,'app_name=video_article')>0 and instr(urltag,'category=xg_hotsoon_video')>0 then 1 else 0 end) tag5,
                max(case when instr(urltag,'app_name=video_article')>0 and instr(urltag,'tt_shortvideo')>0  then 1 else 0 end) tag6,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=tt_subv_live_tab')>0 then  1 else 0 end) tag7,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=video_new')>0 then  1 else 0 end) tag8,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_user_follow')>0 then  1 else 0 end) tag9,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_xg_movie')>0 then  1 else 0 end) tag10,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_xg_game')>0 then  1 else 0 end) tag11,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_xg_society')>0 then  1 else 0 end) tag12,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_xg_nba')>0 then  1 else 0 end) tag13,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_xg_music')>0 then  1 else 0 end) tag14,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_variety')>0 then  1 else 0 end) tag15,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_video_agriculture')>0 then  1 else 0 end) tag16,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_video_food')>0 then  1 else 0 end) tag17,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_video_pet')>0 then  1 else 0 end) tag18,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_sport')>0 then  1 else 0 end) tag19,
                max(case when instr(urltag,'app_name=video_article')>0  and instr(urltag,'category=subv_car')>0 then  1 else 0 end) tag20
        from user_action_tag_d 
        where pt_days>='20190515' and pt_days<'20190701'
        and  instr(url_host,'snssdk.com')>0 
        and  instr(urltag,'app_name=video_article')>0 
        group by pt_days,
                device_number,
                hh) a
     group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '快手各个模块' model,
        '日期' pt_days,
        'uv1_启动' uv1,
        'uv2_推荐页卡' uv2,
        'uv3_同城页卡' uv3,
        'uv4_关注页卡' uv4,
        'uv5_搜索' uv5,
        'uv6_动态' uv6,
        'uv7_消息' uv7,
        'uv8_私信' uv8,
        'uv9_评论' uv9,
        'uv10_点赞' uv10,
        'uv11_分享' uv11,
        'uv12_关注' uv12,
        'uv13_上传' uv13,
        'uv14_观看直播' uv14,
        'pv1_启动' pv1,
        'pv2_推荐页卡' pv2,
        'pv3_同城页卡' pv3,
        'pv4_关注页卡' pv4,
        'pv5_搜索' pv5,
        'pv6_动态' pv6,
        'pv7_消息' pv7,
        'pv8_私信' pv8,
        'pv9_评论' pv9,
        'pv10_点赞' pv10,
        'pv11_分享' pv11,
        'pv12_关注' pv12,
        'pv13_上传' pv13,
        'pv14_观看直播' pv14        
union all
select '快手各个模块' model,
        pt_days,
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
        count(distinct(case when tag13=1 then device_number else null end)) uv13,
        count(distinct(case when tag14=1 then device_number else null end)) uv14,    
        sum(case when tag1=1 then 1 else 0 end)  pv1,
        sum(case when tag2=1 then 1 else 0 end)  pv2,
        sum(case when tag3=1 then 1 else 0 end)  pv3,
        sum(case when tag4=1 then 1 else 0 end)  pv4,
        sum(case when tag5=1 then 1 else 0 end)  pv5,
        sum(case when tag6=1 then 1 else 0 end)  pv6,
        sum(case when tag7=1 then 1 else 0 end)  pv7,
        sum(case when tag8=1 then 1 else 0 end)  pv8,
        sum(case when tag9=1 then 1 else 0 end)  pv9,
        sum(case when tag10=1 then 1 else 0 end)  pv10,
        sum(case when tag11=1 then 1 else 0 end)  pv11,
        sum(case when tag12=1 then 1 else 0 end)  pv12,
        sum(case when tag13=1 then 1 else 0 end)  pv13,
        sum(case when tag14=1 then 1 else 0 end)  pv14
from 
        (select pt_days,
                hh,
                device_number,
                max(case when url_host='api.gifshow.com' then 1 else 0 end) tag1,	
                max(case when url_host='api.gifshow.com' and instr(urltag,'feed/hot')>0 then 1 else 0 end) tag2,
                max(case when url_host='api.gifshow.com' and (instr(urltag,'feed/nearby')>0  or instr(urltag,'intown')>0)  then 1 else 0 end) tag3,
                max(case when url_host='api.gifshow.com' and (instr(urltag,'myfollow')>0 or instr(urltag,'following')>0)  then 1 else 0 end) tag4,
                max(case when url_host='api.gifshow.com' and instr(urltag,'search')>0 then 1 else 0 end) tag5,
                max(case when url_host='api.gifshow.com' and instr(urltag,'news/load')>0 then 1 else 0 end) tag6,
                max(case when url_host='api.gifshow.com' and instr(urltag,'notify/load')>0 then 1 else 0 end) tag7,
                max(case when url_host='api.gifshow.com' and instr(urltag,'message')>0 then 1 else 0 end) tag8,
                max(case when url_host='api.gifshow.com' and instr(urltag,'comment/add')>0 then 1 else 0 end) tag9,
                max(case when url_host='api.gifshow.com' and instr(urltag,'photo/like')>0 then 1 else 0 end) tag10,
                max(case when url_host='api.gifshow.com' and instr(urltag,'share/sharePhoto')>0 then 1 else 0 end) tag11,
                max(case when url_host='api.gifshow.com' and instr(urltag,'relation/follow')>0 then 1 else 0 end) tag12,
                max(case when url_host='upload.gifshow.com' then 1 else 0 end) tag13,	
                max(case when url_host='live.gifshow.com' and instr(urltag,'live/startPlay')>0 then 1 else 0 end) tag14
        from user_action_tag_d 
        where  pt_days>='20190610' and pt_days<'20190701'
          and  url_host in ('api.gifshow.com','upload.gifshow.com','live.gifshow.com')
        group by pt_days,
                device_number,
                hh) a
group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

select  '火山小视频各模块统计分析' model,
        '日期' pt_days,
        'uv_火山启动app_name' uv1,
        'uv_火山启动webcast' uv2,
        'uv_搜索' uv3,
        'uv_直播页卡' uv4,
        'uv_视频页卡' uv5,
        'uv_关注页卡' uv6,
        'uv_观看直播' uv7,
        'uv_分享链接' uv8,
        'uv_微信分享' uv9,
        'uv_微博分享' uv10,
        'uv_朋友圈分享' uv11,
        'uv_qq分享' uv12,
        'uv_qq空间分享' uv13,
        'uv_评论' uv14,
        'uv_查看他人主页' uv15,
        'uv_关注' uv16,
        'uv_发私信' uv17,
        'uv_取消关注' uv18,
        'uv_我的消息' uv19,
        'uv_钱包' uv20,
        'uv_信用卡中心' uv21,
        'uv_充值' uv22,
        'uv_支付宝充值' uv23,
        'uv_微信充值' uv24,
        'uv_发现-同城' uv25,
        'uv_发现' uv26,
        'uv_我的话题圈' uv27,
        'uv_火山K歌' uv28,
        'pv_火山启动app_name' pv1,
        'pv_火山启动webcast' pv2,
        'pv_搜索' pv3,
        'pv_直播页卡' pv4,
        'pv_视频页卡' pv5,
        'pv_关注页卡' pv6,
        'pv_观看直播' pv7,
        'pv_分享链接' pv8,
        'pv_微信分享' pv9,
        'pv_微博分享' pv10,
        'pv_朋友圈分享' pv11,
        'pv_qq分享' pv12,
        'pv_qq空间分享' pv13,
        'pv_评论' pv14,
        'pv_查看他人主页' pv15,
        'pv_关注' pv16,
        'pv_发私信' pv17,
        'pv_取消关注' pv18,
        'pv_我的消息' pv19,
        'pv_钱包' pv20,
        'pv_信用卡中心' pv21,
        'pv_充值' pv22,
        'pv_支付宝充值' pv23,
        'pv_微信充值' pv24,
        'pv_发现-同城' pv25,
        'pv_发现' pv26,
        'pv_我的话题圈' pv27,
        'pv_火山K歌' pv28
union all
select  '火山小视频各模块统计分析' model,
        pt_days,
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
        count(distinct(case when tag13=1 then device_number else null end)) uv13,
        count(distinct(case when tag14=1 then device_number else null end)) uv14,
        count(distinct(case when tag15=1 then device_number else null end)) uv15,
        count(distinct(case when tag16=1 then device_number else null end)) uv16,
        count(distinct(case when tag17=1 then device_number else null end)) uv17,
        count(distinct(case when tag18=1 then device_number else null end)) uv18,
        count(distinct(case when tag19=1 then device_number else null end)) uv19,
        count(distinct(case when tag20=1 then device_number else null end)) uv20,
        count(distinct(case when tag21=1 then device_number else null end)) uv21,
        count(distinct(case when tag22=1 then device_number else null end)) uv22,
        count(distinct(case when tag23=1 then device_number else null end)) uv23,
        count(distinct(case when tag24=1 then device_number else null end)) uv24,
        count(distinct(case when tag25=1 then device_number else null end)) uv25,
        count(distinct(case when tag26=1 then device_number else null end)) uv26,
        count(distinct(case when tag27=1 then device_number else null end)) uv27,
        count(distinct(case when tag28=1 then device_number else null end)) uv28,
        sum(case when tag1=1 then 1 else 0 end)  pv1,
        sum(case when tag2=1 then 1 else 0 end)  pv2,
        sum(case when tag3=1 then 1 else 0 end)  pv3,
        sum(case when tag4=1 then 1 else 0 end)  pv4,
        sum(case when tag5=1 then 1 else 0 end)  pv5,
        sum(case when tag6=1 then 1 else 0 end)  pv6,
        sum(case when tag7=1 then 1 else 0 end)  pv7,
        sum(case when tag8=1 then 1 else 0 end)  pv8,
        sum(case when tag9=1 then 1 else 0 end)  pv9,
        sum(case when tag10=1 then 1 else 0 end)  pv10,
        sum(case when tag11=1 then 1 else 0 end)  pv11,
        sum(case when tag12=1 then 1 else 0 end)  pv12,
        sum(case when tag13=1 then 1 else 0 end)  pv13,
        sum(case when tag14=1 then 1 else 0 end)  pv14,
        sum(case when tag15=1 then 1 else 0 end)  pv15,
        sum(case when tag16=1 then 1 else 0 end)  pv16,
        sum(case when tag17=1 then 1 else 0 end)  pv17,
        sum(case when tag18=1 then 1 else 0 end)  pv18,
        sum(case when tag19=1 then 1 else 0 end)  pv19,
        sum(case when tag20=1 then 1 else 0 end)  pv20,
        sum(case when tag21=1 then 1 else 0 end)  pv21,
        sum(case when tag22=1 then 1 else 0 end)  pv22,
        sum(case when tag23=1 then 1 else 0 end)  pv23,
        sum(case when tag24=1 then 1 else 0 end)  pv24,
        sum(case when tag25=1 then 1 else 0 end)  pv25,
        sum(case when tag26=1 then 1 else 0 end)  pv26,
        sum(case when tag27=1 then 1 else 0 end)  pv27,
        sum(case when tag28=1 then 1 else 0 end)  pv28
from 
        (select pt_days,
                device_number,
                hh,
                max(case when instr(url_host,'.snssdk.com')>0 and instr(urltag,'app_name=live_stream')>0 then 1 else 0 end) tag1,
                max(case when url_host='webcast.huoshan.com' then 1 else 0 end) tag2,
                max(case when instr(url_host,'.snssdk.com')>0 and instr(urltag,'hotsoon%query=')>0 then 1 else 0 end) tag3,
                max(case when url_host='webcast.huoshan.com' and instr(urltag,'type=live')>0 then 1 else 0 end) tag4,
                max(case when url_host in ('api-a.huoshan.com','hotsoon-hl.snssdk.com') and instr(urltag,'type=video')>0 then 1 else 0 end) tag5,
                max(case when url_host in ('api-a.huoshan.com','hotsoon-hl.snssdk.com') and instr(urltag,'type=follow')>0 then 1 else 0 end) tag6,
                max(case when url_host in ('quic-huoshan.snssdk.com','webcast-hl.huoshan.com') and instr(urltag,'room/enter') > 0 then 1 else 0 end) tag7,
                max(case when url_host='hotsoon.snssdk.com' and instr(urltag,'share/link_command')>0 then 1 else 0 end) tag8,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'share_way=weixin')>0 then 1 else 0 end) tag9,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'share_way=weibo')>0 then 1 else 0 end) tag10,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'share_way=weixin_timeline')>0 then 1 else 0 end) tag11,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'share_way=qq')>0 then 1 else 0 end) tag12,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'share_way=qzone')>0 then 1 else 0 end) tag13,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'comments')>0 then 1 else 0 end) tag14,
                max(case when url_host in ('hotsoon.snssdk.com','api-hl.huoshan.com') and instr(urltag,'get_profile%to_user_id')>0 then 1 else 0 end) tag15,
                max(case when url_host in ('hotsoon.snssdk.com','api-hl.huoshan.com') and instr(urltag,'_follow')>0 then 1 else 0 end) tag16,
                max(case when url_host in ('hotsoon.snssdk.com','api-hl.huoshan.com') and instr(urltag,'/ichat')>0 then 1 else 0 end) tag17,
                max(case when url_host in ('hotsoon.snssdk.com','api-hl.huoshan.com') and instr(urltag,'_unfollow')>0 then 1 else 0 end) tag18,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'get_notice')>0 then 1 else 0 end) tag19,
                max(case when url_host in ('hotsoon.snssdk.com','api-hl.huoshan.com') and instr(urltag,'wallet')>0 then 1 else 0 end) tag20,
                max(case when url_host='f-moneyloan.snssdk.com' then 1 else 0 end) tag21,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'payment_channels')>0 then 1 else 0 end) tag22,
                max(case when url_host in ('api-a.huoshan.com','api-a-hl.huoshan.com') and instr(urltag,'_buy%way=0')>0 then 1 else 0 end) tag23,
                max(case when url_host in ('api-a.huoshan.com','api-a-hl.huoshan.com') and instr(urltag,'_buy%way=1')>0 then 1 else 0 end) tag24,
                max(case when url_host in ('api-a.huoshan.com','api-a-hl.huoshan.com') and instr(urltag,'type=city')>0 then 1 else 0 end) tag25,
                max(case when url_host in ('api-a.huoshan.com','api-a-hl.huoshan.com') and instr(urltag,'type=find')>0 then 1 else 0 end) tag26,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'hashtag')>0 then 1 else 0 end) tag27,
                max(case when url_host in ('hotsoon.snssdk.com','hotsoon-hl.snssdk.com') and instr(urltag,'karaoke_hot_videos')>0 then 1 else 0 end) tag28
                from user_action_tag_d 
                where  pt_days>='20190610' and pt_days<'20190701'
                 and  (instr(url_host,'.snssdk.com')>0 or instr(url_host,'.huoshan.com')>0)
                group by pt_days,
                        device_number,
                        hh) a
group by pt_days;

select '-- -- -*******************************************************************************************************************-- -- -';

-- -snssdk 标签统计
select 'snssdk-标签统计' model,regexp_extract(urltag,'(app_name=.*?)&',1) app,count(*) from user_action_tag_d  a
where pt_days='20190625' and prov_id='051' and instr(a.url_host,'snssdk.com')>0 
group by regexp_extract(urltag,'(app_name=.*?)&',1);

select '-- -- -*******************************************************************************************************************-- -- -';

-- 各个host的数据统计
select '白名单域名统计量' model,url_host,count(*) cnt,count(distinct device_number) dau
from user_action_tag_d where  pt_days='20190625'  group by url_host;


