select
        '补充统计' model,
        '日期' pt_days,
        'uv_酷我会员' uv1,
        'uv_酷狗每日推' uv2,
        'uv_喜马拉雅直播' uv3,
        'uv_喜马拉雅助眠冥想' uv4,
        'pv_酷我会员' pv1,
        'pv_酷狗每日推' pv2,
        'pv_喜马拉雅直播' pv3,
        'pv_喜马拉雅助眠冥想' pv4;
select '补充统计' model,
        pt_days,
        count(distinct(case when tag1=1 then device_number else null end)) uv1,
        count(distinct(case when tag2=1 then device_number else null end)) uv2,
        count(distinct(case when tag3=1 then device_number else null end)) uv3,
        count(distinct(case when tag4=1 then device_number else null end)) uv4,
        sum(case when tag1=1 then 1 else 0 end) pv1,
        sum(case when tag2=1 then 1 else 0 end) pv2,
        sum(case when tag3=1 then 1 else 0 end) pv3,
        sum(case when tag4=1 then 1 else 0 end) pv4
    from
        (select pt_days,
                device_number,
                hh,
                max(case when (instr(url_host,'.kuwo.cn')>0 and instr(urltag,'isVip=1')>0) or (url_host='musicpay.kuwo.cn' and instr(urltag,'ptype=vip')>0) then 1 else 0 end) tag1, -- 20190720修改会员规则
                max(case when url_host='ads.service.kugou.com' and instr(urltag,'daily_focus')>0 then 1 else 0 end) tag2,
                max(case when url_host in ('liveroom.ximalaya.com','mobwsa.ximalaya.com') and instr(urltag,'roomId') then 1 else 0 end) tag3,
                max(case when instr(url_host,'.ximalaya.com') and instr(urltag,'sleep/theme')>0 then 1 else 0 end) tag4
            from  user_action_tag_d
            where pt_days>='20190701' and pt_days<='20190731'
              and (instr(url_host,'.kuwo.cn')>0 or url_host='ads.service.kugou.com' or instr(url_host,'.ximalaya.com')>0)
              group BY
                pt_days,
                device_number,
                hh) a
     group by pt_days;
