---各个APP的月活
select appname,
       count(distinct device_number) uv
    from user_action_app_m
    where pt_days='201904'
    group by appname;
  
--新闻行业月活
select '新闻行业月活' model,
       count(distinct device_number)
      from  user_action_app_m
     where pt_days='201904'
     and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng');

    

---各家新闻独占

select '各家新闻独占' model,
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
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')             
                  ) a) a
      group by device_number) b
  where cnt=1
  group by top1_app;


--新闻两两重合
select 
      '新闻两两重合' model,
      'qq&&toutiao' qq_toutiao	,
      'qq&&wangyi' qq_wangyi	,
      'qq&&kuaibao' qq_kuaibao	,
      'qq&&sina' qq_sina	,
      'qq&&ifeng' qq_ifeng	,
      'qq&&sohu' qq_sohu	,
      'toutiao&&wangyi' toutiao_wangyi	,
      'toutiao&&kuaibao' toutiao_kuaibao	,
      'toutiao&&sina' toutiao_sina	,
      'toutiao&&ifeng' toutiao_ifeng	,
      'toutiao&&sohu' toutiao_sohu	,
      'wangyi&&kuaibao' wangyi_kuaibao	,
      'wangyi&&sina' wangyi_sina	,
      'wangyi&&ifeng' wangyi_ifeng	,
      'wangyi&&sohu' wangyi_sohu	,
      'kuaibao&&sina' kuaibao_sina	,
      'kuaibao&&ifeng' kuaibao_ifeng	,
      'kuaibao&&sohu' kuaibao_sohu	,
      'sina&&ifeng' sina_ifeng	,
      'sina&&sohu' sina_sohu	,
      'ifeng&&sohu' ifeng_sohu	
union  all 
select 
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
              max(case when appname='qqnews' then 1 else 0 end) qq_flg,
              max(case when appname='kuaibao' then 1 else 0 end) kb_flg,
              max(case when appname='wangyinews' then 1 else 0 end) wy_flg,
              max(case when appname='toutiao' then 1 else 0 end) tt_flg,
              max(case when appname='sinanews' then 1 else 0 end) sina_flg,
              max(case when appname='ifeng' then 1 else 0 end) if_flg,
              max(case when appname='sohunews' then 1 else 0 end) sh_flg
          from 
          (select appname,
                      device_number,
                      pv
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')             
                  ) a
        group by device_number) b
where cnt>=2;






--重合用户偏好
select '新闻APP重合用户偏好' model,
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
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')             
                  ) a) b
    group by  device_number) c
    where cnt>=2
group by top1_app;



----------------------------------------------------音乐----------------------------------------------------------

--音乐行业的月活
select '音乐行业月活' model,
       count(distinct device_number)
      from  user_action_app_m
     where pt_days='201904'
     and appname in ('qq_music','kuwo','kugou','wangyi_music');




---各家音乐APP独占

select '各家音乐APP独占' model,
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
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a ) a
      group by device_number) b
  where cnt=1
  group by top1_app;





--音乐APP两两重合
select 
      '音乐APP两两重合' model,
      'qq&&kuwo' qq_kuwo ,
      'qq&&wangyi' qq_wangyi ,
      'qq&&kugou' qq_kugou ,
      'kuwo&&wangyi' kuwo_wangyi ,
      'kuwo&&kugou' kuwo_kugou ,
      'wangyi&&kugou' wangyi_kugou 
union all 
select 
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
              max(case when appname='qqmusic' then 1 else 0 end) qq_flg,
              max(case when appname='kuwo' then 1 else 0 end) kw_flg,
              max(case when appname='wangyi' then 1 else 0 end) wy_flg,
              max(case when appname='kugou' then 1 else 0 end) kg_flg
          from 
            (select appname,
                      device_number,
                      pv
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a
        group by device_number) b
where cnt>=2;


--酷我重合偏好
select
    '酷我重合偏好' model,
    a1.appname,
    count(distinct a.device_number) uv
  from 
    (select a.device_number
        from 
          (select device_number,
                count(distinct appname) cnt,
                concat_ws('&',collect_set(appname)) appname
          from  user_action_app_m 
          where  pt_days='201904'
            and  appname in ('qq_music','kuwo','kugou','wangyi_music')
            group by device_number) a
        where instr(appname,'kuwo')>0
          and cnt>=2 ) a
join 
    (select appname,
            device_number,
            row_number() over(partition by device_number order by pv desc) rn 
        from 
            (select appname,
                      device_number,
                      pv
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a) a1
on a.device_number = a1.device_number
where a1.rn=1
group by a1.appname;




--酷狗重合偏好
select
    '酷狗重合偏好' model,
    a1.appname,
    count(distinct a.device_number) uv
  from 
    (select a.device_number
        from 
          (select device_number,
                count(distinct appname) cnt,
                concat_ws('&',collect_set(appname)) appname
          from  user_action_app_m 
          where  pt_days='201904'
            and  appname in ('qq_music','kuwo','kugou','wangyi_music')
            group by device_number) a
        where instr(appname,'kugou')>0
          and cnt>=2 ) a
join 
    (select appname,
            device_number,
            row_number() over(partition by device_number order by pv desc) rn 
        from 
            (select appname,
                      device_number,
                      pv
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a) a1
on a.device_number = a1.device_number
where a1.rn=1
group by a1.appname;



--网易重合爱好
select
   '网易云重合偏好' model,
    a1.appname,
    count(distinct a.device_number) uv
  from 
    (select a.device_number
        from 
          (select device_number,
                count(distinct appname) cnt,
                concat_ws('&',collect_set(appname)) appname
          from  user_action_app_m 
          where  pt_days='201904'
            and  appname in ('qq_music','kuwo','kugou','wangyi_music')
            group by device_number) a
        where instr(appname,'wangyi_music')>0
          and cnt>=2 ) a
join 
    (select appname,
            device_number,
            row_number() over(partition by device_number order by pv desc) rn 
        from 
            (select appname,
                      device_number,
                      pv
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a) a1
on a.device_number = a1.device_number
where a1.rn=1
group by a1.appname;


--qq音乐重合爱好
select
   'qq音乐重合爱好' model,
    a1.appname,
    count(distinct a.device_number) uv
  from 
    (select a.device_number
        from 
          (select device_number,
                count(distinct appname) cnt,
                concat_ws('&',collect_set(appname)) appname
          from  user_action_app_m 
          where  pt_days='201904'
            and  appname in ('qq_music','kuwo','kugou','wangyi_music')
            group by device_number) a
        where instr(appname,'qq_music')>0
          and cnt>=2 ) a
join 
    (select appname,
            device_number,
            row_number() over(partition by device_number order by pv desc) rn 
        from 
            (select appname,
                      device_number,
                      pv
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a) a1
on a.device_number = a1.device_number
where a1.rn=1
group by a1.appname;



-------------------------------

select '各家app永久新增' model,
       appname,
       pt_days,
       count(distinct device_number) uv
 from  
        (select pt_days,
                appname,
                device_number,
                row_number() over(partition by appname,device_number order by pt_days asc) rn
            from  user_action_app_d
            where appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')             
            )  a
where rn=1
group by pt_days,
         appname
order by appname,
         pt_days;



select    '各家app留存' model,
          a.appname,
          count(distinct a.device_number) uv
      from  user_action_app_m a
      where a.pt_days='201903'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')       
        and  exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  user_action_app_m
                  where pt_days='201904'
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')       
                  ) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)
        group by a.appname;


select '新闻各app新增来源' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
(select device_number,
        appname tarapp
     from 
       (select  pt_days,
                appname,
                device_number,
                row_number() over(partition by appname,device_number order by pt_days asc) rn
            from  user_action_app_d
            where appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')             
            )  a
where rn=1
  and pt_days>='20190401' and pt_days<'20190501'
  group by device_number,
           appname) b 
 left join 
        (select
                device_number,
                appname
            from  user_action_app_m
            where pt_days='201903'
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;





select '新闻各app流失去向' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
     (select
          device_number,
          appname
      from  user_action_app_m a
      where a.pt_days='201903'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  user_action_app_m
                  where pt_days='201904'
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
                  ) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)) b 
 left join 
        (select
                device_number,
                appname
            from  user_action_app_m
            where pt_days='201904'
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;



select '音乐各app新增来源' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
(select device_number,
        appname tarapp
     from 
       (select  pt_days,
                appname,
                device_number,
                row_number() over(partition by appname,device_number order by pt_days asc) rn
            from  user_action_app_d
            where appname in ('qq_music','kuwo','kugou','wangyi_music')             
            )  a
where rn=1
  and pt_days>='20190401' and pt_days<'20190501'
  group by device_number,
           appname) b 
 left join 
        (select
                device_number,
                appname
            from  user_action_app_m
            where pt_days='201903'
             and appname in ('qq_music','kuwo','kugou','wangyi_music')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;





select '音乐各app流失去向' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
     (select
          device_number,
          appname
      from  user_action_app_m a
      where a.pt_days='201903'
        and a.appname in ('qq_music','kuwo','kugou','wangyi_music')       
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  user_action_app_m
                  where pt_days='201904'
                  and appname in ('qq_music','kuwo','kugou','wangyi_music')       
                  ) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)) b 
 left join 
        (select
                device_number,
                appname
            from  user_action_app_m
            where pt_days='201904'
             and appname in ('qq_music','kuwo','kugou','wangyi_music')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;
