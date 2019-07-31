drop table if exists tmp_user_app_yy_t1;
create table tmp_user_app_yy_t1 as
select t.*,
       t1.device_number device_number1
    from 
        user_action_app_m  t
     left join 
        (select device_number 
         from zba_dwa.dwa_v_m_cus_nm_user_info b  
         where b.month_id='201904'
         and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t1
      on t.device_number=t1.device_number;


drop table if exists tmp_user_app_yy_t2;
create table tmp_user_app_yy_t2 as
select t.*,
       t1.device_number device_number1
    from 
        user_action_app_d  t
     left join 
        (select device_number 
         from zba_dwa.dwa_v_m_cus_nm_user_info b  
         where b.month_id='201904'
         and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t1
      on t.device_number=t1.device_number;


drop table if exists tmp_user_app_yy_t3;
create table tmp_user_app_yy_t3 as
select          pt_days,
                appname,
                device_number,
                device_number1,
                row_number() over(partition by appname,device_number order by pt_days asc) rn,
                row_number() over(partition by appname,device_number1 order by pt_days asc) rn1
            from  
            (select  t.*,
                     t1.device_number device_number1
                from 
                  user_action_app_d t
                left join 
                    (select device_number 
                    from zba_dwa.dwa_v_m_cus_nm_user_info b  
                     where b.month_id='201904'
                       and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                            '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                            '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                            '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t1
              on t.device_number=t1.device_number 
            where t.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')   
              and t.pt_days>='20190301')  a;
 



---各个APP的月活

select '201905' month_id,
       'app月活' model,
        appname,
        count(distinct device_number) uv
    from tmp_user_app_yy_t1
    where pt_days='201905'
    group by appname;

select '201905' month_id,
       '剔除流量卡-app月活' model,
        appname,
        count(distinct device_number1) uv1
    from tmp_user_app_yy_t1
    where pt_days='201905'
    group by appname;


--新闻行业月活
select '201905' month_id,
       'app月活' model,
        count(distinct device_number) uv
      from  tmp_user_app_yy_t1
     where pt_days='201905'
     and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng');


select '201905' month_id,
       '剔除流量卡-新闻行业月活' model,
        count(distinct device_number1) uv
      from  tmp_user_app_yy_t1
     where pt_days='201905'
     and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng');

    

---各家新闻独占

select '201905' month_id,
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
                 from  tmp_user_app_yy_t1
                 where pt_days = '201905'
                   and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')             
                  ) a) a
      group by device_number) b
  where cnt=1
  group by top1_app;



--新闻两两重合
select '201905' month_id,
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
      '201905' month_id,
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
          (select appname,
                      device_number,
                      pv
                 from  tmp_user_app_yy_t1
                 where pt_days = '201905'
                  and  appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')             
                  ) a
        group by device_number) b
where cnt>=2;






--重合用户偏好
select '201905' month_id,
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
                 from  tmp_user_app_yy_t1
                 where pt_days = '201905'
                   and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')             
                  ) a) b
    group by  device_number) c
    where cnt>=2
group by top1_app;


select '201905' month_id,
      '各家app永久新增' model,
       appname,
       pt_days,
       count(distinct device_number) uv
 from  
      tmp_user_app_yy_t3
 where rn=1
group by pt_days,
         appname
order by appname,
         pt_days;



select    '201905' month_id,
          '各家app留存' model,
          a.appname,
          count(distinct a.device_number) uv
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201904'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')       
        and exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  tmp_user_app_yy_t1
                  where pt_days='201905'
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')       
                  ) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)
        group by a.appname;


select '201905' month_id,
       '新闻各app新增来源' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
(select device_number,
        appname tarapp
     from 
       tmp_user_app_yy_t3
    where pt_days>='20190501' and pt_days<'20190601' and rn=1
      and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') 
  group by device_number,
           appname) b 
 left join 
        (select
                device_number,
                appname
            from  tmp_user_app_yy_t1
            where pt_days in ('201903','201904')
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;



select '201905' month_id,
       '各app新增-次月留存' model,
       b.appname,
       count(distinct b.device_number) uv
 from 
(select device_number,
        appname 
     from 
       tmp_user_app_yy_t3
    where pt_days>='20190401' and pt_days<'20190501' and rn=1
  group by device_number,
           appname) b 
  where exists 
    (select * 
         from 
       (select
                device_number,
                appname
            from  tmp_user_app_yy_t1
            where pt_days = '201905'      
            )  b1
        where  b.device_number=b1.device_number
          and  b.appname=b1.appname) 
group by b.appname;







select '201905' month_id,
      '新闻各app流失去向' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
     (select
          a.device_number,
          a.appname tarapp
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201903'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  tmp_user_app_yy_t1
                  where pt_days in ('201904','201905')
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')  
                  group by device_number,
                           appname) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)) b 
 left join 
        (select
                device_number,
                appname
            from  tmp_user_app_yy_t1
            where pt_days in ('201904','201905')
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') 
            group by device_number,appname)  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;





----------------***********************************************************************----------------


select '201905' month_id,
       '各家app永久新增--剔除流量卡' model,
       appname,
       pt_days,
       count(distinct device_number1) uv
 from  
     tmp_user_app_yy_t3
where rn1=1
group by pt_days,
         appname
order by appname,
         pt_days;

select   '201905' month_id,
         '各家app留存--剔除流浪卡' model,
          a.appname,
          count(distinct a.device_number1) uv
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201904'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')       
        and exists
          (select * from 
              (select
                      device_number1,
                      appname
                  from  tmp_user_app_yy_t1
                  where pt_days='201905'
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng','qq_music','kuwo','kugou','wangyi_music')       
                  ) a1 
              where a.appname=a1.appname and a.device_number1=a1.device_number1)
        group by a.appname;


select '201905' month_id,
       '新闻各app新增来源--剔除流浪卡' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number1) uv
 from 
  (select device_number1,
          appname tarapp
      from 
          tmp_user_app_yy_t3
      where rn1=1 and pt_days>='20190501' and pt_days<'20190601'
    group by device_number1,
            appname) b 
 left join 
        (select
                device_number1,
                appname
            from  tmp_user_app_yy_t1
            where pt_days in ('201903','201904')
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
            )  b1
on  b.device_number1=b1.device_number1
group by b.tarapp,
         b1.appname;



select '201905' month_id,
       '各app新增-次月留存--剔除流量卡' model,
       b.appname,
       count(distinct b.device_number) uv
 from 
(select device_number1,
        appname 
     from 
       tmp_user_app_yy_t3
    where pt_days>='20190401' and pt_days<'20190501' and rn1=1
  group by device_number1,
           appname) b 
  where exists 
    (select * 
         from 
       (select
                device_number1,
                appname
            from  tmp_user_app_yy_t1
            where pt_days = '201905'      
            )  b1
        where  b.device_number1=b1.device_number1
          and  b.appname=b1.appname) 
group by b.appname;


select '201905' month_id,
      '新闻各app流失去向---剔除流量卡' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number1) uv
 from 
     (select
          a.device_number1,
          a.appname tarapp
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201903'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
        and not exists
          (select * from 
              (select
                      device_number1,
                      appname
                  from  tmp_user_app_yy_t1
                  where pt_days in ('201904','201905')
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
                  ) a1 
              where a.appname=a1.appname and a.device_number1=a1.device_number1)) b 
 left join 
        (select
                device_number1,
                appname
            from  tmp_user_app_yy_t1
            where pt_days in ('201904','201905')
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
            )  b1
on  b.device_number1=b1.device_number1
group by b.tarapp,
         b1.appname;

---------------*************************************************************************************---------


----------------------------------------------------音乐----------------------------------------------------------

--音乐行业的月活
select '201905' month_id,
      '音乐行业月活' model,
       count(distinct device_number)
      from  tmp_user_app_yy_t1
     where pt_days='201905'
     and appname in ('qq_music','kuwo','kugou','wangyi_music');




---各家音乐APP独占

select '201905' month_id,
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
                  from  tmp_user_app_yy_t1 
                where  pt_days='201905'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a ) a
      group by device_number) b
  where cnt=1
  group by top1_app;





--音乐APP两两重合
select '201905' month_id,
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
              max(case when appname='qq_music' then 1 else 0 end) qq_flg,
              max(case when appname='kuwo' then 1 else 0 end) kw_flg,
              max(case when appname='wangyi_music' then 1 else 0 end) wy_flg,
              max(case when appname='kugou' then 1 else 0 end) kg_flg
          from 
            (select appname,
                      device_number,
                      pv
                  from  tmp_user_app_yy_t1 
                where  pt_days='201905'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a
        group by device_number) b
where cnt>=2;


--酷我重合偏好
select
    '201905' month_id,
    '酷我重合偏好' model,
    a1.appname,
    count(distinct a.device_number) uv
  from 
    (select a.device_number
        from 
          (select device_number,
                count(distinct appname) cnt,
                concat_ws('&',collect_set(appname)) appname
          from  tmp_user_app_yy_t1 
          where  pt_days='201905'
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
                  from  tmp_user_app_yy_t1 
                where  pt_days='201905'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a) a1
on a.device_number = a1.device_number
where a1.rn=1
group by a1.appname;




--酷狗重合偏好
select
    '201905' month_id,
    '酷狗重合偏好' model,
    a1.appname,
    count(distinct a.device_number) uv
  from 
    (select a.device_number
        from 
          (select device_number,
                count(distinct appname) cnt,
                concat_ws('&',collect_set(appname)) appname
          from  tmp_user_app_yy_t1 
          where  pt_days='201905'
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
                  from  tmp_user_app_yy_t1 
                where  pt_days='201905'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a) a1
on a.device_number = a1.device_number
where a1.rn=1
group by a1.appname;



--网易重合爱好
select
   '201905' month_id,
   '网易云重合偏好' model,
    a1.appname,
    count(distinct a.device_number) uv
  from 
    (select a.device_number
        from 
          (select device_number,
                count(distinct appname) cnt,
                concat_ws('&',collect_set(appname)) appname
          from  tmp_user_app_yy_t1 
          where  pt_days='201905'
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
                  from  tmp_user_app_yy_t1 
                where  pt_days='201905'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a) a1
on a.device_number = a1.device_number
where a1.rn=1
group by a1.appname;


--qq音乐重合爱好
select
    '201905' month_id,
    'qq音乐重合爱好' model,
     a1.appname,
     count(distinct a.device_number) uv
  from 
    (select a.device_number
        from 
          (select device_number,
                count(distinct appname) cnt,
                concat_ws('&',collect_set(appname)) appname
          from  tmp_user_app_yy_t1 
          where  pt_days='201905'
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
                  from  tmp_user_app_yy_t1 
                where  pt_days='201905'
                 and appname in ('qq_music','kuwo','kugou','wangyi_music')             
                  ) a) a1
on a.device_number = a1.device_number
where a1.rn=1
group by a1.appname;



-------------------------------




select '201905' month_id,
      '音乐各app新增来源' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
(select device_number,
        appname tarapp
     from  tmp_user_app_yy_t3
   where appname in ('qq_music','kuwo','kugou','wangyi_music') 
     and rn=1
     and pt_days>='20190501' and pt_days<'20190601'
  group by device_number,
           appname) b 
 left join 
        (select
                device_number,
                appname
            from  tmp_user_app_yy_t1
            where pt_days in ('201903','201904')
             and appname in ('qq_music','kuwo','kugou','wangyi_music')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;





select '201905' month_id,
      '音乐各app流失去向' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
     (select
          a.device_number,
          a.appname tarapp
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201903'
        and a.appname in ('qq_music','kuwo','kugou','wangyi_music')       
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from tmp_user_app_yy_t1
                where pt_days in ('201904','201905')
                  and appname in ('qq_music','kuwo','kugou','wangyi_music')  
                  group by device_number,appname ) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)) b 
 left join 
        (select
                device_number,
                appname
            from  tmp_user_app_yy_t1
            where pt_days in ('201904','201905')
             and appname in ('qq_music','kuwo','kugou','wangyi_music')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;


select  '忠诚用户统计' model,
        '201905' month_id,
        'app' appname,
        '活跃2天及以上用户量' cnt2,
        '活跃3天及以上用户量' cnt3,
        '活跃4天及以上用户量' cnt4,
        '活跃5天及以上用户量' cnt5
union all
select '忠诚用户统计' model,
      '201905' month_id,
       appname,
       count(distinct(case when tag_m1=1 and tag_m2=1 and cast(ad_m1 as int)>=2 and cast(ad_m2 as int)>=2 then device_number else null end)) cnt2,
       count(distinct(case when tag_m1=1 and tag_m2=1 and cast(ad_m1 as int)>=3 and cast(ad_m2 as int)>=3 then device_number else null end)) cnt3,
       count(distinct(case when tag_m1=1 and tag_m2=1 and cast(ad_m1 as int)>=4 and cast(ad_m2 as int)>=4 then device_number else null end)) cnt4,
       count(distinct(case when tag_m1=1 and tag_m2=1 and cast(ad_m1 as int)>=5 and cast(ad_m2 as int)>=5 then device_number else null end)) cnt5
  from 
  (select 
        appname,
        device_number,
        max(case when pt_days='201904' then 1 else 0 end) tag_m1,
        max(case when pt_days='201905' then 1 else 0 end) tag_m2,
        max(case when pt_days='201904' then active_days else 0 end) ad_m1,
        max(case when pt_days='201905' then active_days else 0 end) ad_m2
      from tmp_user_app_yy_t1
      where  pt_days in ('201904','201905')
      group by appname,
              device_number) a
  group by appname;
             

select  '忠诚用户统计' model,
        '201904' month_id,
        'app' appname,
        '活跃2天及以上用户量' cnt2,
        '活跃3天及以上用户量' cnt3,
        '活跃4天及以上用户量' cnt4,
        '活跃5天及以上用户量' cnt5
union all
select '忠诚用户统计' model,
       '201904' month_id,
       appname,
       count(distinct(case when tag_m1=1 and tag_m2=1 and cast(ad_m1 as int)>=2 and cast(ad_m2 as int)>=2 then device_number else null end)) cnt2,
       count(distinct(case when tag_m1=1 and tag_m2=1 and cast(ad_m1 as int)>=3 and cast(ad_m2 as int)>=3 then device_number else null end)) cnt3,
       count(distinct(case when tag_m1=1 and tag_m2=1 and cast(ad_m1 as int)>=4 and cast(ad_m2 as int)>=4 then device_number else null end)) cnt4,
       count(distinct(case when tag_m1=1 and tag_m2=1 and cast(ad_m1 as int)>=5 and cast(ad_m2 as int)>=5 then device_number else null end)) cnt5
  from 
  (select 
        appname,
        device_number,
        max(case when pt_days='201903' then 1 else 0 end) tag_m1,
        max(case when pt_days='201904' then 1 else 0 end) tag_m2,
        max(case when pt_days='201903' then active_days else 0 end) ad_m1,
        max(case when pt_days='201904' then active_days else 0 end) ad_m2
      from tmp_user_app_yy_t1
      where  pt_days in ('201903','201904')
      group by appname,
              device_number) a
  group by appname;

--------------------------------------------


select    '新闻各app流失用户-4月' model,
          a.appname,
          count(distinct a.device_number) uv
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201903'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  tmp_user_app_yy_t1
                  where pt_days = '201904'
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')  
                  group by device_number,
                           appname) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)
      group by a.appname;


select    '新闻各app流失用户-5月' model,
          a.appname,
          count(distinct a.device_number) uv
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201903'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  tmp_user_app_yy_t1
                  where pt_days in ('201904','201905') 
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')  
                  group by device_number,
                           appname) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)
      group by a.appname;




select    '音乐各app流失用户-4月' model,
          a.appname,
          count(distinct a.device_number) uv
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201903'
        and a.appname in ('qq_music','kuwo','kugou','wangyi_music')      
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  tmp_user_app_yy_t1
                  where pt_days = '201904'
                  and appname in ('qq_music','kuwo','kugou','wangyi_music') 
                  group by device_number,
                           appname) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)
      group by a.appname;


select    '音乐各app流失用户-5月' model,
          a.appname,
          count(distinct a.device_number) uv
      from  tmp_user_app_yy_t1 a
      where a.pt_days='201903'
        and a.appname in ('qq_music','kuwo','kugou','wangyi_music')    
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  tmp_user_app_yy_t1
                  where pt_days in ('201904','201905') 
                  and appname in ('qq_music','kuwo','kugou','wangyi_music') 
                  group by device_number,
                           appname) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)
      group by a.appname;


---Top200 App 
select '手百TOP200APP' model,
       t.prod_name,
       count(distinct t.device_number) uv,
       sum(visit_cnt) pv,
       sum(visit_dura) dur
from 
    zb_dwa.dwa_m_ia_basic_user_app t,
    (select device_number from  user_action_app_m where pt_days='201905' and appname='mobbaidu') t1
 where t.device_number=t1.device_number
   and t.month_id='201905'
group by t.prod_name;




----联通全量Top200App
select '联通全量Top200App' model,
       t.prod_name,
       count(distinct t.device_number) uv,
       sum(visit_cnt) pv,
       sum(visit_dura) dur
from 
    zb_dwa.dwa_m_ia_basic_user_app t
 where t.month_id='201905';

drop table if exists tmp_user_app_yy_t1;
drop table if exists tmp_user_app_yy_t2;
drop table if exists tmp_user_app_yy_t3;