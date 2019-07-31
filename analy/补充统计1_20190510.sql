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
                  from  user_action_app_m 
                where  pt_days='201904'
                 and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')             
                  ) a
        group by device_number) b
where cnt>=2;


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
              max(case when appname='qq_music' then 1 else 0 end) qq_flg,
              max(case when appname='kuwo' then 1 else 0 end) kw_flg,
              max(case when appname='wangyi_music' then 1 else 0 end) wy_flg,
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