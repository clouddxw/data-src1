

-----------------------------------------------新闻行业用户属性统计-----------------------------------


select  '新闻行业' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '新闻行业' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '新闻行业' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '新闻行业' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '新闻行业' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '新闻行业' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '新闻行业' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;


-------------------------------------------------头条属性分布------------------------------------------------


select  '头条' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '头条' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '头条' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '头条' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '头条' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '头条' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '头条' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;

----------------------------------------------------腾讯新闻属性分布--------------------------------------------


select  '腾讯新闻' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '腾讯新闻' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '腾讯新闻' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '腾讯新闻' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '腾讯新闻' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '腾讯新闻' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '腾讯新闻' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;


-------------------------------------------------网易新闻属性分布----------------------------------------


select  '网易新闻' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyinews' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '网易新闻' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyinews' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '网易新闻' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyinews' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '网易新闻' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyinews' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '网易新闻' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyinews' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '网易新闻' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyinews' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '网易新闻' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyinews' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;




-------------------------------------------------快报属性分布----------------------------------------


select  '快报' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuaibao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '快报' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuaibao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '快报' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuaibao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '快报' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuaibao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '快报' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuaibao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '快报' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuaibao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '快报' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuaibao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;




-------------------------------------------------搜狐新闻属性分布----------------------------------------


select  '搜狐新闻' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sohu_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '搜狐新闻' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sohu_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '搜狐新闻' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sohu_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '搜狐新闻' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sohu_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '搜狐新闻' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sohu_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '搜狐新闻' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sohu_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '搜狐新闻' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sohu_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;




-------------------------------------------------凤凰新闻属性分布----------------------------------------


select  '凤凰新闻' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='ifeng' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '凤凰新闻' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='ifeng' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '凤凰新闻' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='ifeng' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '凤凰新闻' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='ifeng' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '凤凰新闻' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='ifeng' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '凤凰新闻' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='ifeng' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '凤凰新闻' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='ifeng' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;



-------------------------------------------------新浪新闻属性分布----------------------------------------


select  '新浪新闻' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sina_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '新浪新闻' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sina_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '新浪新闻' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sina_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '新浪新闻' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sina_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '新浪新闻' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sina_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '新浪新闻' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sina_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '新浪新闻' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='sina_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;





-----------------------------------------------音乐行业用户属性统计-----------------------------------


select  '音乐行业' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('qq_music','kuwo','kugou','wangyi_music') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '音乐行业' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('qq_music','kuwo','kugou','wangyi_music') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '音乐行业' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('qq_music','kuwo','kugou','wangyi_music') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '音乐行业' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('qq_music','kuwo','kugou','wangyi_music') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '音乐行业' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('qq_music','kuwo','kugou','wangyi_music') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '音乐行业' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('qq_music','kuwo','kugou','wangyi_music') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '音乐行业' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('qq_music','kuwo','kugou','wangyi_music') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;



-----------------------------QQ音乐属性分布--------------------------------


select  'qq音乐' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='qq_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  'qq音乐' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='qq_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  'qq音乐' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='qq_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  'qq音乐' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='qq_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  'qq音乐' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='qq_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  'qq音乐' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='qq_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  'qq音乐' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='qq_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;

---------------------------------酷狗属性分布-------------------------------


select  '酷狗' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kugou' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '酷狗' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kugou' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '酷狗' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kugou' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '酷狗' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kugou' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '酷狗' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kugou' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '酷狗' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kugou' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '酷狗' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kugou' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;



---------------------------------------------网易云属性分布-------------------------------------


select  '网易云' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyi_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '网易云' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyi_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '网易云' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyi_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '网易云' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyi_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '网易云' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyi_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '网易云' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyi_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '网易云' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='wangyi_music' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;


-------------------------------------------酷我属性分布-------------------------------------


select  '酷我' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuwo' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '酷我' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuwo' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '酷我' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuwo' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '酷我' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuwo' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '酷我' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuwo' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '酷我' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuwo' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '酷我' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='kuwo' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;


-------------------------------------头条内容点击属性分布--------------------------------


select  '头条内容点击' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select   device_number 
                from user_action_context_d
           where pt_days>='20190401' and pt_days<'20190501'
                and model='toutiao_article'
          group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_province;

select  '头条内容点击' app,
        '常驻城市分布' model,
        t1.top1_home_city,
        count(*) cnt
    from 
        (select   device_number 
                from user_action_context_d
           where pt_days>='20190401' and pt_days<'20190501'
                and model='toutiao_article'
          group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.top1_home_city;


select  '头条内容点击' app,
        '品牌分布' model,
        t1.factory_desc,
        count(*) cnt
    from 
        (select   device_number 
                from user_action_context_d
           where pt_days>='20190401' and pt_days<'20190501'
                and model='toutiao_article'
          group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.factory_desc;


select  '头条内容点击' app,
        '证件归属地分布' model,
        t1.cert_prov,
        count(*) cnt
    from 
        (select   device_number 
                from user_action_context_d
           where pt_days>='20190401' and pt_days<'20190501'
                and model='toutiao_article'
          group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cert_prov;


select  '头条内容点击' app,
        '性别分布' model,
        t1.cust_sex,
        count(*) cnt
    from 
        (select   device_number 
                from user_action_context_d
           where pt_days>='20190401' and pt_days<'20190501'
                and model='toutiao_article'
          group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cust_sex;



select  '头条内容点击' app,
        '消费水平分布' model,
        t1.cost_level,
        count(*) cnt
    from 
        (select   device_number 
                from user_action_context_d
           where pt_days>='20190401' and pt_days<'20190501'
                and model='toutiao_article'
          group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.cost_level;



select  '头条内容点击' app,
        '年龄分布' model,
        t1.age_range,
        count(*) cnt
    from 
        (select   device_number 
                from user_action_context_d
           where pt_days>='20190401' and pt_days<'20190501'
                and model='toutiao_article'
          group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    group by t1.age_range;

---------------------------头条内容点击pv分布--------------------------------------------

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
        where pt_days>='20190401' and pt_days<'20190501'
            and model='toutiao_article'
        group by device_number) t
    group by pv_range;










