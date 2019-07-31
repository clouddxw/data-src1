
use research_dep01;
select  '新闻行业-关联3月属性' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
     on t.device_number=t1.device_number
     inner join 
        (select device_number 
           from zba_dwa.dwa_v_m_cus_nm_user_info b  
           where b.month_id='201904'
             and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                        '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                        '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                        '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t2
      on t.device_number=t2.device_number
    group by t1.top1_home_province;

select  '头条-关联3月属性' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t    
     left join 
        (select * from user_info_sample where pt_days='201903') t1
    on t.device_number=t1.device_number
    inner join 
    (select device_number 
        from zba_dwa.dwa_v_m_cus_nm_user_info b  
        where b.month_id='201904'
            and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                    '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                    '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                    '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t2
    on t.device_number=t2.device_number
    group by t1.top1_home_province;


select  '腾讯新闻-关联3月属性' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201903') t1
     on t.device_number=t1.device_number
    inner join 
    (select device_number 
        from zba_dwa.dwa_v_m_cus_nm_user_info b  
        where b.month_id='201904'
            and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                    '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                    '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                    '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t2
    on t.device_number=t2.device_number
    group by t1.top1_home_province;



select  '新闻行业-关联4月属性' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') group by device_number) t     
     left join 
        (select * from user_info_sample where pt_days='201904') t1
     on t.device_number=t1.device_number
     inner join 
        (select device_number 
           from zba_dwa.dwa_v_m_cus_nm_user_info b  
           where b.month_id='201904'
             and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                        '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                        '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                        '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t2
      on t.device_number=t2.device_number    
    group by t1.top1_home_province;

select  '头条-关联4月属性' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='toutiao' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201904') t1
     on t.device_number=t1.device_number
     inner join 
        (select device_number 
           from zba_dwa.dwa_v_m_cus_nm_user_info b  
           where b.month_id='201904'
             and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                        '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                        '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                        '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t2
      on t.device_number=t2.device_number
    group by t1.top1_home_province;


select  '腾讯新闻-关联4月属性' app,
        '常驻省份分布' model,
        t1.top1_home_province,
        count(*) cnt
    from 
        (select device_number from  user_action_app_m  where pt_days='201904' and appname='tencent_news' group by device_number) t
     left join 
        (select * from user_info_sample where pt_days='201904') t1
       on t.device_number=t1.device_number
     inner join 
        (select device_number 
           from zba_dwa.dwa_v_m_cus_nm_user_info b  
           where b.month_id='201904'
             and b.product_class not in ( '90063345','90065147','90065148','90109876','90155946','90163731','90209546','90209558','90284307','90284309',
                                        '90304110','90308773','90305357','90331359','90315327','90357729','90310454','90310862','90348254','90348246',
                                        '90350506','90351712','90359657','90361349','90391107','90395536','90396856','90352112','90366057','90373707',
                                        '90373724','90412690','90404157','90430815','90440508','90440510','90440512','90461694')) t2
      on t.device_number=t2.device_number
    group by t1.top1_home_province;