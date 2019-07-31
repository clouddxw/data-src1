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