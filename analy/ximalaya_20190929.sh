#!/bin/bash

#喜马拉雅点击提取脚本


v_sdate=20190814
v_edate=20190909
v_date=${v_sdate}

while [ ${v_date} -le ${v_edate}  ]
do

      hive -e"
      use lf_airui_pro;
      set mapreduce.job.queuename=ia_dzh_airui;
      set mapreduce.map.memory.mb = 4096;
      set mapreduce.reduce.memory.mb=4096;
      set mapreduce.map.java.opts=-Xmx3480m;
      set mapreduce.reduce.java.opts=-Xmx3480m;
      set mapred.max.split.size=300000000;
      set mapred.min.split.size.per.node=300000000;
      set mapred.min.split.size.per.rack=300000000;
      alter table user_action_context_d drop partition(pt_days=${v_date},model='ximalaya');
      insert overwrite table user_action_context_d partition (pt_days=${v_date},model='ximalaya',prov_id='051')
      select
            device_number,
            con1,
            null con2,
            null con3,
            null con4,
            con5
      from  tmp_user_con_out_t1 where pt_days=${v_date};"


v_date=`date -d "${v_date} +1 day" +%Y%m%d`
done
