#!/bin/bash

#喜马拉雅点击提取脚本

for ((day=20190723;day<=20190731;day++))
do
      if [ $day == 20190723 ]
            then
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
            drop table if exists tmp_user_con_out_t1;
            create table tmp_user_con_out_t1 as
            select      pt_days,
                        device_number,
                        case when url_host in ('mobwsa.ximalaya.com','mobile.ximalaya.com') then regexp_extract(url,'(albumId=)(.*?)&',2)
                              when url_host = 'adse.wsa.ximalaya.com' then regexp_extract(url,'(album=)(.*?)&',2)
                              else null
                        end as con1,
                        ts con5
                  from
                        bigflow_origin_sample_base a
                  where pt_days=${day}
                        and url_host in ('mobwsa.ximalaya.com','adse.wsa.ximalaya.com','mobile.ximalaya.com') and (instr(url,'albumId=')>0 or instr(url,'album=')>0);"
      else
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
            insert into tmp_user_con_out_t1
            select      pt_days,
                        device_number,
                        case when url_host in ('mobwsa.ximalaya.com','mobile.ximalaya.com') then regexp_extract(url,'(albumId=)(.*?)&',2)
                              when url_host = 'adse.wsa.ximalaya.com' then regexp_extract(url,'(album=)(.*?)&',2)
                              else null
                        end as con1,
                        ts con5
                  from
                        bigflow_origin_sample_base a
                  where pt_days=${day}
                        and url_host in ('mobwsa.ximalaya.com','adse.wsa.ximalaya.com','mobile.ximalaya.com') and (instr(url,'albumId=')>0 or instr(url,'album=')>0);"
      fi
done


for ((day=20190801;day<=20190813;day++))
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
        insert into tmp_user_con_out_t1
        select      pt_days,
                    device_number,
                    case when url_host in ('mobwsa.ximalaya.com','mobile.ximalaya.com') then regexp_extract(url,'(albumId=)(.*?)&',2)
                          when url_host = 'adse.wsa.ximalaya.com' then regexp_extract(url,'(album=)(.*?)&',2)
                          else null
                    end as con1,
                    ts con5
              from
                    bigflow_origin_sample_base a
              where pt_days=${day}
                    and url_host in ('mobwsa.ximalaya.com','adse.wsa.ximalaya.com','mobile.ximalaya.com') and (instr(url,'albumId=')>0 or instr(url,'album=')>0);"
done




for ((day=20190723;day<=20190731;day++))
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
      alter table user_action_context_d drop partition(pt_days=${day},model='ximalaya');
      insert overwrite table user_action_context_d partition (pt_days=${day},model='ximalaya',prov_id='051')
      select
            device_number,
            con1,
            null con2,
            null con3,
            null con4,
            con5
      from  tmp_user_con_out_t1 where pt_days=${day};"
done

for ((day=20190801;day<=20190813;day++))
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
      alter table user_action_context_d drop partition(pt_days=${day},model='ximalaya');
      insert overwrite table user_action_context_d partition (pt_days=${day},model='ximalaya',prov_id='051')
      select
            device_number,
            con1,
            null con2,
            null con3,
            null con4,
            con5
      from  tmp_user_con_out_t1 where pt_days=${day};"
done
