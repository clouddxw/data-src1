package com.iresearch.route.dns.analyzer.unicom.actiontag;

import com.iresearch.route.dns.analyzer.SparkAnalyzer;
import com.iresearch.route.dns.analyzer.unicom.base.RouteUnicomBaseDefIf;
import com.iresearch.route.dns.analyzer.unicom.constants.UnicomTable;
import com.iresearch.route.dns.common.CommonSparkJob;
import com.iresearch.route.dns.util.DataUtils;
import com.iresearch.route.dns.util.DateUtils;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Created with IntelliJ IDEA
 * Description:
 * User: lsr
 * Date: 2019/1/5
 * Time: 11:09
 */
public class RouteUnicomActionTagAnalyzer extends SparkAnalyzer implements Serializable,RouteUnicomBaseDefIf {

    private static final Logger logger = LoggerFactory.getLogger(RouteUnicomActionTagAnalyzer.class);

    private static final String appName = "RouteUnicomActionTagAnalyzer";
    /**
     * CREATE TABLE `bt_ext_action_tag_d`(
     *   `device_number` string,
     *   `imei` string,
     *   `url_host` string,
     *   `urltag` string,
     *   `actiontag` string)
     * PARTITIONED BY (
     *   `pt_days` string,
     *   `hh` string,
     *   `prov_id` string)
     * ROW FORMAT SERDE
     *   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     * WITH SERDEPROPERTIES (
     *   'field.delim'='\u0001',
     *   'serialization.format'='\u0001')
     * STORED AS INPUTFORMAT
     *   'org.apache.hadoop.mapred.TextInputFormat'
     * OUTPUTFORMAT
     *   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
     * LOCATION
     *   'hdfs://nameservice1/user/irssh1/research_dep01/bt_ext/bt_ext_action_tag_d';
     *
     *
     *   insert overwrite table bt_ext_action_tag_d
     * partition(pt_days,hh,prov_id)
     * select
     *      device_number,
     *      imei,
     *      url_host,
     *      case  when url_host='news.ssp.qq.com' and url like '%app%' then 'app'
     *            when url_host like '%.snssdk.com' and url like '%app_name=news_article%' then 'app_name=news_article'
     *            when url_host like '%p%.pstatp.com%' and url like '%/list/%' then '/list/'
     *            when url_host='s.inews.gtimg.com' and url like '%KuaiBao%' then 'KuaiBao'
     *            when url_host like '%.snssdk.com' and url like '%app_name=aweme%' then 'app_name=aweme'
     *            when url_host='aweme.snssdk.com' and url like '%create/video%' then 'create/video'
     *            when url_host like '%.snssdk.com' and url like '%upload%aweme%' then 'uploadaweme'
     *            when url_host='api.huoshan.com' and url like '%upload%aweme%' then 'uploadaweme'
     *            when url_host='p.l.qq.com' and url like '%Music%' then 'Music'
     *            when url_host='y.gtimg.cn' and url like '%music%' then 'music'
     *            when url_host='182.254.116.117' and url like '%qqmusic%' then 'qqmusic'
     *      end urltag,
     *      count(*)  actiontag,
     *       pt_days,
     *      hour(end_time) hh,
     *      prov_id
     * from foxdpi01.uc_origin_log_20181030
     * group by device_number,
     *       imei,
     *       url_host,
     *       case  when url_host='news.ssp.qq.com' and url like '%app%' then 'app'
     *             when url_host like '%.snssdk.com' and url like '%app_name=news_article%' then 'app_name=news_article'
     *             when url_host like '%p%.pstatp.com%' and url like '%/list/%' then '/list/'
     *             when url_host='s.inews.gtimg.com' and url like '%KuaiBao%' then 'KuaiBao'
     *             when url_host like '%.snssdk.com' and url like '%app_name=aweme%' then 'app_name=aweme'
     *             when url_host='aweme.snssdk.com' and url like '%create/video%' then 'create/video'
     *             when url_host like '%.snssdk.com' and url like '%upload%aweme%' then 'uploadaweme'
     *             when url_host='api.huoshan.com' and url like '%upload%aweme%' then 'uploadaweme'
     *             when url_host='p.l.qq.com' and url like '%Music%' then 'Music'
     *             when url_host='y.gtimg.cn' and url like '%music%' then 'music'
     *             when url_host='182.254.116.117' and url like '%qqmusic%' then 'qqmusic'
     *       end,
     *       pt_days,
     *       hour(end_time),
     *       prov_id;
     *
     */


    public void runDayAnalyzer(String ptDays, boolean isLocal,String dataBase,boolean produceTagAndContext,boolean produceApp,boolean produceClick){
        spark = RouteUnicomActionTagAnalyzer.buildSparkSession(isLocal, appName);
        if(produceTagAndContext) {
            String bmdhosts = spark.sql("select concat_ws(',',collect_set(concat('\\'',url_host,'\\''))) as bmdhosts from " + dataBase + ".dim_research_dep01_host a where instr(url_host,'%')= 0")
                    .collectAsList().get(0).getString(0);
            ClassTag<String> tag = (ClassTag) scala.reflect.ClassTag$.MODULE$.apply(String.class);
            final Broadcast<String> bmdhostsBroadcast = spark.sparkContext().broadcast(bmdhosts,tag);

            //初始化为1，第一次循环将当天的数据全部删除，后面的循环全部设置为0，不做任何删除分区的操作
            int dropPartitionType = 1;

            for (String provId : provIDList) {
                runActionTagAnalyzer(ptDays, provId, dataBase, bmdhostsBroadcast, dropPartitionType);
                runActionContextAnalyzer(ptDays, provId, dataBase, dropPartitionType);

                dropPartitionType = 0;
            }
        }
        if(produceApp) {
            runActionAppdAnalyzer(ptDays,dataBase);
        }
        if(produceClick){
            //获得murmurhash的udf
            buildMurMurHashUDF();
            //获得32进制转换的udf
            buildBinaryConversionUDF();
            runToutiaoClickAnalyzer(ptDays,dataBase);
        }


    }
    public void runMonthAnalyzer(String ptDays, boolean isLocal,String dataBase){
        spark = RouteUnicomActionTagAnalyzer.buildSparkSession(isLocal, appName);
        runActionAppmAnalyzer(ptDays,dataBase);
    }


    public void runToutiaoClickAnalyzer(String ptDays, String dataBase) {

        runSparkJob(new CommonSparkJob(spark, logger, 1, dataBase, UnicomTable.USER_ACTION_CONTEXT_OUT, null, ptDays, null, null){
            @Override
            public Dataset<Row> run() throws UnsupportedEncodingException {

                //获取前两个月的日期
                String ptMonth = DateUtils.getPervMonth(ptDays,"yyyyMMdd",-2).substring(0,6);

                //用户性别年龄等信息表area_id,cust_sex,cert_age,prov_id
                Dataset<Row> userInfo =spark.table(dataBase+"."+UnicomTable.USER_INFO_SAMPLE).where("pt_days = '"+ptMonth+"'");
                //头条内容聚合
                Dataset<Row> tt_con =  spark.sql( "  select pt_days,"+
                        "         device_number,"+
                        "         concat_ws('$',collect_set(concat_ws(':',con1,con3))) tt_con,"+
                        "         ''  tx_con "+
                        "   from   "+ dataBase + "."+UnicomTable.USER_ACTION_CONTEXT_D +" a"+
                        "    where pt_days = '" + ptDays + "'" +
                        "      and model='toutiao_article'"+
                        "      and length(trim(con1))>0"+
                        "    group by pt_days,device_number");

                //用户关系表，将device_number转换成user_id
                Dataset<Row> relationUserInfo = spark.table(UnicomTable.DWA_V_M_CUS_NM_USER_INFO).where("month_id = '"+ptMonth+"'");

                //因为个别电话号码可能会对应多个user_id,所以做一个窗口函数，只取第一个user_id
                WindowSpec windowSpec = Window.partitionBy("device_number").orderBy("user_id");
                List<String> joinList = new ArrayList<>();
                joinList.add("device_number");
                Seq<String> joinSeq = JavaConverters.asScalaIteratorConverter(joinList.iterator()).asScala().toSeq();
                //tt_con.join(useInfo,tt_con.col("device_number").equalTo(useInfo.col("device_number")))
                Dataset<Row> toutiaoCilck = userInfo.join(relationUserInfo,"device_number")
                        .withColumn("rank",row_number().over(windowSpec))
                        .select(relationUserInfo.col("user_id"),
                                col("device_number"),userInfo.col("cust_sex"),
                                userInfo.col("age_range"),
                                userInfo.col("top1_home_province"),userInfo.col("top1_home_city"),
                                userInfo.col("factory_desc"),userInfo.col("cost_level"))
                        .where("rank = 1")
                        .join(tt_con,joinSeq,"right")
                        .selectExpr( "binaryconversion(murmurhash(case when user_id is not null then user_id else '1000000000' end)) as irt",
                                "cust_sex","age_range",
                                "top1_home_province","top1_home_city",
                                "factory_desc","cost_level",
                                "tt_con", "tx_con",
                                "pt_days");
                saveData(toutiaoCilck, defaultFormat, true, "pt_days");

                return null;
            }
        });

    }



    //APP月表
    public void runActionAppmAnalyzer(String ptDays,String dataBase) {

        runSparkJob(new CommonSparkJob(spark, logger, 1, dataBase, UnicomTable.USER_ACTION_APP_M, null, ptDays, null, null) {
            @Override
            public Dataset<Row> run() throws UnsupportedEncodingException {

                Dataset<Row> appMonth = spark.sql("select   device_number, "+
                        "       appname, "+
                        "       count(distinct pt_days) active_days, "+
                        "       sum(pv) pv, "+
                        "       "+ptDays+" pt_days "+
                        "  from  " + dataBase + "."+UnicomTable.USER_ACTION_APP_D +
                        "    where pt_days>='"+ptDays+"01' and pt_days<='"+ptDays+"31'"+
                        "    group by  "+
                        "       device_number, "+
                        "       appname ");

                saveData(appMonth, defaultFormat, true, "pt_days");
                return null;
            }
        });
    }


    //20190128 新修改的方法
    private void runActionTagAnalyzer(String ptDays,String provId, String dataBase, Broadcast<String> bmdhostsBroadcast,int dropPartitionType) {

            runSparkJob(new CommonSparkJob(spark, logger, dropPartitionType, dataBase, UnicomTable.USER_ACTION_TAG_D, null, ptDays, null, provId) {
                @Override
                public Dataset<Row> run() throws UnsupportedEncodingException {
                    Dataset<Row> tmpSampleBase = spark.sql("  select device_number," +
                            "         url_host," +
                            "         url," +
                            "         hour(start_time) hh," +
                            "         case  when a.url_host='news.ssp.qq.com' and  instr(url,'app')>0 then 'app' "+
                            "                when a.url_host='p.l.qq.com' and instr(url,'Music')>0 then 'Music' "+
                            "                when a.url_host='y.gtimg.cn' and instr(url,'music')>0 then 'music'  "+
                            "                when url_host='track.uc.cn'  and instr(url,'book')>0  then 'book'  "+
                            "                when url_host='iflow.uczzd.cn' and instr(url,'video')>0 then 'video'  "+
                            "                when url_host in ('wmedia-track.uc.cn','m.uczzd.cn') and instr(url,'page=video')>0 then 'page=video' "+
                            "                when url_host ='api.huoshan.com'  and  instr(url,'app_id=13')>0 then 'app_id=13'  "+
                            "                when url_host ='sugs.m.sm.cn' and instr(url,'ucinput')>0 then 'ucinput' "+
                            "                when url_host='navi-user.uc.cn' then concat_ws('&',regexp_extract(url,'(search)',1),regexp_extract(url,'(fiction)',1)) "+
                            "                when instr(url_host,'.kugou.com')>0 then concat_ws('&',regexp_extract(url,'(vip_type=1)',1),regexp_extract(url,'(vip_type=6)',1),regexp_extract(url,'(isVip=1)',1),regexp_extract(url,'(personal_recommend)',1),regexp_extract(url,'(roomId)',1),regexp_extract(url,'(getEnterRoomInfo)',1),regexp_extract(url,'(ktv_room)',1),regexp_extract(url,'(liveroom)',1),regexp_extract(url,'(record.do)',1))  "+
                            "                when instr(url_host,'.ximalaya.com')>0 then concat_ws('&',regexp_extract(url,'(vipStatus=1)',1),regexp_extract(url,'(vipStatus=2)',1),regexp_extract(url,'(vipStatus=3)',1),regexp_extract(url,'(vipCategory)',1),regexp_extract(url,'(recharge)',1),concat_ws('%',regexp_extract(url,'(positionName=focus)',1),regexp_extract(url,'(categoryId=-2)',1)),regexp_extract(url,'(AggregateRankListTabs)',1),regexp_extract(url,'(daily)',1),regexp_extract(url,'(sceneId=-{0,1}[0-9]+)',1),concat_ws('%',regexp_extract(url,'(recommends)',1),regexp_extract(url,'(categoryId=33)',1)),regexp_extract(url,'(topBuzz)',1),concat_ws('%',regexp_extract(url,'(vip)',1),regexp_extract(url,'(channel)',1),regexp_extract(url,'(categoryId=-8)',1)),regexp_extract(url,'(categoryId=-{0,1}[0-9]+)',1),regexp_extract(url,'(rankingListId=-{0,1}[0-9]+)',1)) "+
                            "                when url_host='interface.music.163.com' then concat_ws('&',regexp_extract(url,'(enhance/download)',1),regexp_extract(url,'(comment)',1),regexp_extract(url,'(store)',1),regexp_extract(url,'(cloud/get)',1),regexp_extract(url,'(msg/private)',1),regexp_extract(url,'(user/comments)',1),regexp_extract(url,'(forwards)',1),regexp_extract(url,'(msg/notices)',1),regexp_extract(url,'(song/detail)',1),regexp_extract(url,'(comments/musiciansaid)',1),regexp_extract(url,'(getfollows)',1),regexp_extract(url,'(playlist/category)',1),regexp_extract(url,'(playlist/detail/dynamic)',1),regexp_extract(url,'(djradio)',1),regexp_extract(url,'(djradio/home/paygift)',1),regexp_extract(url,'(videotimeline)',1),regexp_extract(url,'(homepage)',1),regexp_extract(url,'(college/user/get)',1),regexp_extract(url,'(radio/get)',1),regexp_extract(url,'(sublist)',1),regexp_extract(url,'(my/radio)',1),regexp_extract(url,'(playlist/detail)',1),regexp_extract(url,'(mlivestream/entrance/playpage)',1),regexp_extract(url,'(songplay/entry)',1),regexp_extract(url,'(song/enhance/player)',1),regexp_extract(url,'(song/like)',1),regexp_extract(url,'(digitalAlbum/purchased)',1),regexp_extract(url,'(albumproduct/latest)',1),regexp_extract(url,'(new/albums)',1),regexp_extract(url,'(music/matcher)',1),regexp_extract(url,'(music/matcher/sing)',1),regexp_extract(url,'(play/mv)',1),regexp_extract(url,'(dailyTask)',1),regexp_extract(url,'(keyword)',1)) "+
                            "                when instr(url_host,'.snssdk.com')>0  and url like '%app_name=news_article%' then concat_ws('&',regexp_extract(url,'(app_name=news_article)',1),regexp_extract(url,'(news/feed)',1),regexp_extract(url,'(article/information)',1),regexp_extract(url,'(keyword)',1),regexp_extract(url,'(category=video)',1),regexp_extract(url,'(action=GetPlayInfo)',1),regexp_extract(url,'(category=hotsoon_video)',1),regexp_extract(url,'(category=live)',1),regexp_extract(url,'(video/play)',1),regexp_extract(url,'(videolive)',1),regexp_extract(url,'(get_ugc_video)',1),regexp_extract(url,'(answer/detail)',1),regexp_extract(url,'(video/openapi)',1),regexp_extract(url,'(user/profile)',1),regexp_extract(url,'(app_name=video_article)',1),regexp_extract(url,'(vapp/action)',1),regexp_extract(url,'(vapp/danmaku)',1),regexp_extract(url,'(video/app/stream)',1),regexp_extract(url,'(keyword)',1),regexp_extract(url,'(category=xg_hotsoon_video)',1),regexp_extract(url,'(tt_shortvideo)',1),regexp_extract(url,'(category=tt_subv_live_tab)',1),regexp_extract(url,'(category=video_new)',1),regexp_extract(url,'(category=subv_user_follow)',1),regexp_extract(url,'(category=subv_xg_movie)',1),regexp_extract(url,'(category=subv_xg_game)',1),regexp_extract(url,'(category=subv_xg_society)',1),regexp_extract(url,'(category=subv_xg_nba)',1),regexp_extract(url,'(category=subv_xg_music)',1),regexp_extract(url,'(category=subv_variety)',1),regexp_extract(url,'(category=subv_video_agriculture)',1),regexp_extract(url,'(category=subv_video_food)',1),regexp_extract(url,'(category=subv_video_pet)',1),regexp_extract(url,'(category=subv_sport)',1),regexp_extract(url,'(category=subv_car)',1),regexp_extract(url,'(ugc/repost/)',1)) "+
                            "                when instr(url_host,'.kuwo.cn')>0 then concat_ws('&',regexp_extract(url,'(fm/category)',1),regexp_extract(url,'(get_jm_info)',1),regexp_extract(url,'(fmradio)',1),regexp_extract(url,'(isVip=1)',1)) "+
                            "               else null " +
                            "          end urltag," +
                            "          pt_days,prov_id" +
                            "          from " + dataBase + ".origin_sample_base a" +
                            " where prov_id = '" + provId + "' " +
                            " and pt_days = '" + ptDays + "'" +
                            "            and (url_host in(" + bmdhostsBroadcast.getValue() + ")" +
                            "                  or instr(a.url_host,'snssdk.com')>0" +
                            "                  or instr(a.url_host,'shuqireader.com')>0" +
                            "                  or instr(a.url_host,'dingtalk.com')>0" +
                            "                  or instr(a.url_host,'antfortune.com')>0" +
                            "                  or instr(a.url_host,'kugou.com')>0" +
                            "                  or instr(a.url_host,'koowo.com')>0" +
                            "                  or instr(a.url_host,'kuwo.cn')>0" +
                            "                  or instr(a.url_host,'ximalaya.com')>0 )");

                    tmpSampleBase.cache().createOrReplaceTempView("tmpsamplebase");

                    Dataset<Row> actionTagDay = spark.sql("select device_number," +
                            "    urltag," +
                            "    count(*) actiontag," +
                            "    pt_days," +
                            "    hh," +
                            "    prov_id, " +
                            "    url_host " +
                            "from tmpsamplebase " +
                            "group by device_number," +
                            "        urltag," +
                            "        pt_days," +
                            "        hh," +
                            "        prov_id," +
                            "        url_host");
                    saveData(actionTagDay, defaultFormat, true, "pt_days", "hh", "prov_id");

                    return null;
                }
            });

    }

    private void runActionContextAnalyzer(String ptDays,String provId, String dataBase, int dropPartitionType) {
        runSparkJob(new CommonSparkJob(spark, logger, dropPartitionType, dataBase, UnicomTable.USER_ACTION_CONTEXT_D, null, ptDays, null, provId) {
            public Dataset<Row> run() throws UnsupportedEncodingException {

                Dataset<Row> contextDay = spark.sql("select  device_number, "+
                        "        case    when instr(url_host,'inews.qq.com')>0  then regexp_extract(url,'(&id=)(.*?)&',2) "+
                        "                when url_host='p.ssp.qq.com'   then regexp_extract(url,'(&article_id=)(.*?)&',2) "+
                        "                when url_host='lives.l.qq.com'   then regexp_extract(url,'(&articleId=)(.*?)&',2) "+
                        "                when (instr(url_host,'snssdk.com')>0 and  url like '%article/information%app_name=news_article%' ) then regexp_extract(url,'(&item_id=)(.*?)&',2) "+
                        "                when (url_host like 'a%.pstatp.com' and instr(url,'/article/content/')>0 ) then split(url,'/')[7] "+
                        "                when (url_host='krcs.kugou.com'  and instr(url,'keyword=')>0 )  then reflect('java.net.URLDecoder', 'decode', regexp_extract(url,'(&keyword=)(.*?)&',2), 'UTF-8') "+
                        "                when  (url_host='bjacshow.kugou.com' and instr(url,'singerAndSong=')>0 ) then reflect('java.net.URLDecoder', 'decode', regexp_extract(url,'(singerAndSong=)(.*?)&',2), 'UTF-8') "+
                        "                else null  "+
                        "        end con1, "+
                        "        case    when instr(url_host,'inews.qq.com')>0 then substr(regexp_extract(url,'(&id=)(.*?)&',2),0,3)  "+
                        "                when url_host='p.ssp.qq.com'  then substr(regexp_extract(url,'(&article_id=)(.*?)&',2),0,3) "+
                        "                when url_host='lives.l.qq.com'  then substr(regexp_extract(url,'(&articleId=)(.*?)&',2),0,3) "+
                        "                when (url_host like 'a%.pstatp.com' and instr(url,'/article/content/')>0 ) then split(url,'/')[8] "+
                        "                else null  "+
                        "        end con2, "+
                        "        case    when instr(url_host,'inews.qq.com')>0 then regexp_extract(url,'(&chlid=)(.*?)&',2) "+
                        "                when url_host='p.ssp.qq.com'  then regexp_extract(url,'(&channel=)(.*?)&',2) "+
                        "                when url_host='lives.l.qq.com'  then regexp_extract(url,'(&channelId=)(.*?)&',2)  "+
                        "                when (instr(url_host,'snssdk.com')>0 and  url like '%article/information%app_name=news_article%' )  then regexp_extract(url,'(&from_category=)(.*?)&',2) "+
                        "                when (instr(url_host,'snssdk.com')>0 and  instr(url,'news/feed')>0 )  then regexp_extract(url,'(&category=)(.*?)&',2) "+
                        "                else null  "+
                        "        end con3, "+
                        "        case when instr(url_host,'inews.qq.com')>0 then regexp_extract(url,'(&pagestartFrom=)(.*?)&',2)  "+
                        "             when ( instr(url_host,'snssdk.com')>0 and  url like '%article/information%app_name=news_article%' )  then regexp_extract(url,'(&from=)(.*?)&',2) "+
                        "                else null  "+
                        "        end con4, "+
                        "        '' con5,pt_days, "+
                        "        case when instr(url_host,'inews.qq.com')>0 or url_host ='p.ssp.qq.com' then 'tencent_news_article' "+
                        "                when url_host = 'lives.l.qq.com' then 'tencent_news_video' "+
                        "                when instr(url_host,'snssdk.com')>0  then 'toutiao_article' "+
                        "                when url_host like 'a%.pstatp.com' and url like '%/article/content/%' then  'toutiao_pull' "+
                        "                when (instr(url_host,'snssdk.com')>0 and  instr(url,'news/feed')>0 )  then 'toutiao_catgory' "+
                        "                when url_host in ('krcs.kugou.com','bjacshow.kugou.com') then 'kugou' "+
                        "        end model,prov_id  "+
                        "    from tmpsamplebase  "+
                        "where   ( instr(url_host,'inews.qq.com')>0  and (( instr(url,'getSimpleNews')>0 and (instr(url,'&id=')>0 or instr(url,'&child=')>0 or instr(url,'&pagestartFrom')>0) ) or  ( instr(url,'getNewsRelateModule')>0  and (instr(url,'&id=')>0 or instr(url,'&child=')>0 or instr(url,'&pagestartFrom')>0) ))) "+
                        "        or (url_host='p.ssp.qq.com' and  (instr(url,'&article_id=')>0 or instr(url,'&channel=')>0)) "+
                        "        or (url_host='lives.l.qq.com' and (instr(url,'&articleId=')>0 and instr(url,'&channelId=')>0)) "+
                        "        or (instr(url_host,'snssdk.com')>0 and (url like '%article/information%app_name=news_article%' or instr(url,'news/feed')>0) ) "+
                        "        or (url_host='krcs.kugou.com'  and instr(url,'keyword=')>0 ) "+
                        "        or (url_host like 'a%.pstatp.com' and instr(url,'/article/content/')>0 ) "+
                        "        or (url_host='bjacshow.kugou.com' and instr(url,'singerAndSong=')>0 ) ");

                saveData(contextDay, defaultFormat, true, "pt_days","model","prov_id");
                //清除所有cache
                spark.catalog().clearCache();
                return null;
            }
        });
    }


    //计算App的pv
    private void runActionAppdAnalyzer(String ptDays,String dataBase) {

        runSparkJob(new CommonSparkJob(spark, logger, 1, dataBase, UnicomTable.USER_ACTION_APP_D, null, ptDays, null, null) {
            @Override
            public Dataset<Row> run() throws UnsupportedEncodingException {

                Dataset<Row> originSampleBase = spark.sql("select  " +
                        "        device_number," +
                        "        pt_days," +
                        "          case when (url_host in ('r.inews.qq.com','w.inews.qq.com','up.inews.qq.com','rpt.gdt.qq.com') or (url_host='news.ssp.qq.com' and urltag='app')) then 'tencent_news' "+
                        "               when url_host in ('a0.pstatp.com','a1.pstatp.com','a3.pstatp.com','a6.pstatp.com') or (instr(url_host,'snssdk.com')>0 and instr(urltag,'app_name=news_article')>0 ) then 'toutiao' "+
                        "               when url_host in ('r.cnews.qq.com',  'w.cnews.qq.com', 'cnews.wap.mpush.qq.com', 'kuaibao.qq.com') then 'kuaibao' "+
                        "               when (url_host in ('m.analytics.126.net','comment.news.163.com','comment.api.163.com','v.monitor.ws.netease.com','nex.163.com','m.analytics.126.net') or url_host like 'img%.126.net') then 'wangyinews' "+
                        "               when url_host in ('newsapi.sina.cn','interface.sina.cn','beacon.sina.com.cn') then 'sina_news' "+
                        "               when url_host in ('stadig.ifeng.com','user.iclient.ifeng.com','api.iclient.ifeng.com','api.3g.ifeng.com','stadig0.ifeng.com','d.ifengimg.com') then 'ifeng' "+
                        "               when url_host in ('api.k.sohu.com','pic.k.sohu.com','zcache.k.sohu.com','t.ads.sohu.com') then 'sohu_news' "+
                        "               when url_host ='y.qq.com' then 'qq_music' "+
                        "               when (instr(url_host,'kuwo.cn')>0 or instr(url_host,'koowo.com')>0 ) then 'kuwo' "+
                        "               when url_host in ('serveraddr.service.kugou.com','online.kugou.com','imge.kugou.com','nbcollect.kugou.com') then 'kugou' "+
                        "               when url_host='interface.music.163.com' then 'wangyi_music' "+
                        "               when (instr(url_host,'ximalaya.com')>0 or instr(url_host,'xmcdn.com')>0 ) then 'ximalaya' "+
                        "               when url_host in ('cdn102.lizhi.fm','cdnimg102.lizhi.fm') then 'lizhi' "+
                        "               when url_host='api.changba.com' then 'changba' "+
                        "               when (url_host in ('feed.baidu.com','mbd.baidu.com','ext.baidu.com') or url_host rlike '^(vd)([0-9]*)(\\.bdstatic.com)$' or url_host rlike '^(sp)([0-9]*)(\\.baidu.com)$') then 'mobbaidu' "+
                        "               when url_host in ('image.uczzd.cn','iflow.uczzd.cn','track.uc.cn','img.v.uc.cn') then 'uc' "+
                        "               when url_host in ('aweme.snssdk.com','api.amemv.com') then 'douyin' "+
                        "               when url_host='yoo.qpic.cn' then 'yoovideo' "+
                        "               else 'others' "+
                        "          end appname "+
                        "  from  " + dataBase + "."+UnicomTable.USER_ACTION_TAG_D +
                        "  where pt_days = '" + ptDays + "'");
                Dataset<Row> appDay = originSampleBase.groupBy(col("device_number"), col("pt_days"),
                        col("appname"))
                        .agg(col("device_number"), count(lit(1)).as("pv"), col("pt_days"), col("appname"))
                        .select(col("device_number"), col("pv"), col("pt_days"), col("appname"));

                saveData(appDay, defaultFormat, true, "pt_days", "appname");
                return null;
            }
        });
    }



}

