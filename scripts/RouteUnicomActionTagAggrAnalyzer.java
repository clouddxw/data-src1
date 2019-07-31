package com.iresearch.route.dns.analyzer.unicom.actiontag;

import com.iresearch.route.dns.analyzer.SparkAnalyzer;
import com.iresearch.route.dns.analyzer.unicom.base.RouteUnicomBaseDefIf;
import com.iresearch.route.dns.analyzer.unicom.constants.UnicomTable;
import com.iresearch.route.dns.common.CommonSparkJob;
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
 * Date: 2019/4/11
 * Time: 15:47
 */
public class RouteUnicomActionTagAggrAnalyzer extends SparkAnalyzer implements Serializable,RouteUnicomBaseDefIf {

    private static final Logger logger = LoggerFactory.getLogger(RouteUnicomActionTagAggrAnalyzer.class);

    private static final String appName = "RouteUnicomActionTagAggrAnalyzer";

    public void runDayAnalyzer(String ptDays, boolean isLocal, String dataBase) {
        spark = RouteUnicomActionTagAggrAnalyzer.buildSparkSession(isLocal, appName);
        runAnalyd("20190301","20190319","20190331",dataBase);
        runAnalym("201903",dataBase);
        runAnalyprop("201903","201902",dataBase);


    }

    public void runAnalyd(String begindate, String middate, String enddate, String dataBase) {

        Dataset<Row> day01 = spark.sql("select '各app日活数据' model," +
                "   pt_days," +
                "   appname," +
                "   sum(pv) pv," +
                "   count(distinct device_number) uv " +
                "  from " + dataBase + ".user_action_app_d " +
                " where pt_days>='" + begindate + "' and pt_days<'" + enddate + "'" +
                " group by pt_days," +
                "          appname");


        Dataset<Row> day02 = spark.sql("select '新闻行业日活数据' model, " +
                "      pt_days, " +
                "      sum(pv) pv, " +
                "      count(distinct device_number) " +
                "      from " + dataBase + ".user_action_app_d " +
                "    where pt_days>='" + begindate + "' and pt_days<'" + enddate + "' " +
                "    and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng') " +
                "    group by pt_days ");

        Dataset<Row> day03 = spark.sql("select '音乐行业日活数据' model, " +
                "      pt_days, " +
                "      sum(pv) pv, " +
                "      count(distinct device_number) " +
                "      from " + dataBase + ".user_action_app_d " +
                "    where pt_days>='" + begindate + "' and pt_days<'" + enddate + "' " +
                "      and appname in ('qq_music','kuwo','kugou','wangyi_music') " +
                "      group by pt_days");

        Dataset<Row> day04 = spark.sql("select  '酷我' appname, " +
                "       '酷我电台-总' model, " +
                "        pt_days, " +
                "        count(*), " +
                "        count(distinct device_number) " +
                "    from " + dataBase + ".user_action_tag_d " +
                "   where pt_days>='" + middate + "' and pt_days<'" + enddate + "'   " +
                "   and (url_host like '%.kuwo.cn' " +
                "        and (instr(urltag,'fm/category')>0 or instr(urltag,'get_jm_info')>0 or instr(urltag,'fmradio')>0)) " +
                "     group by pt_days ");


        Dataset<Row> day05 = spark.sql("select  '酷我' appname, " +
                "       '酷我会员' model, " +
                "        pt_days, " +
                "        count(*), " +
                "        count(distinct device_number) " +
                "    from " + dataBase + ".user_action_tag_d " +
                "   where pt_days>='" + middate + "' and pt_days<'" + enddate + "'   " +
                "   and (instr(url_host,'.kuwo.cn')>0 " +
                "        and instr(urltag,'isVip=1')>0) " +
                "     group by pt_days");

        Dataset<Row> day06 = spark.sql("select  '酷我' appname, " +
                "       '酷我搜索' model, " +
                "        pt_days, " +
                "        count(*), " +
                "        count(distinct device_number) " +
                "    from " + dataBase + ".user_action_tag_d " +
                "   where pt_days>='" + middate + "' and pt_days<'" + enddate + "'   " +
                "   and url_host='search.kuwo.cn' " +
                "     group by pt_days");


        Dataset<Row> day07 = spark.sql("select '酷狗' app, " +
                "       pt_days, " +
                "       case when url_host='fm.service.kugou.com' then '电台' " +
                "           when url_host='kuhaoapi.kugou.com' then '酷狗号' " +
                "           when url_host='persnfm.service.kugou.com' and instr(urltag,'personal_recommend')>0 then '猜你喜欢' " +
                "           when url_host='acshow.kugou.com' and (instr(urltag,'roomId')>0 or instr(urltag,'getEnterRoomInfo')>0 ) then '视频直播' " +
                "           when url_host='acsing.kugou.com' and instr(urltag,'ktv_room')>0 then 'K歌-KTV' " +
                "           when url_host='acsing.kugou.com' and instr(urltag,'liveroom')>0 then 'K歌-直播' " +
                "           when url_host='acsing.kugou.com' and instr(urltag,'record.do')>0 then 'K歌-录唱' " +
                "           when url_host='acsing.kugou.com' and instr(urltag,'video/recordListen')>0  then '短视频' " +
                "           when url_host='everydayrec.service.kugou.com' then '酷狗每日推' " +
                "           when (url_host='ip2.kugou.com' and instr(urltag,'vip_type=6')>0 ) or (url_host='newsongretry.kugou.com' and instr(urltag,'vip_type=6')>0 ) " +
                "                or (url_host='ip.kugou.com' and instr(urltag,'vip_type=6')>0 ) or (url_host='ads.service.kugou.com' and instr(urltag,'isVip=1')>0 ) " +
                "               then '酷狗会员合计' " +
                "           else null " +
                "       end model, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "    from " + dataBase + ".user_action_tag_d " +
                "   where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "     and instr(url_host,'kugou.com')>0 " +
                "     group by  pt_days, " +
                "               case when url_host='fm.service.kugou.com' then '电台' " +
                "                     when url_host='kuhaoapi.kugou.com' then '酷狗号' " +
                "                     when url_host='persnfm.service.kugou.com' and instr(urltag,'personal_recommend')>0 then '猜你喜欢' " +
                "                     when url_host='acshow.kugou.com' and (instr(urltag,'roomId')>0 or instr(urltag,'getEnterRoomInfo')>0 ) then '视频直播' " +
                "                     when url_host='acsing.kugou.com' and instr(urltag,'ktv_room')>0 then 'K歌-KTV' " +
                "                     when url_host='acsing.kugou.com' and instr(urltag,'liveroom')>0 then 'K歌-直播' " +
                "                     when url_host='acsing.kugou.com' and instr(urltag,'record.do')>0 then 'K歌-录唱' " +
                "                     when url_host='acsing.kugou.com' and instr(urltag,'video/recordListen')>0  then '短视频' " +
                "                     when url_host='everydayrec.service.kugou.com' then '酷狗每日推' " +
                "                     when (url_host='ip2.kugou.com' and instr(urltag,'vip_type=6')>0 ) or (url_host='newsongretry.kugou.com' and instr(urltag,'vip_type=6')>0 ) " +
                "                         or (url_host='ip.kugou.com' and instr(urltag,'vip_type=6')>0 ) or (url_host='ads.service.kugou.com' and instr(urltag,'isVip=1')>0 ) " +
                "                         then '酷狗会员合计' " +
                "                     else null " +
                "                 end");

        Dataset<Row> day08 = spark.sql("select  '网易云' app, " +
                "       pt_days, " +
                "       case  when url_host='interface.music.163.com' and instr(urltag,'enhance/download')>0 then '下载会员' " +
                "             when url_host='interface.music.163.com' and instr(urltag,'store')>0 then '商城store' " +
                "             when url_host='interface.music.163.com' and instr(urltag,'recommend')>0 then '每日推荐' " +
                "             when url_host='interface.music.163.com' and instr(urltag,'comment')>0 then '查看评论comment' " +
                "             when url_host='interface.music.163.com' and instr(urltag,'videotimeline')>0 then '观看MV' " +
                "           else NULL " +
                "       end model, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "    from " + dataBase + ".user_action_tag_d " +
                "   where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "     and url_host='interface.music.163.com' " +
                "     and urltag in ('enhance/download','store','recommend','comment','videotimeline') " +
                "     group by  pt_days, " +
                "               case  when url_host='interface.music.163.com' and instr(urltag,'enhance/download')>0 then '下载会员' " +
                "                     when url_host='interface.music.163.com' and instr(urltag,'store')>0 then '商城store' " +
                "                     when url_host='interface.music.163.com' and instr(urltag,'recommend')>0 then '每日推荐' " +
                "                     when url_host='interface.music.163.com' and instr(urltag,'comment')>0 then '查看评论comment' " +
                "                     when url_host='interface.music.163.com' and instr(urltag,'videotimeline')>0 then '观看MV' " +
                "                   else NULL " +
                "               end");

        Dataset<Row> day09 = spark.sql("select  '喜马拉雅' app, " +
                "       case when instr(urltag,'vipCategory')>0 then '会员cate' " +
                "            when instr(urltag,'vipStatus=2')>0 then '会员status2' " +
                "            else '会员status13' " +
                "       end model, " +
                "       pt_days, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "        from " + dataBase + ".user_action_tag_d " +
                "     where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "        and  instr(url_host,'ximalaya.com')>0 " +
                "        and  (instr(urltag,'vipStatus=1')>0 or instr(urltag,'vipStatus=2')>0 or instr(urltag,'vipStatus=3')>0 or instr(urltag,'vipCategory')>0)  " +
                "    group by   pt_days, " +
                "               case when instr(urltag,'vipCategory')>0 then '会员cate' " +
                "                   when instr(urltag,'vipStatus=2')>0 then '会员status2' " +
                "                   else '会员status13' " +
                "               end");

        Dataset<Row> day10 = spark.sql("select '喜马拉雅' app, " +
                "       pt_days, " +
                "       case when url_host ='liveroom.ximalaya.com' then '直播' " +
                "            when url_host ='mp.ximalaya.com' and instr(urltag,'recharge')>0 then '充值' " +
                "            when url_host ='searchwsa.ximalaya.com' then '搜索' " +
                "            when url_host ='ad.ximalaya.com' and instr(urltag,'positionName=focus%categoryId=-2')>0 then '首页焦点图' " +
                "            when url_host='mwsa.ximalaya.com' and instr(urltag,'vip%channel%categoryId=-8')>0 then 'VIP频道' " +
                "            when url_host='ifm.ximalaya.com' and instr(urltag,'daily')>0 then '每日必听' " +
                "         else NULL " +
                "       end model, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "        from " + dataBase + ".user_action_tag_d " +
                "     where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "        and url_host in ('searchwsa.ximalaya.com','liveroom.ximalaya.com','ad.ximalaya.com','mwsa.ximalaya.com','ifm.ximalaya.com','mp.ximalaya.com')  " +
                "    group by    pt_days, " +
                "                case when url_host ='liveroom.ximalaya.com' then '直播' " +
                "                     when url_host ='mp.ximalaya.com' and instr(urltag,'recharge')>0 then '充值' " +
                "                     when url_host ='searchwsa.ximalaya.com' then '搜索' " +
                "                     when url_host ='ad.ximalaya.com' and instr(urltag,'positionName=focus%categoryId=-2')>0 then '首页焦点图' " +
                "                     when url_host='mwsa.ximalaya.com' and instr(urltag,'vip%channel%categoryId=-8')>0 then 'VIP频道' " +
                "                     when url_host='ifm.ximalaya.com' and instr(urltag,'daily')>0 then '每日必听' " +
                "                  else NULL " +
                "                end");

        Dataset<Row> day11 = spark.sql("select '喜马拉雅新增模块统计' model, " +
                "       pt_days, " +
                "       sum(case when jdbt_flg='1' then 1 else 0 end) jdbt_cnt, " +
                "       sum(case when jp_flg='1' then 1 else 0 end) jp_cnt, " +
                "       sum(case when ttt_flg='1' then 1 else 0 end) ttt_cnt " +
                "    from  " +
                "     (select pt_days, " +
                "             device_number, " +
                "              max(case when instr(urltag,'AggregateRankListTabs')>0 then 1 else 0 end) jdbt_flg, " +
                "              max(case when instr(urltag,'recommends%categoryId=33')>0 then 1 else 0 end) jp_flg, " +
                "              max(case when instr(urltag,'topBuzz')>0 then 1 else 0 end) ttt_flg  " +
                "         from " + dataBase + ".user_action_tag_d " +
                "     where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "       and instr(url_host,'ximalaya.com')>0 and (instr(urltag,'AggregateRankListTabs')>0 or instr(urltag,'recommends%categoryId=33')>0 or instr(urltag,'topBuzz')>0) " +
                "       group by pt_days, " +
                "                device_number) a " +
                "   group by pt_days");

        Dataset<Row> day12 = spark.sql("select        '喜马拉雅场景电台统计' model, " +
                "              pt_days, " +
                "              regexp_extract(urltag,'(sceneId=[0-9]+)',1) id, " +
                "              count(*) pv, " +
                "              count(distinct device_number) uv " +
                "         from " + dataBase + ".user_action_tag_d " +
                "     where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "       and instr(url_host,'ximalaya.com')>0 and instr(urltag,'sceneId=')>0 " +
                "       group by pt_days, " +
                "                regexp_extract(urltag,'(sceneId=[0-9]+)',1)");

        Dataset<Row> day13 = spark.sql("select        '喜马拉雅频道统计' model, " +
                "              pt_days, " +
                "              regexp_extract(urltag,'(categoryId=[0-9]+)',1) id, " +
                "              count(*) pv, " +
                "              count(distinct device_number) uv " +
                "         from " + dataBase + ".user_action_tag_d " +
                "     where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "       and instr(url_host,'ximalaya.com')>0 and instr(urltag,'categoryId=')>0 " +
                "       group by pt_days, " +
                "                regexp_extract(urltag,'(categoryId=[0-9]+)',1)");


        Dataset<Row> day14 = spark.sql("select '百度' app, " +
                "      '信息流-feed&f0' model, " +
                "       pt_days, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "       from " + dataBase + ".user_action_tag_d " +
                "    where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "      and (url_host = 'feed.baidu.com' " +
                "           or url_host like 'f%.baidu.com' " +
                "           or url_host rlike '^(vd)([0-9]*)(\\.bdstatic.com)$') " +
                "     group by pt_days ");

        Dataset<Row> day15 = spark.sql("select '百度' app, " +
                "      '信息流-f0' model, " +
                "       pt_days, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "       from " + dataBase + ".user_action_tag_d " +
                "    where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "      and url_host like 'f%.baidu.com' " +
                "     group by pt_days");

        Dataset<Row> day16 = spark.sql("select '百度' app, " +
                "      '信息流-ext' model, " +
                "       pt_days, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "       from " + dataBase + ".user_action_tag_d " +
                "    where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "      and (url_host in ('feed.baidu.com','ext.baidu.com') " +
                "           or url_host rlike '^(vd)([0-9]*)(\\.bdstatic.com)$') " +
                "     group by pt_days");

        Dataset<Row> day17 = spark.sql("select '百度' app, " +
                "       case when url_host rlike '^(sp)([0-9]*)(\\.baidu.com)$' then '搜索' " +
                "            when url_host rlike '^(vd)([0-9]*)(\\.bdstatic.com)$' then '视频' " +
                "            when url_host='boxnovel.baidu.com' then '百度小说' " +
                "            when url_host='voice.baidu.com' then '语音' " +
                "            when url_host='hmma.baidu.com' then '小程序' " +
                "            else NULL " +
                "       end model, " +
                "       pt_days, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "       from " + dataBase + ".user_action_tag_d " +
                "    where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "      and (url_host in ('boxnovel.baidu.com','voice.baidu.com','hmma.baidu.com') " +
                "           or url_host rlike '^(vd)([0-9]*)(\\.bdstatic.com)$' " +
                "           or url_host rlike '^(sp)([0-9]*)(\\.baidu.com)$') " +
                "     group by  pt_days, " +
                "               case when url_host rlike '^(sp)([0-9]*)(\\.baidu.com)$' then '搜索' " +
                "                   when url_host rlike '^(vd)([0-9]*)(\\.bdstatic.com)$' then '视频' " +
                "                   when url_host='boxnovel.baidu.com' then '百度小说' " +
                "                   when url_host='voice.baidu.com' then '语音' " +
                "                   when url_host='hmma.baidu.com' then '小程序' " +
                "                   else NULL " +
                "               end");

        Dataset<Row> day18 = spark.sql("select 'UC' app, " +
                "      'UC-feed流' model, " +
                "      pt_days, " +
                "      count(*), " +
                "      count(distinct device_number) " +
                "     from " + dataBase + ".user_action_tag_d " +
                "    where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "    and url_host ='iflow.uczzd.cn' " +
                "    group by pt_days");

        Dataset<Row> day19 = spark.sql("select 'UC' app, " +
                "       case when url_host='track.uc.cn' and instr(urltag,'book')>0 then '小说book' " +
                "            when instr(url_host,'shuqireader.com')>0 then '小说shuqi' " +
                "            when url_host='navi-user.uc.cn' and instr(urltag,'fiction')>0 then 'UC-小说fiction' " +
                "            when url_host='iflow.uczzd.cn' and instr(urltag,'video')>0 then '视频点击' " +
                "            when url_host='navi-user.uc.cn' and instr(urltag,'search')>0 then '搜索' " +
                "            when url_host='img.v.uc.cn'  then '小视频' " +
                "         else NULL " +
                "       end model, " +
                "       pt_days, " +
                "       count(*), " +
                "       count(distinct device_number) " +
                "     from " + dataBase + ".user_action_tag_d " +
                "    where pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "    and (url_host in ('track.uc.cn','iflow.uczzd.cn','navi-user.uc.cn','img.v.uc.cn') " +
                "         or url_host like '%.shuqireader.com') " +
                "    group by pt_days, " +
                "             case when url_host='track.uc.cn' and instr(urltag,'book')>0 then '小说book' " +
                "                 when instr(url_host,'shuqireader.com')>0 then '小说shuqi' " +
                "                 when url_host='navi-user.uc.cn' and instr(urltag,'fiction')>0 then 'UC-小说fiction' " +
                "                 when url_host='iflow.uczzd.cn' and instr(urltag,'video')>0 then '视频点击' " +
                "                 when url_host='navi-user.uc.cn' and instr(urltag,'search')>0 then '搜索' " +
                "                 when url_host='img.v.uc.cn'  then '小视频' " +
                "               else NULL " +
                "             end");

        Dataset<Row> day20 = spark.sql("select '头条未加密部分' app, " +
                "      '头条未加密日活' model, " +
                "      pt_days, " +
                "      count(*), " +
                "      count(distinct device_number) " +
                "     from " + dataBase + ".user_action_tag_d " +
                "    where  pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "       and instr(url_host,'snssdk.com')>0 " +
                "       and (instr(urltag,'video/play')>0  or instr(urltag,'action=GetPlayInfo')>0 or instr(urltag,'article/information')>0 or instr(urltag,'answer/detail')>0 ) " +
                "       group by pt_days");

        Dataset<Row> day21 = spark.sql("select '头条未加密部分' app, " +
                "       case when instr(urltag,'keyword')>0 then '头条-搜索' " +
                "            when instr(urltag,'category=video')>0 then '头条-短视频category' " +
                "            when instr(urltag,'action=GetPlayInfo')>0 then '头条-短视频action' " +
                "            when instr(urltag,'video/play')>0 then '头条-短视频/小视频play' " +
                "            when instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0   then '头条-小视频合并' " +
                "            when instr(urltag,'app_id=13')>0 then '小视频api' " +
                "            when url_host='developer.toutiao.com' then '小程序' " +
                "         else null " +
                "      end model, " +
                "      pt_days, " +
                "      count(*), " +
                "      count(distinct device_number) " +
                "     from " + dataBase + ".user_action_tag_d " +
                "     where  pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "       and ((url_host like '%.snssdk.com' " +
                "             and urltag in ('keyword','category=video','action=GetPlayInfo','video/play','category=hotsoon_video','category=live','videolive','get_ugc_video')) " +
                "             or (url_host='api.huoshan.com' and urltag='app_id=13') " +
                "             or url_host='developer.toutiao.com') " +
                "    group by pt_days, " +
                "             case when instr(urltag,'keyword')>0 then '头条-搜索' " +
                "                 when instr(urltag,'category=video')>0 then '头条-短视频category' " +
                "                 when instr(urltag,'action=GetPlayInfo')>0 then '头条-短视频action' " +
                "                 when instr(urltag,'video/play')>0 then '头条-短视频/小视频play' " +
                "                 when instr(urltag,'category=hotsoon_video')>0 or instr(urltag,'category=live')>0 or instr(urltag,'videolive')>0  or instr(urltag,'get_ugc_video')>0  then '头条-小视频合并' " +
                "                 when instr(urltag,'app_id=13')>0 then '小视频api' " +
                "                 when url_host='developer.toutiao.com' then '小程序' " +
                "               else null " +
                "           end");


        Dataset<Row> day22 = spark.sql("select '抖音' app, " +
                "      '上传effect' model, " +
                "      pt_days, " +
                "      sum(case when actiontag>5 then 1 else 0 end) f5, " +
                "      sum(case when actiontag>6 then 1 else 0 end) f6, " +
                "      sum(case when actiontag>7 then 1 else 0 end) f7, " +
                "      sum(case when actiontag>8 then 1 else 0 end) f8 " +
                "   from " +
                " (select pt_days, " +
                "       device_number, " +
                "       actiontag, " +
                "       row_number() over(partition by pt_days,device_number order by cast(actiontag as int) desc) rn " +
                "        from " + dataBase + ".user_action_tag_d " +
                "    where  pt_days>='" + middate + "' and pt_days<'" + enddate + "' " +
                "      and  url_host = 'effect.snssdk.com') a " +
                " where a.rn=1 " +
                " group by pt_days");

        Dataset<Row> day23 = spark.sql("select '腾讯新闻内容频道分布1' app, " +
                " con2, " +
                " count(*) pv, " +
                " count(distinct con1) id_cnt, " +
                " count(distinct device_number) uv " +
                "  from " + dataBase + ".user_action_context_d " +
                "where pt_days>='" + begindate + "' and pt_days<'" + enddate + "' " +
                " and model in ('tencent_news_article','tencent_news_video') " +
                " and con2<>'201' " +
                " group by con2");

        Dataset<Row> day24 = spark.sql("select " +
                "   '腾讯新闻内容频道分布2' app, " +
                "   con3, " +
                "   count(*) pv, " +
                "   count(distinct con1) id_cnt, " +
                "   count(distinct device_number) uv " +
                "    from " + dataBase + ".user_action_context_d " +
                " where  pt_days>='" + begindate + "' and pt_days<'" + enddate + "' " +
                "   and model in ('tencent_news_article','tencent_news_video') " +
                "   and con2='201' " +
                "   group by con3 ");

        Dataset<Row> day25 = spark.sql(" select " +
                "   '腾讯文章pagefrom统计' app, " +
                "   con4, " +
                "   count(*) pv, " +
                "   count(distinct con1) id_cnt, " +
                "   count(distinct device_number) uv " +
                "    from " + dataBase + ".user_action_context_d " +
                " where pt_days>='" + begindate + "' and pt_days<'" + enddate + "' " +
                "   and model='tencent_news_article' " +
                "   group by con4 ");

        Dataset<Row> day26 = spark.sql(" select " +
                "   '头条内容分布统计' app, " +
                "   con3, " +
                "   count(*) pv, " +
                "   count(distinct con1) id_cnt, " +
                "   count(distinct device_number) uv " +
                "    from " + dataBase + ".user_action_context_d " +
                " where pt_days>='" + begindate + "' and pt_days<'" + enddate + "' " +
                "   and model='toutiao_article' " +
                "   group by con3");

        Dataset<Row> day27 = spark.sql(" select " +
                "   '头条内容分布统计' app, " +
                "   con4, " +
                "   count(*) pv, " +
                "   count(distinct con1) id_cnt, " +
                "   count(distinct device_number) uv " +
                "    from " + dataBase + ".user_action_context_d " +
                " where pt_days>='" + begindate + "' and pt_days<'" + enddate + "' " +
                "   and model='toutiao_article' " +
                "   group by con4");

        Dataset<Row> day28 = spark.sql("select '内容表统计', " +
                "       model, " +
                "       count(distinct con1) con_cnt, " +
                "       count(*) pv, " +
                "       count(distinct device_number) uv " +
                "    from " + dataBase + ".user_action_context_d " +
                " where  pt_days>='" + begindate + "' and pt_days<'" + enddate + "' " +
                "   group by model");


        Dataset<Row> day29 = spark.sql("select url_host,count(*) cnt,count(distinct device_number) dau " +
                " from " + dataBase + ".user_action_tag_d where  pt_days='" + enddate + "'  group by url_host");

        Dataset<Row> day30 = spark.sql("select url_host,urltag,count(*) cnt,count(distinct device_number) dau " +
                "   from " + dataBase + ".user_action_tag_d  " +
                " where pt_days='" + enddate + "'  " +
                "   and urltag is not null " +
                "  group by url_host,urltag ");


        List<Row> day01List = day01.collectAsList();
        List<Row> day02List = day02.collectAsList();
        List<Row> day03List = day03.collectAsList();
        List<Row> day04List = day04.collectAsList();
        List<Row> day05List = day05.collectAsList();
        List<Row> day06List = day06.collectAsList();
        List<Row> day07List = day07.collectAsList();
        List<Row> day08List = day08.collectAsList();
        List<Row> day09List = day09.collectAsList();
        List<Row> day10List = day10.collectAsList();
        List<Row> day11List = day11.collectAsList();
        List<Row> day12List = day12.collectAsList();
        List<Row> day13List = day13.collectAsList();
        List<Row> day14List = day14.collectAsList();
        List<Row> day15List = day15.collectAsList();
        List<Row> day16List = day16.collectAsList();
        List<Row> day17List = day17.collectAsList();
        List<Row> day18List = day18.collectAsList();
        List<Row> day19List = day19.collectAsList();
        List<Row> day20List = day20.collectAsList();
        List<Row> day21List = day21.collectAsList();
        List<Row> day22List = day22.collectAsList();
        List<Row> day23List = day23.collectAsList();
        List<Row> day24List = day24.collectAsList();
        List<Row> day25List = day25.collectAsList();
        List<Row> day26List = day26.collectAsList();
        List<Row> day27List = day27.collectAsList();
        List<Row> day28List = day28.collectAsList();
        List<Row> day29List = day29.collectAsList();
        List<Row> day30List = day30.collectAsList();



        day01List.forEach(iter -> logger.info("result:"+iter.toString()));
        day02List.forEach(iter -> logger.info("result:"+iter.toString()));
        day03List.forEach(iter -> logger.info("result:"+iter.toString()));
        day04List.forEach(iter -> logger.info("result:"+iter.toString()));
        day05List.forEach(iter -> logger.info("result:"+iter.toString()));
        day06List.forEach(iter -> logger.info("result:"+iter.toString()));
        day07List.forEach(iter -> logger.info("result:"+iter.toString()));
        day08List.forEach(iter -> logger.info("result:"+iter.toString()));
        day09List.forEach(iter -> logger.info("result:"+iter.toString()));
        day10List.forEach(iter -> logger.info("result:"+iter.toString()));
        day11List.forEach(iter -> logger.info("result:"+iter.toString()));
        day12List.forEach(iter -> logger.info("result:"+iter.toString()));
        day13List.forEach(iter -> logger.info("result:"+iter.toString()));
        day14List.forEach(iter -> logger.info("result:"+iter.toString()));
        day15List.forEach(iter -> logger.info("result:"+iter.toString()));
        day16List.forEach(iter -> logger.info("result:"+iter.toString()));
        day17List.forEach(iter -> logger.info("result:"+iter.toString()));
        day18List.forEach(iter -> logger.info("result:"+iter.toString()));
        day19List.forEach(iter -> logger.info("result:"+iter.toString()));
        day20List.forEach(iter -> logger.info("result:"+iter.toString()));
        day21List.forEach(iter -> logger.info("result:"+iter.toString()));
        day22List.forEach(iter -> logger.info("result:"+iter.toString()));
        day23List.forEach(iter -> logger.info("result:"+iter.toString()));
        day24List.forEach(iter -> logger.info("result:"+iter.toString()));
        day25List.forEach(iter -> logger.info("result:"+iter.toString()));
        day26List.forEach(iter -> logger.info("result:"+iter.toString()));
        day27List.forEach(iter -> logger.info("result:"+iter.toString()));
        day28List.forEach(iter -> logger.info("result:"+iter.toString()));
        day29List.forEach(iter -> logger.info("result:"+iter.toString()));
        day30List.forEach(iter -> logger.info("result:"+iter.toString()));


    }

    public void runAnalym(String month_id, String dataBase) {

        Dataset<Row> m01 = spark.sql("select '各家APP月活' model, " +
                "       appname, " +
                "       count(distinct device_number) uv " +
                "     from " + dataBase + ".user_action_app_m " +
                "    where pt_days='" + month_id + "' " +
                "    group by appname");

        Dataset<Row> m02 = spark.sql("select '新闻行业月活' model, " +
                "       count(distinct device_number) " +
                "       from " + dataBase + ".user_action_app_m " +
                "     where pt_days='" + month_id + "' " +
                "     and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')");

        Dataset<Row> m03 = spark.sql("select '各家新闻独占' model, " +
                "       top1_app, " +
                "       count(*) " +
                "    from  " +
                "      (select device_number, " +
                "            count(distinct appname) cnt, " +
                "            max(case when rn=1 then appname else null end) top1_app " +
                "        from  " +
                "       (select  a.*, " +
                "                row_number() over(partition by device_number order by pv desc) rn  " +
                "          from " +
                "              (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')              " +
                "                  ) a) a " +
                "      group by device_number) b " +
                "  where cnt=1 " +
                "  group by top1_app ");

        Dataset<Row> m04 = spark.sql("select  " +
                "      '新闻两两重合' model, " +
                "      sum(case  when qq_flg='1' and tt_flg='1' then 1 else 0 end) qq_toutiao , " +
                "      sum(case  when qq_flg='1' and wy_flg='1' then 1 else 0 end) qq_wangyi , " +
                "      sum(case  when qq_flg='1' and kb_flg='1' then 1 else 0 end) qq_kuaibao , " +
                "      sum(case  when qq_flg='1' and sina_flg='1' then 1 else 0 end) qq_sina , " +
                "      sum(case  when qq_flg='1' and if_flg='1' then 1 else 0 end) qq_ifeng , " +
                "      sum(case  when qq_flg='1' and sh_flg='1' then 1 else 0 end) qq_sohu , " +
                "      sum(case  when tt_flg='1' and wy_flg='1' then 1 else 0 end) toutiao_wangyi , " +
                "      sum(case  when tt_flg='1' and kb_flg='1' then 1 else 0 end) toutiao_kuaibao , " +
                "      sum(case  when tt_flg='1' and sina_flg='1' then 1 else 0 end) toutiao_sina , " +
                "      sum(case  when tt_flg='1' and if_flg='1' then 1 else 0 end) toutiao_ifeng , " +
                "      sum(case  when tt_flg='1' and sh_flg='1' then 1 else 0 end) toutiao_sohu , " +
                "      sum(case  when wy_flg='1' and kb_flg='1' then 1 else 0 end) wangyi_kuaibao , " +
                "      sum(case  when wy_flg='1' and sina_flg='1' then 1 else 0 end) wangyi_sina , " +
                "      sum(case  when wy_flg='1' and if_flg='1' then 1 else 0 end) wangyi_ifeng , " +
                "      sum(case  when wy_flg='1' and sh_flg='1' then 1 else 0 end) wangyi_sohu , " +
                "      sum(case  when kb_flg='1' and sina_flg='1' then 1 else 0 end) kuaibao_sina , " +
                "      sum(case  when kb_flg='1' and if_flg='1' then 1 else 0 end) kuaibao_ifeng , " +
                "      sum(case  when kb_flg='1' and sh_flg='1' then 1 else 0 end) kuaibao_sohu , " +
                "      sum(case  when sina_flg='1' and if_flg='1' then 1 else 0 end) sina_ifeng , " +
                "      sum(case  when sina_flg='1' and sh_flg='1' then 1 else 0 end) sina_sohu , " +
                "      sum(case  when if_flg='1' and sh_flg='1' then 1 else 0 end) ifeng_sohu  " +
                "  from  " +
                "      (select device_number, " +
                "              count(distinct appname) cnt, " +
                "              max(case when appname='qqnews' then 1 else 0 end) qq_flg, " +
                "              max(case when appname='kuaibao' then 1 else 0 end) kb_flg, " +
                "              max(case when appname='wangyinews' then 1 else 0 end) wy_flg, " +
                "              max(case when appname='toutiao' then 1 else 0 end) tt_flg, " +
                "              max(case when appname='sinanews' then 1 else 0 end) sina_flg, " +
                "              max(case when appname='ifeng' then 1 else 0 end) if_flg, " +
                "              max(case when appname='sohunews' then 1 else 0 end) sh_flg " +
                "          from  " +
                "          (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')              " +
                "                  ) a " +
                "        group by device_number) b " +
                "where cnt>=2");


        Dataset<Row> m05 = spark.sql("select '新闻APP重合用户偏好' " +
                "       top1_app, " +
                "       count(*) uv " +
                "from  " +
                "    (select device_number, " +
                "          count(distinct appname) cnt, " +
                "          max(case when rn=1 then appname end) top1_app " +
                "        from  " +
                "        (select a.*, " +
                "                row_number() over(partition by device_number order by pv desc) rn  " +
                "          from " +
                "              (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')              " +
                "                  ) a) b " +
                "    group by  device_number) c " +
                "    where cnt>=2 " +
                "group by top1_app");


        Dataset<Row> m06 = spark.sql("select '音乐行业月活' model, " +
                "       count(distinct device_number) " +
                "       from " + dataBase + ".user_action_app_m " +
                "     where pt_days='" + month_id + "' " +
                "     and appname in ('qq_music','kuwo','kugou','wangyi_music')");

        Dataset<Row> m07 = spark.sql("select '各家音乐APP独占' model, " +
                "       top1_app, " +
                "       count(*) " +
                "    from  " +
                "      (select device_number, " +
                "            count(distinct appname) cnt, " +
                "            max(case when rn=1 then appname else null end) top1_app " +
                "        from  " +
                "        (select a.*, " +
                "                row_number() over(partition by device_number order by pv desc) rn  " +
                "            from  " +
                "            (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('qq_music','kuwo','kugou','wangyi_music')              " +
                "                  ) a ) a " +
                "      group by device_number) b " +
                "  where cnt=1 " +
                "  group by top1_app");

        Dataset<Row> m08 = spark.sql("select  " +
                "      '音乐APP两两重合' model, " +
                "      sum(case  when qq_flg='1' and kw_flg='1' then 1 else 0 end) qq_kuwo , " +
                "      sum(case  when qq_flg='1' and wy_flg='1' then 1 else 0 end) qq_wangyi , " +
                "      sum(case  when qq_flg='1' and kg_flg='1' then 1 else 0 end) qq_kugou ,  " +
                "      sum(case  when kw_flg='1' and wy_flg='1' then 1 else 0 end) kuwo_wangyi , " +
                "      sum(case  when kw_flg='1' and kg_flg='1' then 1 else 0 end) kuwo_kugou , " +
                "      sum(case  when wy_flg='1' and kg_flg='1' then 1 else 0 end) wangyi_kugou " +
                "  from  " +
                "      (select device_number, " +
                "              count(distinct appname) cnt, " +
                "              max(case when appname='qqmusic' then 1 else 0 end) qq_flg, " +
                "              max(case when appname='kuwo' then 1 else 0 end) kw_flg, " +
                "              max(case when appname='wangyi' then 1 else 0 end) wy_flg, " +
                "              max(case when appname='kugou' then 1 else 0 end) kg_flg " +
                "          from  " +
                "            (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('qq_music','kuwo','kugou','wangyi_music')              " +
                "                  ) a " +
                "        group by device_number) b " +
                "where cnt>=2");

        Dataset<Row> m09 = spark.sql("select " +
                "    a1.appname, " +
                "    count(distinct a.device_number) uv " +
                "  from  " +
                "    (select a.device_number " +
                "        from  " +
                "          (select device_number, " +
                "                count(distinct appname) cnt, " +
                "                concat_ws('&',collect_set(appname)) appname " +
                "           from " + dataBase + ".user_action_app_m  " +
                "          where  pt_days='" + month_id + "' " +
                "            and  appname in ('qq_music','kuwo','kugou','wangyi_music') " +
                "            group by device_number) a " +
                "        where instr(appname,'kuwo')>0 " +
                "          and cnt>=2 ) a " +
                "join  " +
                "    (select appname, " +
                "            device_number, " +
                "            row_number() over(partition by device_number order by pv desc) rn  " +
                "        from  " +
                "            (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('qq_music','kuwo','kugou','wangyi_music')              " +
                "                  ) a) a1 " +
                "on a.device_number = a1.device_number " +
                "where a1.rn=1 " +
                "group by a1.appname");

        Dataset<Row> m10 = spark.sql("select " +
                "    a1.appname, " +
                "    count(distinct a.device_number) uv " +
                "  from  " +
                "    (select a.device_number " +
                "        from  " +
                "          (select device_number, " +
                "                count(distinct appname) cnt, " +
                "                concat_ws('&',collect_set(appname)) appname " +
                "           from " + dataBase + ".user_action_app_m  " +
                "          where  pt_days='" + month_id + "' " +
                "            and  appname in ('qq_music','kuwo','kugou','wangyi_music') " +
                "            group by device_number) a " +
                "        where instr(appname,'kugou')>0 " +
                "          and cnt>=2 ) a " +
                "join  " +
                "    (select appname, " +
                "            device_number, " +
                "            row_number() over(partition by device_number order by pv desc) rn  " +
                "        from  " +
                "            (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('qq_music','kuwo','kugou','wangyi_music')              " +
                "                  ) a) a1 " +
                "on a.device_number = a1.device_number " +
                "where a1.rn=1 " +
                "group by a1.appname ");

        Dataset<Row> m11 = spark.sql("select " +
                "   '网易云重合偏好' model, " +
                "    a1.appname, " +
                "    count(distinct a.device_number) uv " +
                "  from  " +
                "    (select a.device_number " +
                "        from  " +
                "          (select device_number, " +
                "                count(distinct appname) cnt, " +
                "                concat_ws('&',collect_set(appname)) appname " +
                "           from " + dataBase + ".user_action_app_m  " +
                "          where  pt_days='" + month_id + "' " +
                "            and  appname in ('qq_music','kuwo','kugou','wangyi_music') " +
                "            group by device_number) a " +
                "        where instr(appname,'wangyi_music')>0 " +
                "          and cnt>=2 ) a " +
                "join  " +
                "    (select appname, " +
                "            device_number, " +
                "            row_number() over(partition by device_number order by pv desc) rn  " +
                "        from  " +
                "            (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('qq_music','kuwo','kugou','wangyi_music')              " +
                "                  ) a) a1 " +
                "on a.device_number = a1.device_number " +
                "where a1.rn=1 " +
                "group by a1.appname");


        Dataset<Row> m12 = spark.sql("select " +
                "   'qq音乐重合爱好' model, " +
                "    a1.appname, " +
                "    count(distinct a.device_number) uv " +
                "  from  " +
                "    (select a.device_number " +
                "        from  " +
                "          (select device_number, " +
                "                count(distinct appname) cnt, " +
                "                concat_ws('&',collect_set(appname)) appname " +
                "           from " + dataBase + ".user_action_app_m  " +
                "          where  pt_days='" + month_id + "' " +
                "            and  appname in ('qq_music','kuwo','kugou','wangyi_music') " +
                "            group by device_number) a " +
                "        where instr(appname,'qq_music')>0 " +
                "          and cnt>=2 ) a " +
                "join  " +
                "    (select appname, " +
                "            device_number, " +
                "            row_number() over(partition by device_number order by pv desc) rn  " +
                "        from  " +
                "            (select appname, " +
                "                      device_number, " +
                "                      pv " +
                "                   from " + dataBase + ".user_action_app_m  " +
                "                where  pt_days='" + month_id + "' " +
                "                 and appname in ('qq_music','kuwo','kugou','wangyi_music')              " +
                "                  ) a) a1 " +
                "on a.device_number = a1.device_number " +
                "where a1.rn=1 " +
                "group by a1.appname ");

        Dataset<Row> c01 = spark.sql("select  '头条点击频次分布' model," +
                "       pv_range, " +
                "       count(*) uv " +
                "from  " +
                "(select   device_number, " +
                "            case when count(*)=1 then '1' " +
                "                when count(*)>1 and count(*)<=5 then '(1-5]' " +
                "                when count(*)>5 and count(*)<=10 then '(5-10]' " +
                "                when count(*)>10 and count(*)<=20 then '(10-20]' " +
                "                when count(*)>20 then '(20,&]' " +
                "            end pv_range " +
                "         from " + dataBase + ".user_action_context_d " +
                "    where substr(pt_days,1,6)='" + month_id + "' " +
                "        and model='toutiao_article' " +
                "    group by device_number) t " +
                "group by pv_range");

        List<Row> m01List = m01.collectAsList();
        List<Row> m02List = m02.collectAsList();
        List<Row> m03List = m03.collectAsList();
        List<Row> m04List = m04.collectAsList();
        List<Row> m05List = m05.collectAsList();
        List<Row> m06List = m06.collectAsList();
        List<Row> m07List = m07.collectAsList();
        List<Row> m08List = m08.collectAsList();
        List<Row> m09List = m09.collectAsList();
        List<Row> m10List = m10.collectAsList();
        List<Row> m11List = m11.collectAsList();
        List<Row> m12List = m12.collectAsList();
        List<Row> c01List = c01.collectAsList();

        m01List.forEach(iter -> logger.info("result:"+iter.toString()));
        m02List.forEach(iter -> logger.info("result:"+iter.toString()));
        m03List.forEach(iter -> logger.info("result:"+iter.toString()));
        m04List.forEach(iter -> logger.info("result:"+iter.toString()));
        m05List.forEach(iter -> logger.info("result:"+iter.toString()));
        m06List.forEach(iter -> logger.info("result:"+iter.toString()));
        m07List.forEach(iter -> logger.info("result:"+iter.toString()));
        m08List.forEach(iter -> logger.info("result:"+iter.toString()));
        m09List.forEach(iter -> logger.info("result:"+iter.toString()));
        m10List.forEach(iter -> logger.info("result:"+iter.toString()));
        m11List.forEach(iter -> logger.info("result:"+iter.toString()));
        m12List.forEach(iter -> logger.info("result:"+iter.toString()));
        c01List.forEach(iter -> logger.info("result:"+iter.toString()));



    }


    public void runAnalyprop(String month_id, String m_1_id, String dataBase) {

        String[] apparray = {"'tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng'", "'tencent_news'", "'toutiao'", "'kuaibao'", "'wangyinews'", "'sina_news'", "'sohu_news'", "'ifeng'", "'qq_music','kuwo','kugou','wangyi_music'", "'qq_music'", "'kuwo'", "'kugou'", "'wangyi_music'"};
        String[] apparray1 = {"tencent_news,toutiao,kuaibao,wangyinews,sina_news,sohu_news,ifeng", "tencent_news", "toutiao", "kuaibao", "wangyinews", "sina_news", "sohu_news", "ifeng", "qq_music,kuwo,kugou,wangyi_music", "qq_music", "kuwo", "kugou", "wangyi_music"};

        for (int i = 0; i < apparray.length; i++) {
            String appname = apparray[i];
            String appname1 = apparray1[i];

            String[] array = {"top1_home_province", "top1_home_city", "factory_desc", "cert_prov", "cust_sex", "cost_level", "age_range"};
            for (int j = 0; j < array.length; j++) {
                String col = array[j];
                Dataset<Row> con = spark.sql("select  '" + appname1 + "' app, " +
                        "        '" + col + "' model, " +
                        "        t1." + col + ", " +
                        "        count(*) cnt " +
                        "    from  " +
                        "        (select device_number  from " + dataBase + ".user_action_app_m  where pt_days='" + month_id + "' and appname in (" + appname + ") group by device_number) t " +
                        "     left join  " +
                        "        (select *  from " + dataBase + ".user_info_sample where pt_days='" + m_1_id + "') t1 " +
                        "    on t.device_number=t1.device_number " +
                        "    group by t1." + col);

                List<Row> conList = con.collectAsList();
                conList.forEach(iter -> logger.info("result:"+iter.toString()));

            }
        }


        String[] array = {"top1_home_province", "top1_home_city", "factory_desc", "cert_prov", "cust_sex", "cost_level", "age_range"};
        for (int j = 0; j < array.length; j++) {
            String col = array[j];
            Dataset<Row> con = spark.sql("select  '头条内容点击' app," +
                    "        '" + col + "' model, " +
                    "        t1." + col + ", " +
                    "        count(*) cnt" +
                    "    from " +
                    "        (select   device_number " +
                    "                 from " + dataBase + ".user_action_context_d" +
                    "           where substr(pt_days,1,6)='" + month_id + "' " +
                    "                and model='toutiao_article'" +
                    "          group by device_number) t" +
                    "     left join " +
                    "        (select *  from " + dataBase + ".user_info_sample where pt_days='" + m_1_id + "') t1" +
                    "    on t.device_number=t1.device_number" +
                    "    group by t1." + col);
            List<Row> conList = con.collectAsList();
            conList.forEach(iter -> logger.info("result:"+iter.toString()));

        }


    }

}
