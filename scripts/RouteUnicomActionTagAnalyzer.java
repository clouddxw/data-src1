1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
54
55
56
57
58
59
60
61
62
63
64
65
66
67
68
69
70
71
72
73
74
75
76
77
78
79
80
81
82
83
84
85
86
87
88
89
90
91
92
93
94
95
96
97
98
99
100
101
102
103
104
105
106
107
108
109
110
111
112
113
114
115
116
117
118
119
120
121
122
123
124
125
126
127
128
129
130
131
132
133
134
135
136
137
138
139
140
141
142
143
144
145
146
147
148
149
150
151
152
153
154
155
156
157
158
159
160
161
162
163
164
165
166
167
168
169
170
171
172
173
174
175
176
177
178
179
180
181
182
183
184
185
186
187
188
189
190
191
192
193
194
195
196
197
198
199
200
201
202
203
204
205
206
207
208
209
210
211
212
213
214
215
216
217
218
219
220
221
222
223
224
225
226
227
228
229
230
231
232
233
234
235
236
237
238
239
240
241
242
243
244
245
246
247
248
249
250
251
252
253
254
255
256
257
258
259
260
261
262
263
264
265
266
267
268
269
270
271
272
273
274
275
276
277
278
279
280
281
282
283
284
285
286
287
288
289
290
291
292
293
294
295
296
297
298
299
300
301
302
303
304
305
306
307
308
309
310
311
312
313
314
315
316
317
318
319
320
321
322
323
324
325
326
327
328
329
330
331
332
333
334
335
336
337
338
339
340
341
342
343
344
345
346
347
348
349
350
351
352
353
354
355
356
357
358
359
360
361
362
363
364
365
366
367
368
369
370
371
372
373
374
375
376
377
378
379
380
381
382
383
384
385
386
387
388
389
390
391
392
393
394
395
396
397
398
399
400
401
402
403
404
405
406
407
408
409
410
411
412
413
414
415
416
417
418
419
420
421
422
423
424
425
426
427
428
429
430
431
432
433
434
435
436
437
438
439
440
441
442
443
444
445
446
447
448
449
450
451
452
453
454
455
456
457
458
459
460
461
462
463
464
465
466
467
468
469
470
471
472
473
474
475
476
477
478
479
480
481
482
483
484
485
486
487
488
489
490
491
492
493
494
495
496
497
498
499
500
501
502
503
504
505
506
507
508
509
510
511
512
513
514
515
516
517
518
519
520
521
522
523
524
525
526
527
528
529
530
531
532
533
534
535
536
537
538
539
540
541
542
543
544
545
546
547
548
549
550
551
552
553
554
555
556
557
558
559
560
561
562
563
564
565
566
567
568
569
570
571
572
573
574
575
576
577
578
579
580
581
582
583
584
585
586
587
588
589
590
591
592
593
594
595
596
597
598
599
600
601
602
603
604
605
606
607
608
609
610
611
612
613
614
615
616
617
618
619
620
621
622
623
624
625
626
627
628
629
630
631
632
633
634
635
636
637
638
639
640
641
642
643
644
645
646
647
648
649
650
651
652
653
654
655
656
657
658
659
660
661
662
663
664
665
666
667
668
669
670
671
672
673
674
675
676
677
678
679
680
681
682
683
684
685
686
687
688
689
690
691
692
693
694
695
696
697
698
699
700
701
702
703
704
705
706
707
708
709
710
711
712
713
714
715
716
717
718
719
720
721
722
723
724
725
726
727
728
729
730
731
732
733
734
735
736
737
738
739
740
741
742
743
744
745
746
747
748
749
750
751
752
753
754
755
756
757
758
759
760
761
762
763
764
765
766
767
768
769
770
771
772
773
774
775
776
777
778
779
780
781
782
783
784
785
786
787
788
789
790
791
792
793
794
795
796
797
798
799
800
801
802
803
804
805
806
807
808
809
810
811
812
813
814
815
816
817
818
819
820
821
822
823
824
825
826
827
828
829
830
831
832
833
834
835
836
837
838
839
840
841
842
843
844
845
846
847
848
849
850
851
852
853
854
855
856
857
858
859
860
861
862
863
864
865
866
867
868
869
870
871
872
873
874
875
876
877
878
879
880
881
882
883
884
885
886
887
888
889
890
891
892
893
894
895
896
897
898
899
900
901
902
903
904
905
906
907
908
909
910
911
912
913
914
915
916
917
918
919
920
921
922
923
924
925
926
927
928
929
930
931
932
933
934
package com.iresearch.route.dns.analyzer.unicom.actiontag;

import com.iresearch.route.dns.analyzer.SparkAnalyzer;
import com.iresearch.route.dns.analyzer.unicom.base.RouteUnicomBaseDefIf;
import com.iresearch.route.dns.analyzer.unicom.constants.UnicomTable;
import com.iresearch.route.dns.common.CommonSparkJob;
import com.iresearch.route.dns.util.DataUtils;
import com.iresearch.route.dns.util.DateUtils;
import com.sun.org.apache.bcel.internal.generic.DADD;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
            runActionAppnewAnalyzer(ptDays,dataBase);
            runActionApplossAnalyzer(ptDays,dataBase);
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
                String ptMonth = DateUtils.getMonth(ptDays,"yyyyMMdd",-2).substring(0,6);

                //用户性别年龄等信息表area_id,cust_sex,cert_age,prov_id
                Dataset<Row> userInfo =spark.table(dataBase+"."+UnicomTable.USER_INFO_SAMPLE).where("pt_days = '"+ptMonth+"'");

                //头条内容聚合
                Dataset<Row> tt_con =  spark.sql( "  select pt_days,"+
                        "         device_number,"+
                        "         case when model in ('vd_qq','vd_youku') then concat_ws('$',collect_set(concat_ws(':',con1,con2,con3))) else  concat_ws('$',collect_set(concat_ws(':',con1,con3))) tt_con,"+
                        "         model  tx_con "+
                        "   from   "+ dataBase + "."+UnicomTable.USER_ACTION_CONTEXT_D +" a"+
                        "    where pt_days = '" + ptDays + "'" +
                        "      and model in ('toutiao_article','tencent_news_article','tencent_news_video','toutiao_pull','vd_qq','vd_youku') "+
                        "      and length(trim(con1))>0"+
                        "    group by pt_days,device_number,model "+
                        "union all  "+
                        "select pt_days, "+
                        "       device_number, "+
                        "        concat_ws('$',collect_set(con1)) tt_con, "+
                        "        'ximalaya' tx_con "+
                        "     from  "+
                        "       (select pt_days, "+
                        "               device_number, "+
                        "               con1, "+
                        "               floor(cast(con5 as bigint)/180) ceil "+
                        "   from   "+ dataBase + "."+UnicomTable.USER_ACTION_CONTEXT_D +" a"+
                        "    where pt_days = '" + ptDays + "'" +
                        "      and model = 'ximalaya' "+
                        "      and length(trim(con1))>0"+
                        "    group by pt_days,device_number,con1,floor(cast(con5 as bigint)/180)) a "+
                        "        group by pt_days,device_number"
                );

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
                saveData(toutiaoCilck, DEFAULT_FORMAT, true, "pt_days");

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

                saveData(appMonth, DEFAULT_FORMAT, true, "pt_days");
                return null;
            }
        });
    }


    //20190128 新修改的方法
    private void runActionTagAnalyzer(String ptDays,String provId, String dataBase, Broadcast<String> bmdhostsBroadcast,int dropPartitionType) {

            runSparkJob(new CommonSparkJob(spark, logger, dropPartitionType, dataBase, UnicomTable.USER_ACTION_TAG_D, null, ptDays, null, provId) {
                @Override
                public Dataset<Row> run() throws UnsupportedEncodingException {
                    Dataset<Row> tmpSampleBase = spark.sql(" select device_number," +
                            "         url_host," +
                            "         case when instr(url,'http://')<=0 then concat('http://',url) else url end url," +
                            "         ts as start_time, "+
                            "         hour(start_time) hh," +
                            "         'model3' data_model," +
                            "         case when a.url_host='news.ssp.qq.com' and  instr(url,'app')>0 then 'app'      "+
                            "              when a.url_host='p.l.qq.com' and instr(url,'Music')>0 then 'Music'      "+
                            "              when a.url_host='y.gtimg.cn' and instr(url,'music')>0 then 'music'       "+
                            "              when url_host='track.uc.cn'  and instr(url,'book')>0  then 'book'       "+
                            "              when url_host='iflow.uczzd.cn' and instr(url,'video')>0 then 'video'       "+
                            "              when url_host in ('wmedia-track.uc.cn','m.uczzd.cn') and instr(url,'page=video')>0 then 'page=video'      "+
                            "              when url_host ='api.huoshan.com'  and  instr(url,'app_id=13')>0 then 'app_id=13'       "+
                            "              when url_host ='sugs.m.sm.cn' and instr(url,'ucinput')>0 then 'ucinput'      "+
                            "              when url_host='navi-user.uc.cn'  "+
                            "                  then concat_ws('&', "+
                            "                                  regexp_extract(url,'(search)',1), "+
                            "                                  regexp_extract(url,'(fiction)',1) "+
                            "                                )      "+
                            "              when url_host='api.iplay.163.com' then regexp_extract(url,'(livestream)',1)      "+
                            "              when instr(url_host,'.kugou.com')>0  "+
                            "                  then concat_ws('&', "+
                            "                                  regexp_extract(url,'(vip_type=1)',1), "+
                            "                                  regexp_extract(url,'(vip_type=6)',1), "+
                            "                                  regexp_extract(url,'(isVip=1)',1), "+
                            "                                  regexp_extract(url,'(personal_recommend)',1), "+
                            "                                  regexp_extract(url,'(roomId)',1), "+
                            "                                  regexp_extract(url,'(getEnterRoomInfo)',1), "+
                            "                                  regexp_extract(url,'(ktv_room)',1), "+
                            "                                  regexp_extract(url,'(liveroom)',1), "+
                            "                                  regexp_extract(url,'(record.do)',1), "+
                            "                                  regexp_extract(url,'(daily_focus)',1), "+
                            "                                  regexp_extract(url,'audiobook',0) "+
                            "                                )       "+
                            "              when instr(url_host,'.ximalaya.com')>0  "+
                            "                  then concat_ws('&', "+
                            "                                  regexp_extract(url,'(vipStatus=1)',1), "+
                            "                                  regexp_extract(url,'(vipStatus=2)',1), "+
                            "                                  regexp_extract(url,'(vipStatus=3)',1), "+
                            "                                  regexp_extract(url,'(vipCategory)',1), "+
                            "                                  regexp_extract(url,'(recharge)',1), "+
                            "                                  concat_ws('%',regexp_extract(url,'(positionName=focus)',1),regexp_extract(url,'(categoryId=-2)',1)), "+
                            "                                  regexp_extract(url,'(AggregateRankListTabs)',1), "+
                            "                                  regexp_extract(url,'(daily)',1), "+
                            "                                  regexp_extract(url,'(sceneId=-{0,1}[0-9]+)',1), "+
                            "                                  concat_ws('%',regexp_extract(url,'(recommends)',1),regexp_extract(url,'(categoryId=33)',1)), "+
                            "                                  regexp_extract(url,'(topBuzz)',1), "+
                            "                                  concat_ws('%',regexp_extract(url,'(vip)',1),regexp_extract(url,'(channel)',1),regexp_extract(url,'(categoryId=-8)',1)), "+
                            "                                  regexp_extract(url,'(categoryId=-{0,1}[0-9]+)',1), "+
                            "                                  regexp_extract(url,'(rankingListId=-{0,1}[0-9]+)',1), "+
                            "                                  regexp_extract(url,'(sleep/theme)',1), "+
                            "                                  regexp_extract(url,'(roomId=)',1), "+
                            "                                  regexp_extract(url,'(discovery-feed)',1) "+
                            "                                )      "+
                            "              when url_host like 'p%-xg.byteimg.com'       "+
                            "                  then concat_ws('&',      "+
                            "                                  regexp_extract(url,'(img/tos-cn)',1),      "+
                            "                                  regexp_extract(url,'(img/pgc-image)',1),      "+
                            "                                  regexp_extract(url,'(img/p1901)',1)      "+
                            "                                )      "+
                            "              when url_host like 'p%-xg.bytecdn.cn' then regexp_extract(url,'(large)',1)       "+
                            "              when url_host like 'sf%-xgcdn-tos.pstatp.com'       "+
                            "                  then concat_ws('&',      "+
                            "                                  regexp_extract(url,'(web.business.image)',1),      "+
                            "                                  regexp_extract(url,'(img/mosaic-legacy)',1),      "+
                            "                                  regexp_extract(url,'(960x0)',1)      "+
                            "                                )      "+
                            "              when url_host='upload.gifshow.com' then regexp_extract(url,'(uploadCover)',1)      "+
                            "              when url_host like 'p%-tt.byteimg.com' or url_host like 'sf%-ttcdn-tos.pstatp.com'      "+
                            "                  then concat_ws('&',      "+
                            "                                  regexp_extract(url,'img/pgc-image',0),      "+
                            "                                  regexp_extract(url,'img/tos-cn',0),      "+
                            "                                  regexp_extract(url,'web.business.image',0),      "+
                            "                                  regexp_extract(url,'img/mosaic-legacy',0),      "+
                            "                                  regexp_extract(url,'960x0',0),      "+
                            "                                  regexp_extract(url,'from=shortvideo',0)      "+
                            "                                )      "+
                            "              when instr(url_host,'.163.com')>0      "+
                            "                  then concat_ws('&',      "+
                            "                                  regexp_extract(url,'(enhance/download)',1),      "+
                            "                                  regexp_extract(url,'(comment)',1),      "+
                            "                                  regexp_extract(url,'(store)',1),      "+
                            "                                  regexp_extract(url,'(cloud/get)',1),      "+
                            "                                  regexp_extract(url,'(msg/private)',1),      "+
                            "                                  regexp_extract(url,'(user/comments)',1),      "+
                            "                                  regexp_extract(url,'(msg/notices)',1),      "+
                            "                                  regexp_extract(url,'(song/detail)',1),      "+
                            "                                  regexp_extract(url,'(comments/musiciansaid)',1),      "+
                            "                                  regexp_extract(url,'(getfollows)',1),      "+
                            "                                  regexp_extract(url,'(playlist/category)',1),      "+
                            "                                  regexp_extract(url,'(playlist/detail/dynamic)',1),      "+
                            "                                  regexp_extract(url,'(djradio)',1),      "+
                            "                                  regexp_extract(url,'(djradio/home/paygift)',1),      "+
                            "                                  regexp_extract(url,'(videotimeline)',1),      "+
                            "                                  regexp_extract(url,'(homepage)',1),      "+
                            "                                  regexp_extract(url,'(college/user/get)',1),      "+
                            "                                  regexp_extract(url,'(radio/get)',1),      "+
                            "                                  regexp_extract(url,'(sublist)',1),      "+
                            "                                  regexp_extract(url,'(my/radio)',1),      "+
                            "                                  regexp_extract(url,'(playlist/detail)',1),      "+
                            "                                  regexp_extract(url,'(mlivestream/entrance/playpage)',1),      "+
                            "                                  regexp_extract(url,'(songplay/entry)',1),      "+
                            "                                  regexp_extract(url,'(song/enhance/player)',1),      "+
                            "                                  regexp_extract(url,'(song/like)',1),      "+
                            "                                  regexp_extract(url,'(digitalAlbum/purchased)',1),      "+
                            "                                  regexp_extract(url,'(albumproduct/latest)',1),      "+
                            "                                  regexp_extract(url,'(new/albums)',1),      "+
                            "                                  regexp_extract(url,'(music/matcher)',1),      "+
                            "                                  regexp_extract(url,'(music/matcher/sing)',1),      "+
                            "                                  regexp_extract(url,'(play/mv)',1),      "+
                            "                                  regexp_extract(url,'(dailyTask)',1),      "+
                            "                                  regexp_extract(url,'(keyword)',1),      "+
                            "                                  regexp_extract(url,'(dailyrecommend)',1),      "+
                            "                                  regexp_extract(url,'(nearby)',1),      "+
                            "                                  regexp_extract(url,'(search/get)',1),      "+
                            "                                  regexp_extract(url,'(mlivestream)',1),      "+
                            "                                  regexp_extract(url,'eapi',0),      "+
                            "                                  regexp_extract(url,'monitor/impress',0),      "+
                            "                                  regexp_extract(url,'usersafe/pl/count',0),      "+
                            "                                  regexp_extract(url,'msg/private',0),      "+
                            "                                  regexp_extract(url,'college/user/get',0),      "+
                            "                                  regexp_extract(url,'happy/info',0),      "+
                            "                                  regexp_extract(url,'forwards',0),      "+
                            "                                  regexp_extract(url,'videotimeline',0),      "+
                            "                                  regexp_extract(url,'homepage',0),      "+
                            "                                  regexp_extract(url,'sublist',0),      "+
                            "                                  regexp_extract(url,'my/radio',0),      "+
                            "                                  regexp_extract(url,'song/enhance/privilege',0),      "+
                            "                                  regexp_extract(url,'college/user/get',0),      "+
                            "                                  regexp_extract(url,'nearby',0),      "+
                            "                                  regexp_extract(url,'socialsquare',0),      "+
                            "                                  regexp_extract(url,'forward',0)      "+
                            "                              )                                "+
                            "              when instr(url_host,'.kuwo.cn')>0  "+
                            "                  then concat_ws('&', "+
                            "                                  regexp_extract(url,'(fm/category)',1), "+
                            "                                  regexp_extract(url,'(get_jm_info)',1), "+
                            "                                  regexp_extract(url,'(fmradio)',1), "+
                            "                                  regexp_extract(url,'(isVip=1)',1), "+
                            "                                  regexp_extract(url,'(ptype=vip)',1) "+
                            "                                )      "+
                            "              when url_host='api.gifshow.com'  "+
                            "                  then concat_ws('&', "+
                            "                                  regexp_extract(url,'(feed/hot)',1), "+
                            "                                  regexp_extract(url,'(feed/nearby)',1), "+
                            "                                  regexp_extract(url,'(intown)',1), "+
                            "                                  regexp_extract(url,'(myfollow)',1), "+
                            "                                  regexp_extract(url,'(following)',1), "+
                            "                                  regexp_extract(url,'(search)',1), "+
                            "                                  regexp_extract(url,'(news/load)',1), "+
                            "                                  regexp_extract(url,'(notify/load)',1), "+
                            "                                  regexp_extract(url,'(message)',1), "+
                            "                                  regexp_extract(url,'(comment/add)',1), "+
                            "                                  regexp_extract(url,'(photo/like)',1), "+
                            "                                  regexp_extract(url,'(share/sharePhoto)',1), "+
                            "                                  regexp_extract(url,'(relation/follow)',1) "+
                            "                                )      "+
                            "              when instr(url_host,'.huoshan.com')>0  "+
                            "                  then concat_ws('&', "+
                            "                                  regexp_extract(url,'(type=live)',1), "+
                            "                                  regexp_extract(url,'(type=video)',1), "+
                            "                                  regexp_extract(url,'(type=follow)',1), "+
                            "                                  regexp_extract(url,'(room/enter)',1), "+
                            "                                  concat_ws('%',regexp_extract(url,'(get_profile)',1),regexp_extract(url,'(to_user_id)',1)), "+
                            "                                  regexp_extract(url,'(_follow)',1), "+
                            "                                  regexp_extract(url,'(/ichat)',1), "+
                            "                                  regexp_extract(url,'(_unfollow)',1), "+
                            "                                  regexp_extract(url,'(wallet)',1), "+
                            "                                  concat_ws('%',regexp_extract(url,'(_buy)',1),regexp_extract(url,'(way=[0-9]+)',1)),regexp_extract(url,'(type=city)',1), "+
                            "                                  regexp_extract(url,'(type=find)',1) "+
                            "                                )      "+
                            "              when url_host='live.gifshow.com' and instr(url,'live/startPlay')>0 then 'live/startPlay'      "+
                            "              when url_host='t13img.yangkeduo.com' and  instr(url,'/cart/')>0 then 'cart'      "+
                            "              when url_host='pinduoduoimg.yangkeduo.com' and instr(url,'/promotion/')>0 then 'promotion'      "+
                            "              when url_host='t16img.yangkeduo.com' and instr(url,'/pdd_ims/')>0 then 'pdd_ims'      "+
                            "              when instr(url_host,'.snssdk.com')>0   "+
                            "                  then concat_ws('&', "+
                            "                                  regexp_extract(url,'(app_name=.*?)&',1), "+
                            "                                  regexp_extract(url,'(news/feed)',1), "+
                            "                                  regexp_extract(url,'(article/information)',1), "+
                            "                                  regexp_extract(url,'(keyword)',1), "+
                            "                                  regexp_extract(url,'(category=.*?)&',1), "+
                            "                                  regexp_extract(url,'(video/play)',1), "+
                            "                                  regexp_extract(url,'(videolive)',1), "+
                            "                                  regexp_extract(url,'(get_ugc_video)',1), "+
                            "                                  regexp_extract(url,'(answer/detail)',1), "+
                            "                                  regexp_extract(url,'(video/openapi)',1), "+
                            "                                  regexp_extract(url,'(user/profile)',1), "+
                            "                                  regexp_extract(url,'(ugc/repost/)',1), "+
                            "                                  regexp_extract(url,'(video/app)',1), "+
                            "                                  regexp_extract(url,'(vapp/action)',1), "+
                            "                                  regexp_extract(url,'(vapp/danmaku)',1), "+
                            "                                  regexp_extract(url,'(video/app/stream)',1), "+
                            "                                  regexp_extract(url,'(tt_shortvideo)',1), "+
                            "                                  concat_ws('%',regexp_extract(url,'(hotsoon)',1),regexp_extract(url,'(query)',1)), "+
                            "                                  regexp_extract(url,'(type=.*?)&',1), "+
                            "                                  regexp_extract(url,'(room/enter)',1), "+
                            "                                  regexp_extract(url,'(share/link_command)',1), "+
                            "                                  regexp_extract(url,'(share_way=.*?)&',1), "+
                            "                                  regexp_extract(url,'(comments)',1), "+
                            "                                  concat_ws('%',regexp_extract(url,'(get_profile)',1),regexp_extract(url,'(to_user_id)',1)), "+
                            "                                  regexp_extract(url,'(_follow)',1), "+
                            "                                  regexp_extract(url,'(/ichat)',1), "+
                            "                                  regexp_extract(url,'(_unfollow)',1), "+
                            "                                  regexp_extract(url,'(get_notice)',1), "+
                            "                                  regexp_extract(url,'(wallet)',1), "+
                            "                                  regexp_extract(url,'(payment_channels)',1), "+
                            "                                  regexp_extract(url,'(hashtag)',1), "+
                            "                                  regexp_extract(url,'(karaoke_hot_videos)',1), "+
                            "                                  regexp_extract(url,'(action=.*?)&',1), "+
                            "                                  regexp_extract(url,'(from_category=.*?)&',1) "+
                            "                                )      "+
                            "             else null       "+
                            "          end urltag,         "+
                            "          pt_days,prov_id" +
                            "          from " + dataBase + "."+UnicomTable.BIGFLOW_ORIGIN_SAMPLE_BASE+" a" +
                            " where prov_id = '" + provId + "' " +
                            " and pt_days = '" + ptDays + "'" +
                            "            and (url_host in(" + bmdhostsBroadcast.getValue() + ")" +
                            "                  or instr(a.url_host,'shuqireader.com')>0" +
                            "                  or instr(a.url_host,'dingtalk.com')>0" +
                            "                  or instr(a.url_host,'antfortune.com')>0" +
                            "                  or instr(a.url_host,'kugou.com')>0" +
                            "                  or instr(a.url_host,'koowo.com')>0" +
                            "                  or instr(a.url_host,'kuwo.cn')>0" +
                            "                  or instr(a.url_host,'snssdk.com')>0" +
                            "                  or instr(a.url_host,'ximalaya.com')>0 "+
                            "                  or a.url_host like 'a%.bytecdn.cn') "+
                            "union all"+
                            "select   device_number," +
                            "         url_host," +
                            "         case when instr(url,'http://')<=0 then concat('http://',url) else url end url," +
                            "         ts as start_time, "+
                            "         hour(start_time) hh," +
                            "         'model2' data_model," +
                            "         null urltag," +
                            "         pt_days,prov_id" +
                            "         from " + dataBase + "."+UnicomTable.ORIGIN_SAMPLE_BASE+" a" +
                            " where prov_id = '" + provId + "' " +
                            " and pt_days = '" + ptDays + "'" +
                            " and ( (url like 'http://live%.l.qq.com/livemsg%' and lower(coalesce(parse_url(url,'QUERY','channelId'),'')) not like 'news%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%sport%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%news%') " +
                            "   or ( url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%sports%') " +
                            "   or ( url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%news%') " +
                            "   or ( url like 'http://live%.l.qq.com/livemsg%' and parse_url(url,'QUERY','channelId') like 'news%') " +
                            "   or ( (url like 'http://btrace.qq.com/kvcollect%' and parse_url(url,'QUERY','step') in ('3','6') and user_agent like '%MicroMessenger%')) " +
                            "   or ( url like 'http://vv.video.qq.com/getvbkey%' and user_agent like '%QQSports%') " +
                            "   or ( url like 'http://live%.l.aiseet.atianqi.com/livemsg%') " +
                            "   or ( url like 'http://live%.l.cp81.ott.cibntv.net/livemsg%') " +
                            "   or ( url like 'http://live%.l.ott.video.qq.com/livemsg%') " +
                            "   or ( url like 'http://live%.l.t002.ottcn.com/livemsg?%') " +
                            "   or ( url like 'http://tv.t002.ottcn.com/i-tvbin/qtv_video/video_recommend/exit_recommend?%') " +
                            "   or ( url_host='tv.cp81.ott.cibntv.net') " +
                            "   or ( url_host='btrace.play.cp81.ott.cibntv.net') " +
                            "   or ( url like 'http://vv.video.qq.com%' and user_agent not like '%QQSports%') " +
                            "   or ( url like 'http://info.zb.video.qq.com/?%') " +
                            "   or ( url like 'http://tv.aiseet.atianqi.com/i-tvbin/qtv_video/get_lookhim?%') " +
                            "   or ( url like 'http://vv.play.aiseet.atianqi.com/getvinfo%') " +
                            "   or ( url like 'http://pingfore.qq.com/pingd?dm=v.qq.com%') " +
                            "   or ( url like 'http://pingfore.qq.com/pingd?dm=pgv.live.qq.com&url=/txv/cover/%') " +
                            "   or ( url like 'http://pingfore.qq.com/pingd?dm=v.qq.com&url=/x/cover/%') " +
                            "   or ( url like 'http://bullet.video.qq.com/fcgi-bin/target/regist?%') " +
                            "   or ( url like 'http://btrace.qq.com/kvcollect%') " +
                            "   or ( url like 'http://btrace.video.qq.com%') " +
                            "   or ( url like 'http://pay.video.qq.com%') " +
                            "   or ( url like 'http://v.qq.com%') " +
                            "   or ( url_host='ups.youku.com' and url like 'http://ups.youku.com/ups/get.json%') " +
                            "   or ( url like 'http://api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://api.mobile.youku.com/layout/ipad%/play/detail%' or url like 'http://api.mobile.youku.com/layout/android%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/android%/play/detail%') " +
                            "   or ( url_host='api.mobile.youku.com' and url like 'http://api.mobile.youku.com/player/audio/switch%') " +
                            "   or ( url_host='das.api.mobile.youku.com' and url like 'http://das.api.mobile.youku.com/show/relation/%') " +
                            "   or ( url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','vid'))>2) " +
                            "   or ( url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','v'))>2) " +
                            "   or ( url_host in('val.atm.youku.com') and length(parse_url(url,'QUERY','v'))>2) " +
                            "   or ( url_host in('v.youku.com','val.atm.youku.com' ,'ykrec.youku.com','vip.youku.com') and length(parse_url(url,'QUERY','vid'))>2) " +
                            "   or ( url_host in ('cmstool.youku.com')) " +
                            "   or ( url_host='v.youku.com' and url like 'http://v.youku.com/v_show/id_%') " +
                            "   or ( url_host='playlog.youku.com') " +
                            "   or ( url_host='vali.cp31.ott.cibntv.net') " +
                            "   or ( url like 'http://cibn.api.3g.cp31.ott.cibntv.net/%/common/h265/play%') " +
                            "   or ( url like 'http://tv.api.3g.youku.com/%/common/h265/play%' or url like 'http://tv.api.3g.youku.com/adv%') " +
                            "   or ( url like 'http://cibn.api.3g.cp31.ott.cibntv.net/player/cpplayinfo?%') " +
                            "   or ( url like 'http://ups.cp31.ott.cibntv.net/ups/get.json?%') " +
                            "   or ( url like 'http://pl.cp12.wasu.tv/playlist/m3u8?%') " +
                            "   or ( url like 'http://val.atm.cp31.ott.cibntv.net/show?%') " +
                            "   or ( url like 'http://statis.api.3g.youku.com/openapi-wireless/statis/vv%') " +
                            "   or ( url_host='vali-dns.cp31.ott.cibntv.net') " +
                            "   or ( url like 'http://gm.mmstat.com/yt/preclk%' and parse_url(url,'QUERY','turl') like '%show%') " +
                            "   or ( url like 'http://m.atm.youku.com/dot/video?%' and parse_url(url,'QUERY','os')='ios') " +
                            "   or ( url like 'http://v2html.atm.youku.com/vhtml?%' ) " +
                            " ) "

                    );
                    tmpSampleBase.cache().createOrReplaceTempView("tmpsamplebase");

                    Dataset<Row> actionTagDay = spark.sql("select device_number," +
                            "    urltag," +
                            "    count(*) actiontag," +
                            "    pt_days," +
                            "    hh," +
                            "    prov_id, " +
                            "    url_host " +
                            "from tmpsamplebase " +
                            "where data_model='model3' " +
                            "group by device_number," +
                            "        urltag," +
                            "        pt_days," +
                            "        hh," +
                            "        prov_id," +
                            "        url_host");
                    saveData(actionTagDay, DEFAULT_FORMAT, true, "pt_days", "hh", "prov_id");

                    return null;
                }
            });

    }

    private void runActionContextAnalyzer(String ptDays,String provId, String dataBase, int dropPartitionType) {
        runSparkJob(new CommonSparkJob(spark, logger, dropPartitionType, dataBase, UnicomTable.USER_ACTION_CONTEXT_D, null, ptDays, null, provId) {
            public Dataset<Row> run() throws UnsupportedEncodingException {

                Dataset<Row> contextDay = spark.sql("select  device_number,  " +
                        "        case    when instr(url_host,'inews.qq.com')>0  then regexp_extract(url,'(&id=)(.*?)&',2)  " +
                        "                when url_host='p.ssp.qq.com'   then regexp_extract(url,'(&article_id=)(.*?)&',2)  " +
                        "                when url_host='lives.l.qq.com'   then regexp_extract(url,'(&articleId=)(.*?)&',2)  " +
                        "                when (instr(url_host,'snssdk.com')>0 and  url like '%article/information%app_name=news_article%' ) then regexp_extract(url,'(&item_id=)(.*?)&',2)  " +
                        "                when ((url_host like 'a%.pstatp.com' or url_host like 'a%.bytecdn.cn') and instr(url,'/article/content/')>0 ) then split(url,'/')[7]  " +
                        "                when (url_host='krcs.kugou.com'  and instr(url,'keyword=')>0 )  then reflect('java.net.URLDecoder', 'decode', regexp_extract(url,'(&keyword=)(.*?)&',2), 'UTF-8')  " +
                        "                when  (url_host='bjacshow.kugou.com' and instr(url,'singerAndSong=')>0 ) then reflect('java.net.URLDecoder', 'decode', regexp_extract(url,'(singerAndSong=)(.*?)&',2), 'UTF-8')  " +
                        "                when  url_host in ('t13img.yangkeduo.com','t00img.yangkeduo.com','pinduoduoimg.yangkeduo.com') then regexp_replace(regexp_extract(url,'/([goods/images|cart|promotion]+?/[0-9\\-]+?/.+?)\\\\.[jpg|png|jpeg]',1),'goods/images','goods%images')  " +
                        "                when url_host in ('mobwsa.ximalaya.com','mobile.ximalaya.com') then regexp_extract(url,'(albumId=)(.*?)&',2)   " +
                        "                when url_host = 'adse.wsa.ximalaya.com' then regexp_extract(url,'(album=)(.*?)&',2)  " +
                        "        when    (url like 'http://live%.l.qq.com/livemsg%' and lower(coalesce(parse_url(url,'QUERY','channelId'),'')) not like 'news%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%sport%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%news%')    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%sports%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%news%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and parse_url(url,'QUERY','channelId') like 'news%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    (url like 'http://btrace.qq.com/kvcollect%' and parse_url(url,'QUERY','step') in ('3','6') and user_agent like '%MicroMessenger%')    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://vv.video.qq.com/getvbkey%' and user_agent like '%QQSports%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://live%.l.aiseet.atianqi.com/livemsg%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://live%.l.cp81.ott.cibntv.net/livemsg%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://live%.l.ott.video.qq.com/livemsg%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://live%.l.t002.ottcn.com/livemsg?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://tv.t002.ottcn.com/i-tvbin/qtv_video/video_recommend/exit_recommend?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url_host='tv.cp81.ott.cibntv.net'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vids') , 'UTF-8') " +
                        "        when    url_host='btrace.play.cp81.ott.cibntv.net'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://vv.video.qq.com%' and user_agent not like '%QQSports%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://info.zb.video.qq.com/?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','livepid') , 'UTF-8')  " +
                        "        when    url like 'http://tv.aiseet.atianqi.com/i-tvbin/qtv_video/get_lookhim?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://vv.play.aiseet.atianqi.com/getvinfo%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=v.qq.com%'    then    concat('qq20_',reflect('java.net.URLDecoder', 'decode', regexp_extract(url,'cover/(.*?)/index\\.html',1) , 'UTF-8'))  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=pgv.live.qq.com&url=/txv/cover/%'    then    reflect('java.net.URLDecoder', 'decode', regexp_extract(url,'/([0-9a-zA-Z]+)\\.html&rand=',1) , 'UTF-8')  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=v.qq.com&url=/x/cover/%'    then    reflect('java.net.URLDecoder', 'decode', regexp_extract(url,'/([0-9a-zA-Z]+)\\.html&rdm=',1) , 'UTF-8')  " +
                        "        when    url like 'http://bullet.video.qq.com/fcgi-bin/target/regist?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://btrace.qq.com/kvcollect%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://btrace.video.qq.com%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','mvid') , 'UTF-8') " +
                        "        when    url_host='ups.youku.com' and url like 'http://ups.youku.com/ups/get.json%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://api.mobile.youku.com/layout/ipad%/play/detail%' or url like 'http://api.mobile.youku.com/layout/android%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/android%/play/detail%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','id') , 'UTF-8') " +
                        "        when    url_host='api.mobile.youku.com' and url like 'http://api.mobile.youku.com/player/audio/switch%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url_host='das.api.mobile.youku.com' and url like 'http://das.api.mobile.youku.com/show/relation/%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','vid'))>2    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','v'))>2    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','v') , 'UTF-8') " +
                        "        when    url_host in('val.atm.youku.com') and length(parse_url(url,'QUERY','v'))>2    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','v') , 'UTF-8') " +
                        "        when    url_host in('v.youku.com','val.atm.youku.com' ,'ykrec.youku.com','vip.youku.com') and length(parse_url(url,'QUERY','vid'))>2    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url_host in ('cmstool.youku.com')    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','videoid') , 'UTF-8') " +
                        "        when    url_host='playlog.youku.com'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','v') , 'UTF-8') " +
                        "        when    url_host='vali.cp31.ott.cibntv.net'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://cibn.api.3g.cp31.ott.cibntv.net/%/common/h265/play%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','id') , 'UTF-8') " +
                        "        when    url like 'http://tv.api.3g.youku.com/%/common/h265/play%' or url like 'http://tv.api.3g.youku.com/adv%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','id') , 'UTF-8') " +
                        "        when    url like 'http://cibn.api.3g.cp31.ott.cibntv.net/player/cpplayinfo?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://ups.cp31.ott.cibntv.net/ups/get.json?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://pl.cp12.wasu.tv/playlist/m3u8?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://val.atm.cp31.ott.cibntv.net/show?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','v') , 'UTF-8') " +
                        "        when    url like 'http://statis.api.3g.youku.com/openapi-wireless/statis/vv%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','id') , 'UTF-8') " +
                        "        when    url_host='vali-dns.cp31.ott.cibntv.net'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','vid') , 'UTF-8')  " +
                        "        when    url like 'http://gm.mmstat.com/yt/preclk%' and parse_url(url,'QUERY','turl') like '%show%'    then    reflect('java.net.URLDecoder', 'decode', split(split(parse_url(url,'QUERY','turl'),'id_')[1],'\\.')[0] , 'UTF-8') " +
                        "        when    url like 'http://m.atm.youku.com/dot/video?%' and parse_url(url,'QUERY','os')='ios'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','v') , 'UTF-8') " +
                        "        when    url like 'http://v2html.atm.youku.com/vhtml?%'    then    reflect('java.net.URLDecoder', 'decode', parse_url(url,'QUERY','v') , 'UTF-8') " +
                        "                else null   " +
                        "        end con1,  " +
                        "        case    when instr(url_host,'inews.qq.com')>0 then substr(regexp_extract(url,'(&id=)(.*?)&',2),0,3)   " +
                        "                when url_host='p.ssp.qq.com'  then substr(regexp_extract(url,'(&article_id=)(.*?)&',2),0,3)  " +
                        "                when url_host='lives.l.qq.com'  then substr(regexp_extract(url,'(&articleId=)(.*?)&',2),0,3)  " +
                        "                when ((url_host like 'a%.pstatp.com' or url_host like 'a%.bytecdn.cn') and instr(url,'/article/content/')>0 ) then split(url,'/')[8]  " +
                        "        when    (url like 'http://live%.l.qq.com/livemsg%' and lower(coalesce(parse_url(url,'QUERY','channelId'),'')) not like 'news%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%sport%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%news%')    then    'qq'  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%sports%'    then    'qq_sport'  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%news%'    then    'qq_news'  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and parse_url(url,'QUERY','channelId') like 'news%'    then    'qq_news'  " +
                        "        when    (url like 'http://btrace.qq.com/kvcollect%' and parse_url(url,'QUERY','step') in ('3','6') and user_agent like '%MicroMessenger%')    then    'qq_weixin'  " +
                        "        when    url like 'http://vv.video.qq.com/getvbkey%' and user_agent like '%QQSports%'    then    'qq_sport'  " +
                        "        when    url like 'http://live%.l.aiseet.atianqi.com/livemsg%'    then    'qq'  " +
                        "        when    url like 'http://live%.l.cp81.ott.cibntv.net/livemsg%'    then    'qq'  " +
                        "        when    url like 'http://live%.l.ott.video.qq.com/livemsg%'    then    'qq'  " +
                        "        when    url like 'http://live%.l.t002.ottcn.com/livemsg?%'    then    'qq'  " +
                        "        when    url like 'http://tv.t002.ottcn.com/i-tvbin/qtv_video/video_recommend/exit_recommend?%'    then    'qq'  " +
                        "        when    url_host='tv.cp81.ott.cibntv.net'    then    'qq'  " +
                        "        when    url_host='btrace.play.cp81.ott.cibntv.net'    then    'qq'  " +
                        "        when    url like 'http://vv.video.qq.com%' and user_agent not like '%QQSports%'    then    'qq'  " +
                        "        when    url like 'http://info.zb.video.qq.com/?%'    then    'qq'  " +
                        "        when    url like 'http://tv.aiseet.atianqi.com/i-tvbin/qtv_video/get_lookhim?%'    then    'qq'  " +
                        "        when    url like 'http://vv.play.aiseet.atianqi.com/getvinfo%'    then    'qq'  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=v.qq.com%'    then    'qq'  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=pgv.live.qq.com&url=/txv/cover/%'    then    'qq'  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=v.qq.com&url=/x/cover/%'    then    'qq'  " +
                        "        when    url like 'http://bullet.video.qq.com/fcgi-bin/target/regist?%'    then    'qq'  " +
                        "        when    url like 'http://btrace.qq.com/kvcollect%'    then    'qq'  " +
                        "        when    url like 'http://btrace.video.qq.com%'    then    'qq'  " +
                        "        when    url like 'http://pay.video.qq.com%'    then    'qq'  " +
                        "        when    url like 'http://v.qq.com%'    then    'qq'  " +
                        "        when    url_host='ups.youku.com' and url like 'http://ups.youku.com/ups/get.json%'    then    'youku'  " +
                        "        when    url like 'http://api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://api.mobile.youku.com/layout/ipad%/play/detail%' or url like 'http://api.mobile.youku.com/layout/android%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/android%/play/detail%'    then    'youku'  " +
                        "        when    url_host='api.mobile.youku.com' and url like 'http://api.mobile.youku.com/player/audio/switch%'    then    'youku'  " +
                        "        when    url_host='das.api.mobile.youku.com' and url like 'http://das.api.mobile.youku.com/show/relation/%'    then    'youku'  " +
                        "        when    url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','vid'))>2    then    'youku'  " +
                        "        when    url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','v'))>2    then    'youku'  " +
                        "        when    url_host in('val.atm.youku.com') and length(parse_url(url,'QUERY','v'))>2    then    'youku'  " +
                        "        when    url_host in('v.youku.com','val.atm.youku.com' ,'ykrec.youku.com','vip.youku.com') and length(parse_url(url,'QUERY','vid'))>2    then    'youku'  " +
                        "        when    url_host in ('cmstool.youku.com')    then    'youku'  " +
                        "        when    url_host='v.youku.com' and url like 'http://v.youku.com/v_show/id_%'    then    'youku'  " +
                        "        when    url_host='playlog.youku.com'    then    'youku'  " +
                        "        when    url_host='vali.cp31.ott.cibntv.net'    then    'youku'  " +
                        "        when    url like 'http://cibn.api.3g.cp31.ott.cibntv.net/%/common/h265/play%'    then    'youku'  " +
                        "        when    url like 'http://tv.api.3g.youku.com/%/common/h265/play%' or url like 'http://tv.api.3g.youku.com/adv%'    then    'youku'  " +
                        "        when    url like 'http://cibn.api.3g.cp31.ott.cibntv.net/player/cpplayinfo?%'    then    'youku'  " +
                        "        when    url like 'http://ups.cp31.ott.cibntv.net/ups/get.json?%'    then    'youku'  " +
                        "        when    url like 'http://pl.cp12.wasu.tv/playlist/m3u8?%'    then    'youku'  " +
                        "        when    url like 'http://val.atm.cp31.ott.cibntv.net/show?%'    then    'youku'  " +
                        "        when    url like 'http://statis.api.3g.youku.com/openapi-wireless/statis/vv%'    then    'youku'  " +
                        "        when    url_host='vali-dns.cp31.ott.cibntv.net'    then    'youku'  " +
                        "        when    url like 'http://gm.mmstat.com/yt/preclk%' and parse_url(url,'QUERY','turl') like '%show%'    then    'youku'  " +
                        "        when    url like 'http://m.atm.youku.com/dot/video?%' and parse_url(url,'QUERY','os')='ios'    then    'youku'  " +
                        "        when    url like 'http://v2html.atm.youku.com/vhtml?%'    then    'youku'  " +
                        "                else null   " +
                        "        end con2,  " +
                        "        case    when instr(url_host,'inews.qq.com')>0 then regexp_extract(url,'(&chlid=)(.*?)&',2)  " +
                        "                when url_host='p.ssp.qq.com'  then regexp_extract(url,'(&channel=)(.*?)&',2)  " +
                        "                when url_host='lives.l.qq.com'  then regexp_extract(url,'(&channelId=)(.*?)&',2)   " +
                        "                when (instr(url_host,'snssdk.com')>0 and  url like '%article/information%app_name=news_article%' )  then regexp_extract(url,'(&from_category=)(.*?)&',2)  " +
                        "                when (instr(url_host,'snssdk.com')>0 and  instr(url,'news/feed')>0 )  then regexp_extract(url,'(&category=)(.*?)&',2)  " +
                        "        when    (url like 'http://live%.l.qq.com/livemsg%' and lower(coalesce(parse_url(url,'QUERY','channelId'),'')) not like 'news%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%sport%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%news%')    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%sports%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%news%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and parse_url(url,'QUERY','channelId') like 'news%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    (url like 'http://btrace.qq.com/kvcollect%' and parse_url(url,'QUERY','step') in ('3','6') and user_agent like '%MicroMessenger%')    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://vv.video.qq.com/getvbkey%' and user_agent like '%QQSports%'    then    'App'  " +
                        "        when    url like 'http://live%.l.aiseet.atianqi.com/livemsg%'    then    'App'  " +
                        "        when    url like 'http://live%.l.cp81.ott.cibntv.net/livemsg%'    then    'App'  " +
                        "        when    url like 'http://live%.l.ott.video.qq.com/livemsg%'    then    'App'  " +
                        "        when    url like 'http://live%.l.t002.ottcn.com/livemsg?%'    then    'App'  " +
                        "        when    url like 'http://tv.t002.ottcn.com/i-tvbin/qtv_video/video_recommend/exit_recommend?%'    then    'App'  " +
                        "        when    url_host='tv.cp81.ott.cibntv.net'    then    'App'  " +
                        "        when    url_host='btrace.play.cp81.ott.cibntv.net'    then    'App'  " +
                        "        when    url like 'http://vv.video.qq.com%' and user_agent not like '%QQSports%'    then    'App'  " +
                        "        when    url like 'http://info.zb.video.qq.com/?%'    then    'App'  " +
                        "        when    url like 'http://tv.aiseet.atianqi.com/i-tvbin/qtv_video/get_lookhim?%'    then    'App'  " +
                        "        when    url like 'http://vv.play.aiseet.atianqi.com/getvinfo%'    then    'App'  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=v.qq.com%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=pgv.live.qq.com&url=/txv/cover/%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=v.qq.com&url=/x/cover/%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://bullet.video.qq.com/fcgi-bin/target/regist?%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://btrace.qq.com/kvcollect%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://btrace.video.qq.com%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://pay.video.qq.com%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://v.qq.com%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host='ups.youku.com' and url like 'http://ups.youku.com/ups/get.json%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://api.mobile.youku.com/layout/ipad%/play/detail%' or url like 'http://api.mobile.youku.com/layout/android%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/android%/play/detail%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host='api.mobile.youku.com' and url like 'http://api.mobile.youku.com/player/audio/switch%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host='das.api.mobile.youku.com' and url like 'http://das.api.mobile.youku.com/show/relation/%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','vid'))>2    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','v'))>2    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host in('val.atm.youku.com') and length(parse_url(url,'QUERY','v'))>2    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host in('v.youku.com','val.atm.youku.com' ,'ykrec.youku.com','vip.youku.com') and length(parse_url(url,'QUERY','vid'))>2    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host in ('cmstool.youku.com')    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host='v.youku.com' and url like 'http://v.youku.com/v_show/id_%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host='playlog.youku.com'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url_host='vali.cp31.ott.cibntv.net'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://cibn.api.3g.cp31.ott.cibntv.net/%/common/h265/play%'    then    'App'  " +
                        "        when    url like 'http://tv.api.3g.youku.com/%/common/h265/play%' or url like 'http://tv.api.3g.youku.com/adv%'    then    'App'  " +
                        "        when    url like 'http://cibn.api.3g.cp31.ott.cibntv.net/player/cpplayinfo?%'    then    'App'  " +
                        "        when    url like 'http://ups.cp31.ott.cibntv.net/ups/get.json?%'    then    'App'  " +
                        "        when    url like 'http://pl.cp12.wasu.tv/playlist/m3u8?%'    then    'App'  " +
                        "        when    url like 'http://val.atm.cp31.ott.cibntv.net/show?%'    then    'App'  " +
                        "        when    url like 'http://statis.api.3g.youku.com/openapi-wireless/statis/vv%'    then    'App'  " +
                        "        when    url_host='vali-dns.cp31.ott.cibntv.net'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://gm.mmstat.com/yt/preclk%' and parse_url(url,'QUERY','turl') like '%show%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://m.atm.youku.com/dot/video?%' and parse_url(url,'QUERY','os')='ios'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "        when    url like 'http://v2html.atm.youku.com/vhtml?%'    then    (case when user_agent like '%Mozilla%' then 'Web' else 'App' end)  " +
                        "                else null   " +
                        "        end con3,  " +
                        "        case when instr(url_host,'inews.qq.com')>0 then regexp_extract(url,'(&pagestartFrom=)(.*?)&',2)   " +
                        "             when ( instr(url_host,'snssdk.com')>0 and  url like '%article/information%app_name=news_article%' )  then regexp_extract(url,'(&from=)(.*?)&',2)  " +
                        "                else null   " +
                        "        end con4,  " +
                        "        case when url_host in ('mobwsa.ximalaya.com','adse.wsa.ximalaya.com','mobile.ximalaya.com') then start_time else '' end con5,pt_days,  " +
                        "        case when instr(url_host,'inews.qq.com')>0 or url_host ='p.ssp.qq.com' then 'tencent_news_article'  " +
                        "                when url_host = 'lives.l.qq.com' then 'tencent_news_video'  " +
                        "                when instr(url_host,'snssdk.com')>0  then 'toutiao_article'  " +
                        "                when (url_host like 'a%.pstatp.com' or url_host like 'a%.bytecdn.cn') and url like '%/article/content/%' then  'toutiao_pull'  " +
                        "                when (instr(url_host,'snssdk.com')>0 and  instr(url,'news/feed')>0 )  then 'toutiao_catgory'  " +
                        "                when url_host in ('t13img.yangkeduo.com','t00img.yangkeduo.com','pinduoduoimg.yangkeduo.com') then 'pdd'  " +
                        "                when url_host in ('krcs.kugou.com','bjacshow.kugou.com') then 'kugou'  " +
                        "        when    (url like 'http://live%.l.qq.com/livemsg%' and lower(coalesce(parse_url(url,'QUERY','channelId'),'')) not like 'news%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%sport%' and lower(coalesce(parse_url(url,'QUERY','v'),'')) not like '%news%')    then    'vd_qq'    " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%sports%'    then    'vd_qq'    " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and lower(parse_url(url,'QUERY','v')) like '%news%'    then    'vd_qq'    " +
                        "        when    url like 'http://live%.l.qq.com/livemsg%' and parse_url(url,'QUERY','channelId') like 'news%'    then    'vd_qq'    " +
                        "        when    (url like 'http://btrace.qq.com/kvcollect%' and parse_url(url,'QUERY','step') in ('3','6') and user_agent like '%MicroMessenger%')    then    'vd_qq'    " +
                        "        when    url like 'http://vv.video.qq.com/getvbkey%' and user_agent like '%QQSports%'    then    'vd_qq'    " +
                        "        when    url like 'http://live%.l.aiseet.atianqi.com/livemsg%'    then    'vd_qq'    " +
                        "        when    url like 'http://live%.l.cp81.ott.cibntv.net/livemsg%'    then    'vd_qq'    " +
                        "        when    url like 'http://live%.l.ott.video.qq.com/livemsg%'    then    'vd_qq'    " +
                        "        when    url like 'http://live%.l.t002.ottcn.com/livemsg?%'    then    'vd_qq'    " +
                        "        when    url like 'http://tv.t002.ottcn.com/i-tvbin/qtv_video/video_recommend/exit_recommend?%'    then    'vd_qq'    " +
                        "        when    url_host='tv.cp81.ott.cibntv.net'    then    'vd_qq'    " +
                        "        when    url_host='btrace.play.cp81.ott.cibntv.net'    then    'vd_qq'    " +
                        "        when    url like 'http://vv.video.qq.com%' and user_agent not like '%QQSports%'    then    'vd_qq'    " +
                        "        when    url like 'http://info.zb.video.qq.com/?%'    then    'vd_qq'    " +
                        "        when    url like 'http://tv.aiseet.atianqi.com/i-tvbin/qtv_video/get_lookhim?%'    then    'vd_qq'    " +
                        "        when    url like 'http://vv.play.aiseet.atianqi.com/getvinfo%'    then    'vd_qq'    " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=v.qq.com%'    then    'vd_qq'    " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=pgv.live.qq.com&url=/txv/cover/%'    then    'vd_qq'    " +
                        "        when    url like 'http://pingfore.qq.com/pingd?dm=v.qq.com&url=/x/cover/%'    then    'vd_qq'    " +
                        "        when    url like 'http://bullet.video.qq.com/fcgi-bin/target/regist?%'    then    'vd_qq'    " +
                        "        when    url like 'http://btrace.qq.com/kvcollect%'    then    'vd_qq'    " +
                        "        when    url like 'http://btrace.video.qq.com%'    then    'vd_qq'    " +
                        "        when    url like 'http://pay.video.qq.com%'    then    'vd_qq'    " +
                        "        when    url like 'http://v.qq.com%'    then    'vd_qq'    " +
                        "        when    url_host='ups.youku.com' and url like 'http://ups.youku.com/ups/get.json%'    then    'vd_youku'    " +
                        "        when    url like 'http://api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://api.mobile.youku.com/layout/ipad%/play/detail%' or url like 'http://api.mobile.youku.com/layout/android%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.mobile.youku.com/layout/ios%/play/detail%' or url like 'http://detail.api.mobile.youku.com/layout/android%/play/detail%'    then    'vd_youku'    " +
                        "        when    url_host='api.mobile.youku.com' and url like 'http://api.mobile.youku.com/player/audio/switch%'    then    'vd_youku'    " +
                        "        when    url_host='das.api.mobile.youku.com' and url like 'http://das.api.mobile.youku.com/show/relation/%'    then    'vd_youku'    " +
                        "        when    url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','vid'))>2    then    'vd_youku'    " +
                        "        when    url_host='iyes.youku.com' and url like 'http://iyes.youku.com/adv?%' and length(parse_url(url,'QUERY','v'))>2    then    'vd_youku'    " +
                        "        when    url_host in('val.atm.youku.com') and length(parse_url(url,'QUERY','v'))>2    then    'vd_youku'    " +
                        "        when    url_host in('v.youku.com','val.atm.youku.com' ,'ykrec.youku.com','vip.youku.com') and length(parse_url(url,'QUERY','vid'))>2    then    'vd_youku'    " +
                        "        when    url_host in ('cmstool.youku.com')    then    'vd_youku'    " +
                        "        when    url_host='v.youku.com' and url like 'http://v.youku.com/v_show/id_%'    then    'vd_youku'    " +
                        "        when    url_host='playlog.youku.com'    then    'vd_youku'    " +
                        "        when    url_host='vali.cp31.ott.cibntv.net'    then    'vd_youku'    " +
                        "        when    url like 'http://cibn.api.3g.cp31.ott.cibntv.net/%/common/h265/play%'    then    'vd_youku'    " +
                        "        when    url like 'http://tv.api.3g.youku.com/%/common/h265/play%' or url like 'http://tv.api.3g.youku.com/adv%'    then    'vd_youku'    " +
                        "        when    url like 'http://cibn.api.3g.cp31.ott.cibntv.net/player/cpplayinfo?%'    then    'vd_youku'    " +
                        "        when    url like 'http://ups.cp31.ott.cibntv.net/ups/get.json?%'    then    'vd_youku'    " +
                        "        when    url like 'http://pl.cp12.wasu.tv/playlist/m3u8?%'    then    'vd_youku'    " +
                        "        when    url like 'http://val.atm.cp31.ott.cibntv.net/show?%'    then    'vd_youku'    " +
                        "        when    url like 'http://statis.api.3g.youku.com/openapi-wireless/statis/vv%'    then    'vd_youku'    " +
                        "        when    url_host='vali-dns.cp31.ott.cibntv.net'    then    'vd_youku'    " +
                        "        when    url like 'http://gm.mmstat.com/yt/preclk%' and parse_url(url,'QUERY','turl') like '%show%'    then    'vd_youku'    " +
                        "        when    url like 'http://m.atm.youku.com/dot/video?%' and parse_url(url,'QUERY','os')='ios'    then    'vd_youku'    " +
                        "        when    url like 'http://v2html.atm.youku.com/vhtml?%'    then    'vd_youku'    " +
                        "        when url_host in ('mobwsa.ximalaya.com','adse.wsa.ximalaya.com','mobile.ximalaya.com') then 'ximalaya'  " +
                        "        end model,prov_id   " +
                        "    from user_action_tmpsamplebase   " +
                        "where   ( instr(url_host,'inews.qq.com')>0  and (( instr(url,'getSimpleNews')>0 and (instr(url,'&id=')>0 or instr(url,'&child=')>0 or instr(url,'&pagestartFrom')>0) ) or  ( instr(url,'getNewsRelateModule')>0  and (instr(url,'&id=')>0 or instr(url,'&child=')>0 or instr(url,'&pagestartFrom')>0) )))  " +
                        "        or (url_host='p.ssp.qq.com' and  (instr(url,'&article_id=')>0 or instr(url,'&channel=')>0))  " +
                        "        or (url_host='lives.l.qq.com' and (instr(url,'&articleId=')>0 and instr(url,'&channelId=')>0))  " +
                        "        or (instr(url_host,'snssdk.com')>0 and (url like '%article/information%app_name=news_article%' or instr(url,'news/feed')>0) )  " +
                        "        or (url_host='krcs.kugou.com'  and instr(url,'keyword=')>0 )  " +
                        "        or ((url_host like 'a%.pstatp.com' or url_host like 'a%.bytecdn.cn') and instr(url,'/article/content/')>0 )  " +
                        "        or (url_host in ('mobwsa.ximalaya.com','adse.wsa.ximalaya.com','mobile.ximalaya.com') and (instr(url,'albumId=')>0 or instr(url,'album=')>0) )  " +
                        "        or (url_host in ('t13img.yangkeduo.com','t00img.yangkeduo.com','pinduoduoimg.yangkeduo.com') and (instr(url,'goods/images')>0 or instr(url,'cart')>0 or instr(url,'promotion')>0))  " +
                        "        or (url_host='bjacshow.kugou.com' and instr(url,'singerAndSong=')>0 ) " +
                        "        or data_model='model2' ");

                saveData(contextDay, DEFAULT_FORMAT, true, "pt_days","model","prov_id");
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
                        "               when url_host in ('rl.inews.qq.com','wl.inews.qq.com') then 'tencent_news_lite' "+
                        "               when url_host in ('a0.pstatp.com','a1.pstatp.com','a3.pstatp.com','a6.pstatp.com','p1-tt.byteimg.com','p3-tt.byteimg.com') or (instr(url_host,'snssdk.com')>0 and instr(urltag,'app_name=news_article')>0 ) then 'toutiao' "+
                        "               when url_host in ('r.cnews.qq.com',  'w.cnews.qq.com', 'cnews.wap.mpush.qq.com', 'kuaibao.qq.com') then 'kuaibao' "+
                        "               when (url_host in ('m.analytics.126.net','comment.news.163.com','comment.api.163.com','v.monitor.ws.netease.com','nex.163.com','c.m.163.com','nimg.ws.126.net') or url_host like 'img%.126.net') then 'wangyinews' "+
                        "               when url_host in ('newsapi.sina.cn','interface.sina.cn','beacon.sina.com.cn') then 'sina_news' "+
                        "               when url_host in ('stadig.ifeng.com','user.iclient.ifeng.com','api.iclient.ifeng.com','api.3g.ifeng.com','stadig0.ifeng.com','d.ifengimg.com') then 'ifeng' "+
                        "               when url_host in ('api.k.sohu.com','pic.k.sohu.com','zcache.k.sohu.com','t.ads.sohu.com') then 'sohu_news' "+
                        "               when url_host  in ('y.qq.com','stat.3g.music.qq.com','monitor.music.qq.com') then 'qq_music' "+
                        "               when (instr(url_host,'kuwo.cn')>0 or instr(url_host,'koowo.com')>0 ) then 'kuwo' "+
                        "               when url_host in ('serveraddr.service.kugou.com','online.kugou.com','imge.kugou.com','nbcollect.kugou.com') then 'kugou' "+
                        "               when url_host in ('interface.music.163.com','clientlog.music.163.com','music.163.com','interface3.music.163.com','clientlog3.music.163.com','api.iplay.163.com','apm3.music.163.com') then 'wangyi_music' "+
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

                saveData(appDay, DEFAULT_FORMAT, true, "pt_days", "appname");
                return null;
            }
        });
    }

    private void runActionAppnewAnalyzer(String ptDays,String dataBase) {

        runSparkJob(new CommonSparkJob(spark, logger, 1, dataBase, UnicomTable.USER_ACTION_APP_NEW_D, null, ptDays, null, null) {
            @Override
            public Dataset<Row> run() throws UnsupportedEncodingException {

                Dataset<Row> userActionApp = spark.sql("select a.pt_days, "+
                        "       a.appname, "+
                        "       a.device_number "+
                        "   from    "+
                        " USER_ACTION_APP_D a  "+
                        "where pt_days = '" + ptDays + "'"+
                        "  and not exists "+
                        "   (select * from  user_action_app_new_d a1 where a.appname=a1.appname and a.device_number=a1.device_number and a1.pt_days<'" + ptDays + "' ) ");

                saveData(userActionApp, DEFAULT_FORMAT, true, "pt_days");
                return null;
            }
        });
    }


    private void runActionApplossAnalyzer(String ptDays,String dataBase) {

        runSparkJob(new CommonSparkJob(spark, logger, 1, dataBase, UnicomTable.USER_ACTION_APP_LOSS_D, null, ptDays, null, null) {
            @Override
            public Dataset<Row> run() throws UnsupportedEncodingException {

                SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
                Date dt= null;
                try {
                    dt = sdf.parse(ptDays);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                Calendar rightNow=Calendar.getInstance();
                rightNow.setTime(dt);
                rightNow.add(Calendar.DATE,-90);
                Date dt1=rightNow.getTime();
                String day_90=sdf.format(dt1);

                Dataset<Row> userActionAppLoss = spark.sql("select '" + ptDays + "' pt_days, "+
                        "       a.appname, "+
                        "       a.device_number "+
                        "   from    "+
                        " USER_ACTION_APP_D a  "+
                        "where pt_days='" + day_90 +"'"+
                        "  and not exists "+
                        "   (select * from   "+
                        "         (select appname,device_number  "+
                        "                from USER_ACTION_APP_D  "+
                        "            where pt_days>'" + day_90 +"' and pt_days<='" + ptDays +"'"+
                        "            group by appname,device_number) a1  "+
                        "        where a.device_number=a1.device_number and a.appname=a1.appname) ");

                saveData(userActionAppLoss, DEFAULT_FORMAT, true, "pt_days");
                return null;
            }
        });
    }


}

