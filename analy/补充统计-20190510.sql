--支付宝各模块统计
select    '支付宝各模块统计' model,
          '日期' pt_days,
          'uv_支付宝登陆' uv_1,
          'uv_支付宝财富模块' uv0,
          'uv_支付宝使用mdap' uv1,
          'uv_支付宝使用mcgw' uv2,
          'uv_支付宝使用mobilegw' uv3,
          'uv_支付宝-移动特权' uv4,
          'uv_支付宝-蘑菇租房' uv5,
          'uv_支付宝-怪兽充电' uv6,
          'uv_支付宝-哈喽单车' uv7,
          'uv_支付宝-饿了么入口' uv8,
          'uv_支付宝-天猫小程序入口' uv9,
          'uv_支付宝-优酷会员' uv10,
          'uv_支付宝-校园生活' uv11,
          'uv_支付宝-车主服务' uv12,
          'uv_支付宝-高德' uv13,
          'uv_支付宝-大麦网' uv14,
          'uv_支付宝-嘀嗒出行' uv15,
          'uv_支付宝-每日必抢/聚划算' uv16,
          'uv_支付宝-天猫商品点击' uv17,
          'uv_支付宝-淘宝商品点击' uv18,
          'uv_芝麻信用' uv19,
          'uv_支付宝-财富号' uv21,
          'uv_汽车报价' uv22,
          'uv_高德实时公交' uv23,
          'uv_寻证易' uv24,
          'uv_萌宠到家' uv25,
          'uv_移动花卡' uv26,
          'uv_社保查询' uv27,
          'uv_书旗小说' uv28,
          'uv_肯德基+' uv29,
          'uv_国家移民局' uv31,
          'uv_爱租机' uv32,
          'uv_哈罗出行' uv33,
          'uv_12306出行' uv34,
          'uv_麦当劳' uv35,
          'uv_交管12123' uv36,
          'uv_哇哈哈到家' uv37,
          'uv_ofo小黄车' uv38,
          'uv_股票财富号' uv40,
          'uv_adv财富号' uv41,
          'uv_私募财富号' uv42,
          'uv_天弘基金' uv43,
          'uv_render财富' uv44,
          'uv_支付宝小程序' uv45,
          'uv_最美证件照' uv46,
          'uv_全国社保计算器' uv47,
          'uv_工资个税计算器' uv48,
          'uv_浙江预约挂号' uv49,
          'uv_药急送' uv50,
          'uv_免费照片打印' uv51,
          'uv_人人租机' uv52,
          'uv_内啥' uv53,
          'uv_公立医院体检预约' uv54,
          'uv_齐天大圣' uv55,
          'uv_旧衣服回收' uv56,
          'uv_违章缴费' uv57,
          'uv_政府业务' uv58,
          'uv_魔盒citybox' uv59,
          'uv_衣二三' uv60,
          'uv_微车' uv61,
          'uv_优酷会员兑换' uv62,
          'uv_支付宝登陆' pv_1,
          'pv_支付宝财富模块' pv0,
          'pv_支付宝使用mdap' pv1,
          'pv_支付宝使用mcgw' pv2,
          'pv_支付宝使用mobilegw' pv3,
          'pv_支付宝-移动特权' pv4,
          'pv_支付宝-蘑菇租房' pv5,
          'pv_支付宝-怪兽充电' pv6,
          'pv_支付宝-哈喽单车' pv7,
          'pv_支付宝-饿了么入口' pv8,
          'pv_支付宝-天猫小程序入口' pv9,
          'pv_支付宝-优酷会员' pv10,
          'pv_支付宝-校园生活' pv11,
          'pv_支付宝-车主服务' pv12,
          'pv_支付宝-高德' pv13,
          'pv_支付宝-大麦网' pv14,
          'pv_支付宝-嘀嗒出行' pv15,
          'pv_支付宝-每日必抢/聚划算' pv16,
          'pv_支付宝-天猫商品点击' pv17,
          'pv_支付宝-淘宝商品点击' pv18,
          'pv_芝麻信用' pv19,
          'pv_支付宝-财富号' pv21,
          'pv_汽车报价' pv22,
          'pv_高德实时公交' pv23,
          'pv_寻证易' pv24,
          'pv_萌宠到家' pv25,
          'pv_移动花卡' pv26,
          'pv_社保查询' pv27,
          'pv_书旗小说' pv28,
          'pv_肯德基+' pv29,
          'pv_国家移民局' pv31,
          'pv_爱租机' pv32,
          'pv_哈罗出行' pv33,
          'pv_12306出行' pv34,
          'pv_麦当劳' pv35,
          'pv_交管12123' pv36,
          'pv_哇哈哈到家' pv37,
          'pv_ofo小黄车' pv38,
          'pv_股票财富号' pv40,
          'pv_adv财富号' pv41,
          'pv_私募财富号' pv42,
          'pv_天弘基金' pv43,
          'pv_render财富' pv44,
          'pv_支付宝小程序' pv45,
          'pv_最美证件照' pv46,
          'pv_全国社保计算器' pv47,
          'pv_工资个税计算器' pv48,
          'pv_浙江预约挂号' pv49,
          'pv_药急送' pv50,
          'pv_免费照片打印' pv51,
          'pv_人人租机' pv52,
          'pv_内啥' pv53,
          'pv_公立医院体检预约' pv54,
          'pv_齐天大圣' pv55,
          'pv_旧衣服回收' pv56,
          'pv_违章缴费' pv57,
          'pv_政府业务' pv58,
          'pv_魔盒citybox' pv59,
          'pv_衣二三' pv60,
          'pv_微车' pv61,
          'pv_优酷会员兑换' pv62
union all
select  '支付宝各模块统计' model,
        pt_days,
        sum(case when tagg_login>0 then 1 else 0 end) uv_1,
        sum(case when tagg0>0 then 1 else 0 end)  uv0,
        sum(case when tagg1>0 then 1 else 0 end)  uv1,
        sum(case when tagg2>0 then 1 else 0 end)  uv2,
        sum(case when tagg3>0 then 1 else 0 end)  uv3,
        sum(case when tagg4>0 then 1 else 0 end)  uv4,
        sum(case when tagg5>0 then 1 else 0 end)  uv5,
        sum(case when tagg6>0 then 1 else 0 end)  uv6,
        sum(case when tagg7>0 then 1 else 0 end)  uv7,
        sum(case when tagg8>0 then 1 else 0 end)  uv8,
        sum(case when tagg9>0 then 1 else 0 end)  uv9,
        sum(case when tagg10>0 then 1 else 0 end)  uv10,
        sum(case when tagg11>0 then 1 else 0 end)  uv11,
        sum(case when tagg12>0 then 1 else 0 end)  uv12,
        sum(case when tagg13>0 then 1 else 0 end)  uv13,
        sum(case when tagg14>0 then 1 else 0 end)  uv14,
        sum(case when tagg15>0 then 1 else 0 end)  uv15,
        sum(case when tagg16>0 then 1 else 0 end)  uv16,
        sum(case when tagg17>0 then 1 else 0 end)  uv17,
        sum(case when tagg18>0 then 1 else 0 end)  uv18,
        sum(case when tagg19>0 then 1 else 0 end)  uv19,
        sum(case when tagg21>0 then 1 else 0 end)  uv21,
        sum(case when tagg22>0 then 1 else 0 end)  uv22,
        sum(case when tagg23>0 then 1 else 0 end)  uv23,
        sum(case when tagg24>0 then 1 else 0 end)  uv24,
        sum(case when tagg25>0 then 1 else 0 end)  uv25,
        sum(case when tagg26>0 then 1 else 0 end)  uv26,
        sum(case when tagg27>0 then 1 else 0 end)  uv27,
        sum(case when tagg28>0 then 1 else 0 end)  uv28,
        sum(case when tagg29>0 then 1 else 0 end)  uv29,
        sum(case when tagg31>0 then 1 else 0 end)  uv31,
        sum(case when tagg32>0 then 1 else 0 end)  uv32,
        sum(case when tagg33>0 then 1 else 0 end)  uv33,
        sum(case when tagg34>0 then 1 else 0 end)  uv34,
        sum(case when tagg35>0 then 1 else 0 end)  uv35,
        sum(case when tagg36>0 then 1 else 0 end)  uv36,
        sum(case when tagg37>0 then 1 else 0 end)  uv37,
        sum(case when tagg38>0 then 1 else 0 end)  uv38,
        sum(case when tagg40>0 then 1 else 0 end)  uv40,
        sum(case when tagg41>0 then 1 else 0 end)  uv41,
        sum(case when tagg42>0 then 1 else 0 end)  uv42,
        sum(case when tagg43>0 then 1 else 0 end)  uv43,
        sum(case when tagg44>0 then 1 else 0 end)  uv44,
        sum(case when tagg45>0 then 1 else 0 end)  uv45,
        sum(case when tagg46>0 then 1 else 0 end)  uv46,
        sum(case when tagg47>0 then 1 else 0 end)  uv47,
        sum(case when tagg48>0 then 1 else 0 end)  uv48,
        sum(case when tagg49>0 then 1 else 0 end)  uv49,
        sum(case when tagg50>0 then 1 else 0 end)  uv50,
        sum(case when tagg51>0 then 1 else 0 end)  uv51,
        sum(case when tagg52>0 then 1 else 0 end)  uv52,
        sum(case when tagg53>0 then 1 else 0 end)  uv53,
        sum(case when tagg54>0 then 1 else 0 end)  uv54,
        sum(case when tagg55>0 then 1 else 0 end)  uv55,
        sum(case when tagg56>0 then 1 else 0 end)  uv56,
        sum(case when tagg57>0 then 1 else 0 end)  uv57,
        sum(case when tagg58>0 then 1 else 0 end)  uv58,
        sum(case when tagg59>0 then 1 else 0 end)  uv59,
        sum(case when tagg60>0 then 1 else 0 end)  uv60,
        sum(case when tagg61>0 then 1 else 0 end)  uv61,
        sum(case when tagg62>0 then 1 else 0 end)  uv62,
        sum(tagg_login) pv_1,
        sum(tagg0) pv0,
        sum(tagg1) pv1,
        sum(tagg2) pv2,
        sum(tagg3) pv3,
        sum(tagg4) pv4,
        sum(tagg5) pv5,
        sum(tagg6) pv6,
        sum(tagg7) pv7,
        sum(tagg8) pv8,
        sum(tagg9) pv9,
        sum(tagg10) pv10,
        sum(tagg11) pv11,
        sum(tagg12) pv12,
        sum(tagg13) pv13,
        sum(tagg14) pv14,
        sum(tagg15) pv15,
        sum(tagg16) pv16,
        sum(tagg17) pv17,
        sum(tagg18) pv18,
        sum(tagg19) pv19,
        sum(tagg21) pv21,
        sum(tagg22) pv22,
        sum(tagg23) pv23,
        sum(tagg24) pv24,
        sum(tagg25) pv25,
        sum(tagg26) pv26,
        sum(tagg27) pv27,
        sum(tagg28) pv28,
        sum(tagg29) pv29,
        sum(tagg31) pv31,
        sum(tagg32) pv32,
        sum(tagg33) pv33,
        sum(tagg34) pv34,
        sum(tagg35) pv35,
        sum(tagg36) pv36,
        sum(tagg37) pv37,
        sum(tagg38) pv38,
        sum(tagg40) pv40,
        sum(tagg41) pv41,
        sum(tagg42) pv42,
        sum(tagg43) pv43,
        sum(tagg44) pv44,
        sum(tagg45) pv45,
        sum(tagg46) pv46,
        sum(tagg47) pv47,
        sum(tagg48) pv48,
        sum(tagg49) pv49,
        sum(tagg50) pv50,
        sum(tagg51) pv51,
        sum(tagg52) pv52,
        sum(tagg53) pv53,
        sum(tagg54) pv54,
        sum(tagg55) pv55,
        sum(tagg56) pv56,
        sum(tagg57) pv57,
        sum(tagg58) pv58,
        sum(tagg59) pv59,
        sum(tagg60) pv60,
        sum(tagg61) pv61,
        sum(tagg62) pv62
  from 
    (select   pt_days,
              device_number,
              sum(case when tag_login=1 then 1 else 0 end) tagg_login,
              sum(case when tag_cf=1 then 1 else 0 end) tagg0,
              sum(case when tag1=1 then 1 else 0 end)    tagg1,
              sum(case when tag2=1 then 1 else 0 end)    tagg2,
              sum(case when tag3=1 then 1 else 0 end)    tagg3,
              sum(case when tag4=1  and tag_login=1    then 1 else 0 end)    tagg4,
              sum(case when tag5=1  and tag_login=1    then 1 else 0 end)    tagg5,
              sum(case when tag6=1  and tag_login=1    then 1 else 0 end)    tagg6,
              sum(case when tag7=1  and tag_login=1    then 1 else 0 end)    tagg7,
              sum(case when tag8=1  and tag_login=1    then 1 else 0 end)    tagg8,
              sum(case when tag9=1  and tag_login=1    then 1 else 0 end)    tagg9,
              sum(case when tag10=1  and tag_login=1    then 1 else 0 end)    tagg10,
              sum(case when tag11=1  and tag_login=1    then 1 else 0 end)    tagg11,
              sum(case when tag12=1  and tag_login=1    then 1 else 0 end)    tagg12,
              sum(case when tag13=1  and tag_login=1    then 1 else 0 end)    tagg13,
              sum(case when tag14=1  and tag_login=1    then 1 else 0 end)    tagg14,
              sum(case when tag15=1  and tag_login=1    then 1 else 0 end)    tagg15,
              sum(case when tag16=1  and tag_login=1    then 1 else 0 end)    tagg16,
              sum(case when tag17=1  and tag_login=1    then 1 else 0 end)    tagg17,
              sum(case when tag18=1  and tag_login=1    then 1 else 0 end)    tagg18,
              sum(case when tag19=1  and tag_login=1    then 1 else 0 end)    tagg19,
              sum(case when tag21=1  and tag_login=1    then 1 else 0 end)    tagg21,
              sum(case when tag22=1  and tag_login=1    then 1 else 0 end)    tagg22,
              sum(case when tag23=1  and tag_login=1    then 1 else 0 end)    tagg23,
              sum(case when tag24=1  and tag_login=1    then 1 else 0 end)    tagg24,
              sum(case when tag25=1  and tag_login=1    then 1 else 0 end)    tagg25,
              sum(case when tag26=1  and tag_login=1    then 1 else 0 end)    tagg26,
              sum(case when tag27=1  and tag_login=1    then 1 else 0 end)    tagg27,
              sum(case when tag28=1  and tag_login=1    then 1 else 0 end)    tagg28,
              sum(case when tag29=1  and tag_login=1    then 1 else 0 end)    tagg29,
              sum(case when tag31=1  and tag_login=1    then 1 else 0 end)    tagg31,
              sum(case when tag32=1  and tag_login=1    then 1 else 0 end)    tagg32,
              sum(case when tag33=1  and tag_login=1    then 1 else 0 end)    tagg33,
              sum(case when tag34=1  and tag_login=1    then 1 else 0 end)    tagg34,
              sum(case when tag35=1  and tag_login=1    then 1 else 0 end)    tagg35,
              sum(case when tag36=1  and tag_login=1    then 1 else 0 end)    tagg36,
              sum(case when tag37=1  and tag_login=1    then 1 else 0 end)    tagg37,
              sum(case when tag38=1  and tag_login=1    then 1 else 0 end)    tagg38,
              sum(case when tag40=1  and tag_login=1    then 1 else 0 end)    tagg40,
              sum(case when tag41=1  and tag_login=1    then 1 else 0 end)    tagg41,
              sum(case when tag42=1  and tag_login=1    then 1 else 0 end)    tagg42,
              sum(case when tag43=1  and tag_login=1    then 1 else 0 end)    tagg43,
              sum(case when tag44=1  and tag_login=1    then 1 else 0 end)    tagg44,
              sum(case when tag45=1  and tag_login=1    then 1 else 0 end)    tagg45,
              sum(case when tag46=1  and tag_login=1    then 1 else 0 end)    tagg46,
              sum(case when tag47=1  and tag_login=1    then 1 else 0 end)    tagg47,
              sum(case when tag48=1  and tag_login=1    then 1 else 0 end)    tagg48,
              sum(case when tag49=1  and tag_login=1    then 1 else 0 end)    tagg49,
              sum(case when tag50=1  and tag_login=1    then 1 else 0 end)    tagg50,
              sum(case when tag51=1  and tag_login=1    then 1 else 0 end)    tagg51,
              sum(case when tag52=1  and tag_login=1    then 1 else 0 end)    tagg52,
              sum(case when tag53=1  and tag_login=1    then 1 else 0 end)    tagg53,
              sum(case when tag54=1  and tag_login=1    then 1 else 0 end)    tagg54,
              sum(case when tag55=1  and tag_login=1    then 1 else 0 end)    tagg55,
              sum(case when tag56=1  and tag_login=1    then 1 else 0 end)    tagg56,
              sum(case when tag57=1  and tag_login=1    then 1 else 0 end)    tagg57,
              sum(case when tag58=1  and tag_login=1    then 1 else 0 end)    tagg58,
              sum(case when tag59=1  and tag_login=1    then 1 else 0 end)    tagg59,
              sum(case when tag60=1  and tag_login=1    then 1 else 0 end)    tagg60,
              sum(case when tag61=1  and tag_login=1    then 1 else 0 end)    tagg61,
              sum(case when tag62=1  and tag_login=1    then 1 else 0 end)    tagg62
    from ( select pt_days,
                  device_number,
                  hh,
                  max(case when instr(url_host,'antfortune.com')>0 then 1 else 0 end) tag_cf,
                  max(case when url_host='loggw.alipay.com' then 1 else 0 end) tag_login,
                  max(case when url_host='mdap.alipay.com' then 1 else 0 end) tag1,
                  max(case when url_host='mcgw.alipay.com' then 1 else 0 end) tag2,
                  max(case when url_host='mobilegw.alipay.com' then 1 else 0 end) tag3,
                  max(case when url_host='mfbizweb.19ego.cn' then 1 else 0 end) tag4,
                  max(case when url_host='api.mgzf.com' then 1 else 0 end) tag5,
                  max(case when url_host='ali.enmonster.com' then 1 else 0 end) tag6,
                  max(case when url_host='api.hellobike.com' then 1 else 0 end) tag7,
                  max(case when url_host='h5.ele.me' then 1 else 0 end) tag8,
                  max(case when url_host='pages.tmall.com' then 1 else 0 end) tag9,
                  max(case when url_host='h5.vip.youku.com' then 1 else 0 end) tag10,
                  max(case when url_host='campus.alipay-eco.com' then 1 else 0 end) tag11,
                  max(case when url_host='mycar-vbizplatform.alipay-eco.com' then 1 else 0 end) tag12,
                  max(case when url_host='mps.amap.com' then 1 else 0 end) tag13,
                  max(case when url_host='mtop.damai.cn' then 1 else 0 end) tag14,
                  max(case when url_host='apis.didapinche.com' then 1 else 0 end) tag15,
                  max(case when url_host='fragment.tmall.com' then 1 else 0 end) tag16,
                  max(case when url_host='s.click.tmall.com' then 1 else 0 end) tag17,
                  max(case when url_host='s.click.taobao.com' then 1 else 0 end) tag18,
                  max(case when instr(url_host,'zmxy.com')>0 then 1 else 0 end) tag19,
                  max(case when url_host='fundcaifuhao.antfortune.com' then 1 else 0 end) tag21,
                  max(case when url_host='magear.pangku.com' then 1 else 0 end) tag22,
                  max(case when url_host='aisle.amap.com' then 1 else 0 end) tag23,
                  max(case when url_host='easysearchcard.chuxingyouhui.com' then 1 else 0 end) tag24,
                  max(case when url_host='mcdj.qipinke.com' then 1 else 0 end) tag25,
                  max(case when url_host='huaka.cmicrwx.cn' then 1 else 0 end) tag26,
                  max(case when url_host='cbp.alipay-eco.com' then 1 else 0 end) tag27,
                  max(case when url_host='miniappweb.shuqireader.com' then 1 else 0 end) tag28,
                  max(case when url_host in ('orders.kfc.com.cn','imgorder.kfc.com.cn') then 1 else 0 end) tag29,
                  max(case when url_host='niaprod.alipay-eco.com' then 1 else 0 end) tag31,
                  max(case when url_host='app.woaizuji.com' then 1 else 0 end) tag32,
                  max(case when url_host='ubt.hellobike.com' then 1 else 0 end) tag33,
                  max(case when url_host='crnetzsjk.crnet.info' then 1 else 0 end) tag34,
                  max(case when url_host='imcd.jaxcx.com' then 1 else 0 end) tag35,
                  max(case when url_host='goveco.alipay.com' then 1 else 0 end) tag36,
                  max(case when url_host='www.mywhh.com' then 1 else 0 end) tag37,
                  max(case when url_host='scofo.ofo.so' then 1 else 0 end) tag38,
                  max(case when url_host='stockcaifuhao.antfortune.com' then 1 else 0 end) tag40,
                  max(case when url_host='advcaifuhao.antfortune.com' then 1 else 0 end) tag41,
                  max(case when url_host='pecaifuhao.antfortune.com' then 1 else 0 end) tag42,
                  max(case when url_host='cfh.tianhongjijin.com.cn' then 1 else 0 end) tag43,
                  max(case when url_host='render.antfortune.com' then 1 else 0 end) tag44,
                  max(case when url_host='mini.open.alipay.com' then 1 else 0 end) tag45,
                  max(case when url_host='apicall.id-photo-verify.com' then 1 else 0 end) tag46,
                  max(case when url_host='scalc-min.renliwo.com' then 1 else 0 end) tag47,
                  max(case when url_host='taxcal.renliwo.com' then 1 else 0 end) tag48,
                  max(case when url_host='ghws5.zj12580.cn' then 1 else 0 end) tag49,
                  max(case when url_host='api.zp315.cn' then 1 else 0 end) tag50,
                  max(case when url_host='api.songzhaopian.com' then 1 else 0 end) tag51,
                  max(case when url_host='m.rrzuji.com' then 1 else 0 end) tag52,
                  max(case when url_host='www.neisha.cc' then 1 else 0 end) tag53,
                  max(case when url_host='xcx.etong-online.com' then 1 else 0 end) tag54,
                  max(case when url_host='mys4s.cn' then 1 else 0 end) tag55,
                  max(case when url_host='lmf.gzluowang.com' then 1 else 0 end) tag56,
                  max(case when url_host='appgw.cmp520.com' then 1 else 0 end) tag57,
                  max(case when url_host='goveco.alipay.com' then 1 else 0 end) tag58,
                  max(case when url_host='cityboxapi.fruitday.com' then 1 else 0 end) tag59,
                  max(case when url_host='api.95vintage.com' then 1 else 0 end) tag60,
                  max(case when url_host='ai.wcar.net.cn' then 1 else 0 end) tag61,
                  max(case when url_host='sky.vip.youku.com' then 1 else 0 end) tag62
            from user_action_tag_d 
            where  pt_days>='20190428' and pt_days<'20190508'
              and  (url_host in ('loggw.alipay.com','mdap.alipay.com','mcgw.alipay.com','mobilegw.alipay.com','mfbizweb.19ego.cn','api.mgzf.com',
                            'ali.enmonster.com','api.hellobike.com','h5.ele.me','pages.tmall.com','h5.vip.youku.com','campus.alipay-eco.com',
                            'mycar-vbizplatform.alipay-eco.com','mps.amap.com','mtop.damai.cn','apis.didapinche.com','fragment.tmall.com',
                            's.click.tmall.com','s.click.taobao.com','fundcaifuhao.antfortune.com','magear.pangku.com',
                            'aisle.amap.com','easysearchcard.chuxingyouhui.com','mcdj.qipinke.com','huaka.cmicrwx.cn','cbp.alipay-eco.com',
                            'miniappweb.shuqireader.com','orders.kfc.com.cn','imgorder.kfc.com.cn','niaprod.alipay-eco.com','app.woaizuji.com',
                            'ubt.hellobike.com','crnetzsjk.crnet.info','imcd.jaxcx.com','goveco.alipay.com','www.mywhh.com','scofo.ofo.so',
                            'stockcaifuhao.antfortune.com','advcaifuhao.antfortune.com','pecaifuhao.antfortune.com',
                            'cfh.tianhongjijin.com.cn','render.antfortune.com','mini.open.alipay.com','apicall.id-photo-verify.com',
                            'scalc-min.renliwo.com','taxcal.renliwo.com','ghws5.zj12580.cn','api.zp315.cn','api.songzhaopian.com','m.rrzuji.com',
                            'www.neisha.cc','xcx.etong-online.com','mys4s.cn','lmf.gzluowang.com','appgw.cmp520.com','goveco.alipay.com',
                            'cityboxapi.fruitday.com','api.95vintage.com','ai.wcar.net.cn','sky.vip.youku.com') 
                    or url_host like '%.zmxy.com'
                    or instr(url_host,'zmxy.com.cn')>0
                    or instr(url_host,'antfortune.com')>0)
                    group by pt_days,
                          device_number,
                          hh) a
                group by  pt_days,
                          device_number
                    ) b
group by pt_days;





--各个host的数据统计
select '白名单域名统计量' model,url_host,count(*) cnt,count(distinct device_number) dau
from user_action_tag_d where  pt_days='20190502'  group by url_host;



select '新闻各app流失去向' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
     (select
          a.device_number,
          appname
      from  user_action_app_m a
      where a.pt_days='201903'
        and a.appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  user_action_app_m
                  where pt_days='201904'
                  and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
                  ) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)) b 
 left join 
        (select
                device_number,
                appname
            from  user_action_app_m
            where pt_days='201904'
             and appname in ('tencent_news','toutiao','kuaibao','wangyinews','sina_news','sohu_news','ifeng')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;



select '音乐各app新增来源' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
(select device_number,
        appname tarapp
     from 
       (select  pt_days,
                appname,
                device_number,
                row_number() over(partition by appname,device_number order by pt_days asc) rn
            from  user_action_app_d
            where appname in ('qq_music','kuwo','kugou','wangyi_music')             
            )  a
where rn=1
  and pt_days>='20190401' and pt_days<'20190501'
  group by device_number,
           appname) b 
 left join 
        (select
                device_number,
                appname
            from  user_action_app_m
            where pt_days='201903'
             and appname in ('qq_music','kuwo','kugou','wangyi_music')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;





select '音乐各app流失去向' model,
       b.tarapp,
       b1.appname srcapp,
       count(distinct b.device_number) uv
 from 
     (select
          a.device_number,
          appname
      from  user_action_app_m a
      where a.pt_days='201903'
        and a.appname in ('qq_music','kuwo','kugou','wangyi_music')       
        and not exists
          (select * from 
              (select
                      device_number,
                      appname
                  from  user_action_app_m
                  where pt_days='201904'
                  and appname in ('qq_music','kuwo','kugou','wangyi_music')       
                  ) a1 
              where a.appname=a1.appname and a.device_number=a1.device_number)) b 
 left join 
        (select
                device_number,
                appname
            from  user_action_app_m
            where pt_days='201904'
             and appname in ('qq_music','kuwo','kugou','wangyi_music')       
            )  b1
on  b.device_number=b1.device_number
group by b.tarapp,
         b1.appname;