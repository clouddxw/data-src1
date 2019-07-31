select * from user_action_context_out
where ((pt_days >='20190601' and pt_days<='20190631') or (pt_days >='20190710' and pt_days<='20190722' )) 
and tx_con in ('toutiao_article','tencent_news_article','tencent_news_video','ximalaya');