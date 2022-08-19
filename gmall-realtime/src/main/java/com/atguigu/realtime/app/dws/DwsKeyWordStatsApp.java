package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.app.function.KeyWordUDTF;
import com.atguigu.realtime.bean.KeywordStats;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.SQLException;

import static com.atguigu.realtime.common.Constant.*;

public class DwsKeyWordStatsApp extends BaseSqlApp {
    
    public static void main(String[] args) {
        new DwsKeyWordStatsApp().init(5004, 1, "DwsKeyWordStatsApp");
    }
    
    @Override
    public void run(StreamTableEnvironment tEnv) throws SQLException, ClassNotFoundException {
        //1.建立动态表和Source联系
        tEnv
                .executeSql(
                        "create table page_log" +
                                "(" +
                                " common map<string,string>," +
                                " page map<string,string>," +
                                " ts bigint," +
                                //水印
                                // 需要先把long类型的ts转为yyyy-MM-dd类型的et
                                " et as to_timestamp(from_unixtime(ts/1000))," +
                                //然后再把这个et转为水印
                                " watermark for et as et - interval '3' second" +
                                ")" +
                                "with" +
                                "(" +
                                " 'connector' = 'kafka'," +
                                " 'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092'," +
                                " 'properties.group.id' = 'DwsKeywordStatsApp'," +
                                " 'topic' = '" + TOPIC_DWD_PAGE_LOG + "'," +
                                " 'scan.startup.mode' = 'latest-offset'," +
                                " 'format' = 'json'" +
                                ")" +
                                "");
        //2.查询出用户搜索的词
        Table t1 = tEnv
                .sqlQuery("" +
                        " select" +
                        " page['item'] kw," +
                        " et" +
                        " from page_log" +
                        " where " +
                        " page['item'] is not null " +
                        " and" +
                        " page['page_id'] = 'good_list'");
        
        tEnv.createTemporaryView("t1",t1);
    
        //3.分词
        //3.1注册自定义函数
        tEnv.createTemporaryFunction("ik_analyzer", KeyWordUDTF.class);
        //3.2使用自定义函数切词
        Table t2 = tEnv
                .sqlQuery("select" +
                        " kw, et, word" +
                        " from t1" +
                        " join lateral table(ik_analyzer(kw)) on true");
        tEnv.createTemporaryView("t2",t2);
    
        //4.开窗聚合
        Table resTable = tEnv
                .sqlQuery("select" +
                        " date_format(tumble_start(et, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                        " date_format(tumble_end(et, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt, " + " word keyword," +
                        " 'search' source," +
                        " count(*) ct," +
                        " unix_timestamp()*1000 ts" +
                        " from t2" +
                        " group by word, tumble(et, interval '10' second)");
    
        resTable.execute().print();
        //5.写入到clickhouse
        tEnv
                .toRetractStream(resTable, KeywordStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickhouseSink("gmall2021","keyword_stats_2021",KeywordStats.class));
    }
}
