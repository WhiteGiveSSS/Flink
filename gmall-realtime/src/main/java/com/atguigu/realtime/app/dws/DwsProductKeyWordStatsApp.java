package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.app.function.ExplodeUDTF;
import com.atguigu.realtime.app.function.KeyWordUDTF;
import com.atguigu.realtime.bean.KeywordStats;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.SQLException;

import static com.atguigu.realtime.common.Constant.*;

public class DwsProductKeyWordStatsApp extends BaseSqlApp {
    
    public static void main(String[] args) {
        new DwsProductKeyWordStatsApp().init(5005, 1, "DwsProductKeyWordStatsApp");
    }
    
    @Override
    public void run(StreamTableEnvironment tEnv) throws SQLException, ClassNotFoundException {
        //1.从kafka读取数据
        tEnv
                .executeSql("create table product_stats" +
                        "(" +
                        " stt string, " +
                        " edt string, " +
                        " sku_name string, " +
                        " click_ct bigint, " +
                        " order_ct bigint, " +
                        " cart_ct bigint " +
                        ")" +
                        "with" +
                        "(" +
                        "   'connector' = 'kafka'," +
                        "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092'," +
                        "   'properties.group.id' = 'DwsProductKeywordStatsApp'," +
                        "   'topic' = '" + TOPIC_DWS_PRODUCT_STATS + "'," +
                        "   'scan.startup.mode' = 'latest-offset'," +
                        "   'format' = 'json'" +
                        ")");
        
        //2.过滤出至少一个不为0的记录
        Table t1 = tEnv
                .sqlQuery("select * " +
                        " from product_stats" +
                        " where click_ct >0 or order_ct >0 or cart_ct >0");
        tEnv.createTemporaryView("t1", t1);
        
        //3.切词
        tEnv.createTemporaryFunction("ik_analyzer", KeyWordUDTF.class);
        Table t2 = tEnv
                .sqlQuery("select" +
                        " stt, " +
                        " edt, " +
                        " word, " +
                        " click_ct," +
                        " order_ct, " +
                        " cart_ct " +
                        " from t1" +
                        " join lateral table(ik_analyzer(sku_name)) on true");
        tEnv.createTemporaryView("t2", t2);
        
        //4.聚合
        Table t3 = tEnv
                .sqlQuery("select" +
                        " stt, edt, word," +
                        " sum(click_ct) click_ct," +
                        " sum(order_ct) order_ct," +
                        " sum(cart_ct) cart_ct" +
                        " from t2" +
                        " group by stt, edt, word");
        tEnv.createTemporaryView("t3",t3);
        
        //5.将统计结果从一行变为多行
        tEnv.createTemporaryFunction("my_explode_function", ExplodeUDTF.class);
        Table resTable = tEnv
                .sqlQuery("select" +
                        " stt, " +
                        " edt, " +
                        " word keyword," +
                        " source," +
                        " ct," +
                        " unix_timestamp()*1000 ts" +
                        " from t3," +
                        " lateral table(my_explode_function(click_ct,order_ct,cart_ct))");
    
        //6.写入clickhouse
        tEnv
                .toRetractStream(resTable, KeywordStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickhouseSink("gmall2021", "keyword_stats_2021", KeywordStats.class));
    }
}
