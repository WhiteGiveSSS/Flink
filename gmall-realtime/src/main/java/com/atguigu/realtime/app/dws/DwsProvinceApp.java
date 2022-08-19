package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.bean.ProvinceStats;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.SQLException;

import static com.atguigu.realtime.common.Constant.TOPIC_DWM_ORDER_WIDE;
import static com.atguigu.realtime.common.Constant.TOPIC_DWS_PRODUCT_STATS;

public class DwsProvinceApp extends BaseSqlApp {
    public static void main(String[] args) {
        new DwsProvinceApp().init(5003,1,"DwsProvinceApp");
    }
    @Override
    public void run(StreamTableEnvironment tEnv) throws SQLException, ClassNotFoundException {
        //1.建一个动态表和kafka的topic关联
        tEnv.executeSql("create table order_wide(" +
                "   province_id bigint, " +
                "   province_name string, " +
                "   province_area_code string, " +
                "   province_iso_code string, " +
                "   province_3166_2_code string, " +
                "   order_id bigint, " +
                "   split_total_amount decimal(20, 2)," +
                "   create_time string, " +
                "   et as to_timestamp(create_time), " +
                "   watermark for et as et - interval '5' second " +
                ")with(" +
                "   'connector' = 'kafka'," +
                "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092'," +
                "   'properties.group.id' = 'DwsProvinceStatsApp'," +
                "   'topic' = '" + TOPIC_DWM_ORDER_WIDE + "'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'json'" +
                ")");
        
        //2.在动态表上执行查询
        Table resultTable = tEnv
                .sqlQuery("select " +
                        " date_format(tumble_start(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                        " date_format(tumble_end(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                        " province_id, " +
                        " province_name, " +
                        " province_area_code area_code, " +
                        " province_iso_code iso_code, " +
                        " province_3166_2_code iso_3166_2," +
                        " sum(split_total_amount) order_amount, " +
                        " count(distinct(order_id)) order_count, " +
                        " unix_timestamp() * 1000 ts " +
                        "from order_wide " +
                        "group by " +
                        " tumble(et, interval '5' second), " +
                        " province_id, " +
                        " province_name, " +
                        " province_area_code, " +
                        " province_iso_code, " +
                        " province_3166_2_code");
        
        //3.把结果写入clickhouse的动态表中
        tEnv
                .toRetractStream(resultTable, ProvinceStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickhouseSink("gmall2021", "province_stats_2021", ProvinceStats.class));
    }
}
