package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.annotation.NoSink;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.bean.VisitorStats;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.sink.PhoenixSink;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import static com.atguigu.realtime.common.Constant.*;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/25 9:20
 */
public class FlinkSinkUtil {
    // 得到一个kafka source
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + ""); // broker要求事务超时时间不能超过15分钟
        return new FlinkKafkaProducer<String>(
                topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, null, element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
    
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + ""); // broker要求事务超时时间不能超过15分钟
        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                "default",//默认topic,用不上,随便写
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element,
                                                                    @Nullable Long timestamp) {
                        return new ProducerRecord<>(element.f1.getSinkTable(), element.f0.toJSONString().getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
    
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getHBaseSink() {
        //自定义Sink:需要和JDBC连接 -> 继承Rich版本的SinkFunction
        return new PhoenixSink();
    }
    
    /*
        反射
        1.getDeclaredFields可以获得所有属性, getFields只能获得public属性
        2.获取对象: filedName.get(对象的实例) == 对象的实例.filedName
     */
    public static <T> SinkFunction<T> getClickhouseSink(String database, String table, Class<T> tClass) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert into ").append(table).append("(");
        
        Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            //反射时遇到注解为空时才执行
            NoSink noSink = field.getAnnotation(NoSink.class);
            if (noSink == null) {
                sql.append(field.getName() + ",");
            }
        }
        
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")values(");
        
        for (Field field : fields) {
            NoSink noSink = field.getAnnotation(NoSink.class);
            if (noSink == null) {
                sql.append("?,");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        
        String url = CLICKHOUSE_URL_PRE + database;
        return getJDBCSink(sql.toString(), CLICKHOUSE_DRIVER, url);
    }
    
    public static <T> SinkFunction<T> getJDBCSink(String sql, String driver, String url) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    //给占位符赋值
                    public void accept(PreparedStatement ps, T t) throws SQLException {
                        Field[] fields = t.getClass().getDeclaredFields();
                        for (int i = 0, position = 1; i < fields.length; i++) {
                            // 给属性添加访问权限
                            NoSink noSink = fields[i].getAnnotation(NoSink.class);
                            if (noSink == null) {
                                fields[i].setAccessible(true);
                                Object obj = fields[i].get(t);
                                ps.setObject(position++, obj);
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder().withBatchIntervalMs(1000).withBatchSize(1024).withMaxRetries(3).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withDriverName(driver).withUrl(url).build());
    }
    
    public static void main(String[] args) {
        getClickhouseSink("asd", "123", VisitorStats.class);
    }
}
