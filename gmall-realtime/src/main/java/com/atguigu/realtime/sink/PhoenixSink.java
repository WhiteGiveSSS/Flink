package com.atguigu.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.RedisUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    
    ValueState<Boolean> firstTableState;
    Connection connection;
    //初始化,建立连接
    @Override
    public void open(Configuration parameters) throws Exception {
        //使用标准的JDBC建立Phoenix连接
        //1.加载驱动(常见数据库不需要加载,java会根据URL自动加载,但是Phoenix需要)
        Class.forName(Constant.PHOENIX_DRIVER);
        //2.获取连接对象
        //连接对象不能作为状态: 1.无法序列化 2.端口占用问题
        connection = DriverManager.getConnection(Constant.PHOENIX_URL);
        
        //设置第一次建表的状态
        firstTableState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("firstTable", Boolean.class));
    }
    
    //处理每条数据 -> 每条数据的组成是 data + 配置表记录
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        //1.判断表是否存在
        checkTable(value);
        //2.把这条数据写入到Phoenix中
        writeToHBase(value);
        //3.更新redis缓存 -> 如果这次操作是update,去cache中删除记录
        delCache(value);
    }
    
    private void delCache(Tuple2<JSONObject, TableProcess> value) {
        String key = value.f1.getSinkTable().toUpperCase(Locale.ROOT) + ":" + value.f0.get("id");
        Jedis jedis = RedisUtil.getRedis();
        //如果key不存在,直接删除不会有影响
        jedis.select(1);
        jedis.del(key);
        jedis.close();
    }
    
    private void writeToHBase(Tuple2<JSONObject, TableProcess> value) throws SQLException {
        JSONObject data = value.f0;
        TableProcess tableProcess = value.f1;
    
        String[] columns = tableProcess.getSinkColumns().split(",");
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tableProcess.getSinkTable())
                .append(" (")
                .append(tableProcess.getSinkColumns())
                .append(" )values(");
        for (int i = 0; i < columns.length; i++) {
            sql.append("?,");
        }
        sql
                .deleteCharAt(sql.length() - 1)
                .append(")");
        
        
        PreparedStatement ps = connection.prepareStatement(sql.toString());
        //PreparedStatement,再赋值
        for (int i = 0; i < columns.length; i++) {
            String str = data.get(columns[i]) == null ? "" : data.get(columns[i]).toString();
            ps.setString(i + 1,str);
        }
        
        ps.execute();
        connection.commit();
        ps.close();
    }
    
    private void checkTable(Tuple2<JSONObject, TableProcess> value) throws SQLException, IOException {
        if (firstTableState.value() == null){
    
            //1.编写SQL
            TableProcess tp = value.f1;
            StringBuilder sql = new StringBuilder();
            sql
                    .append("create table if not exists ")
                    .append(tp.getSinkTable())
                    .append("(");
        /*for (String c : tp.getSinkColumns().split(",")) {
            sql.append(c).append(" varchar,");
        }
        sql.deleteCharAt(sql.length() - 1); // 去掉最后一个逗号*/
            sql
                    .append(tp.getSinkColumns().replaceAll(",", " varchar,"))
                    .append(" varchar, constraint pk primary key(");
            // 拼接主键
            sql.append(tp.getSinkPk() == null ? "id" : tp.getSinkPk());
    
            sql
                    .append("))")
                    .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
            //2.预处理
            System.out.println(sql.toString());
            PreparedStatement ps = connection.prepareStatement(sql.toString());
            //3.设置参数
    
            //4.执行,commit,关闭ps
            ps.execute();
            connection.commit();
            ps.close();
            
            firstTableState.update(false);
        }
    }
    
    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}
