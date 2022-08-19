package com.atguigu.realtime.app;

import com.atguigu.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;


public abstract class BaseSqlApp {
    public abstract void run(StreamTableEnvironment tEnv) throws SQLException, ClassNotFoundException;
    
    public void init(int port,
                     int p,
                     String ck) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
        
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/flink-realtime/ck/" + ck);
        
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        try {
            run(tEnv);
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }
    
        try {
            env.execute(ck);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
