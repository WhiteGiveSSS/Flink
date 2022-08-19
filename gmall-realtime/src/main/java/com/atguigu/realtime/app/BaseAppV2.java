package com.atguigu.realtime.app;

import com.atguigu.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;


public abstract class BaseAppV2 {
    public abstract void run(StreamExecutionEnvironment env,
                             HashMap<String, DataStreamSource<String>> sourceStream ) throws SQLException, ClassNotFoundException;
    
    public void init(int port,
                     int p,
                     String ck,
                     String groupId,
                     String topic,
                     String ...otherTopic) {
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
        
        // 具体的业务
        ArrayList<String> topicList = new ArrayList<>(Arrays.asList(otherTopic));
        topicList.add(topic);
    
        HashMap<String, DataStreamSource<String>> topicMap = new HashMap<>();
    
        for (String t : topicList) {
            DataStreamSource<String> sourceStream = env
                    .addSource(FlinkSourceUtil.getKafkaSource(groupId, t));
            topicMap.put(t,sourceStream);
        }
    
        
        
//        sourceStream.print();  // 不同的应用有不同的业务
        try {
            run(env, topicMap);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    
        try {
            env.execute(ck);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
