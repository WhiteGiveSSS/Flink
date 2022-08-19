package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/*
    需求：统计从主页直接退出的用户
    实现：使用CEP匹配,将没有跳出的用户匹配到,需求的用户是超时用户
 */
public class DwmJumpApp extends BaseAppV1 {
    
    public static void main(String[] args) {
        new DwmJumpApp().init(3002, 1, "DwmJumpApp", "DwmJumpApp", Constant.TOPIC_DWD_PAGE_LOG);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        
        //1.获取流
//        source = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000} ",
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":2000} ",
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":3000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":1000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":7000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":50000} "
//                );
        KeyedStream<JSONObject, String> stream = source
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((obj, ts) -> obj.getLong("ts")))
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        
        //2.设置CEP规则
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .next("last")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() == 0;
                    }
                })
                .within(Time.seconds(5));
        //3.将CEP匹配到流
        PatternStream<JSONObject> patternStream = CEP.pattern(stream, pattern);
        //4.选取输出
        SingleOutputStreamOperator<JSONObject> select = patternStream
                .select(new OutputTag<JSONObject>("jump") {},
                        new PatternTimeoutFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                                return pattern.get("first").get(0);
                            }
                        },
                        new PatternSelectFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                                return pattern.get("first").get(0);
                            }
                        });
        //union将超时的和正常的合并在一起
        select
                .union(select.getSideOutput(new OutputTag<JSONObject>("jump") {
                }))
                .map(JSONObject::toString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_USER_JUMP_DETAIL));
    }
}
