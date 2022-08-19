package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.CommonUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;

public class DwdLogApp extends BaseAppV1 {
    
    public static void main(String[] args) {
        new DwdLogApp().init(2001,1,"DwdLogApp","DwdLogApp", Constant.TOPIC_ODS_LOG);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        //1.识别新老用户
        SingleOutputStreamOperator<JSONObject> distinguishedStream = distinguishNerUser(source);
        //2.分流
        Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>> splitStream = splitStream(distinguishedStream);
        //3.写入不同的topic
        writeToKafka(splitStream);
    }
    
    private void writeToKafka(Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>> splitStream) {
        splitStream.f0.map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_START_LOG));
        splitStream.f1.map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_PAGE_LOG));
        splitStream.f2.map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_DISPLAY_LOG));
    }
    
    private Tuple3<SingleOutputStreamOperator<JSONObject>, DataStream<JSONObject>, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> distinguishedStream) {
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {};
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("displayTag") {};
        // 启动日志: 主流   页面和曝光进入侧输出流
        SingleOutputStreamOperator<JSONObject> splitStream = distinguishedStream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject input,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        JSONObject start = input.getJSONObject("start");
                        if (start != null) { // 启动日志
                            // 把启动日志写入到主流中
                            out.collect(input);
                        } else {
                        
                            // 1. 如果是页面
                            JSONObject page = input.getJSONObject("page");
                            if (page != null) {
                                ctx.output(pageTag, input);
                            }
                            // 2. 如果曝光
                            JSONArray displays = input.getJSONArray("displays");
                            if (displays != null) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    // 把一些其他信息插入到display中
                                    display.put("ts", input.getLong("ts"));
                                    display.put("page_id", input.getJSONObject("page").getString("page_id"));
                                
                                    display.putAll(input.getJSONObject("common"));
                                
                                    ctx.output(displayTag, display);
                                }
                            }
                        }
                    }
                });
        return Tuple3.of(splitStream,splitStream.getSideOutput(pageTag),splitStream.getSideOutput(displayTag));
    }
    
    
    /*
        is_new不准确,需要重新设置
        因为有网络延迟,所以使用EventTime水印
        全体都有数据,所以需要开窗并且加状态
        只有第一个窗口的第一条(EventTime最早)数据需要设置为new
     */
    private SingleOutputStreamOperator<JSONObject> distinguishNerUser(DataStreamSource<String> source) {
        return source
                .map(JSON::parseObject)
                //先加水印在keyBy
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((obj,ts) -> obj.getLong("ts")))
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
    
                    ValueState<Boolean> isNewState;
                    
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isNewState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isNew", Boolean.class));
                    }
    
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        //第一次登录
                        if (isNewState.value() == null){
                            List<JSONObject> list = CommonUtil.toList(elements);
                            list.sort(Comparator.comparing(o -> o.getLong("ts")));
                            for (int i = 0; i < list.size(); i++) {
                                JSONObject jsonObject = list.get(i).getJSONObject("common");
                                if (i == 0){
                                    jsonObject.put("is_new",1);
                                    isNewState.update(false);
                                } else {
                                    jsonObject.put("is_new",0);
                                }
                                out.collect(list.get(i));
                            }
                        } else {
                            for (JSONObject element : elements) {
                                JSONObject jsonObject = element.getJSONObject("common");
                                jsonObject.put("is_new", 0);
                                out.collect(jsonObject);
                            }
                        }
                        
                    }
                });
                
                
    }
}
