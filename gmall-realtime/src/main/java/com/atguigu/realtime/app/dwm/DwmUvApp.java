package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.CommonUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class DwmUvApp extends BaseAppV1 {
    
    public static void main(String[] args) {
        new DwmUvApp().init(3001,1,"DwmUvApp","DwmUvApp", Constant.TOPIC_DWD_PAGE_LOG);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        source
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((obj,ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    ValueState<Long> firstWindowState;
                    SimpleDateFormat formatter;
    
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstWindowState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("firstWindow", Long.class));
                        formatter = new SimpleDateFormat("yyyy-MM-dd");
                    }
    
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        //根据window结束时间来确定今天的时间
                        String today = formatter.format(context.window().getEnd());
                        String last = formatter.format(firstWindowState.value() == null ? 0L : firstWindowState.value());
                        
                        if (!today.equals(last)){
                            firstWindowState.clear();
                        }
                        
                        if (firstWindowState.value() == null) {
                            List<JSONObject> list = CommonUtil.toList(elements);
                            JSONObject first = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                            out.collect(first);
                            firstWindowState.update(first.getLong("ts"));
                        }
                    }
                })
                .map(JSON::toString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWM_UV));
    }
}
