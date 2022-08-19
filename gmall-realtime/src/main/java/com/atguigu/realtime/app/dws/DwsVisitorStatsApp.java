package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.VisitorStats;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.util.Strings;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashMap;

import static com.atguigu.realtime.common.Constant.*;

public class DwsVisitorStatsApp extends BaseAppV2 {
    
    public static void main(String[] args) {
        new DwsVisitorStatsApp().init(5001,1,"DwsVisitorStatsApp","DwsVisitorStatsApp",
                TOPIC_DWM_USER_JUMP_DETAIL,TOPIC_DWM_UV,TOPIC_DWD_PAGE_LOG);
    }
    @Override
    public void run(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> sourceStream) throws SQLException, ClassNotFoundException {
        
        //1.将流中的数据解析为同一类型 ->VisitorStats, 然后union
        DataStream<VisitorStats> unionStream = parseAndUnion(sourceStream);
        //2.分组开窗聚合
        SingleOutputStreamOperator<VisitorStats> reducedStream = reduceWindow(unionStream);
        //3.将数据写入到clickhouse
        reducedStream.print();
        writeToClickhouse(reducedStream);
    }
    
    private void writeToClickhouse(SingleOutputStreamOperator<VisitorStats> reducedStream) {
        //在JDBC基础上封装一个Sink
        reducedStream.addSink(FlinkSinkUtil.getClickhouseSink("gmall2021","visitor_stats_2021",VisitorStats.class));
    }
    
    private SingleOutputStreamOperator<VisitorStats> reduceWindow(DataStream<VisitorStats> unionStream) {
        SingleOutputStreamOperator<VisitorStats> reduceStream = unionStream
                .keyBy(vs -> vs.getVc() + "_" + vs.getCh() + "_" + vs.getAr() + "_" + vs.getIs_new())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats vs1, VisitorStats vs2) throws Exception {
                                vs1.setPv_ct(vs1.getPv_ct() + vs2.getPv_ct());
                                vs1.setUv_ct(vs1.getUv_ct() + vs2.getUv_ct());
                                vs1.setSv_ct(vs1.getSv_ct() + vs2.getSv_ct());
                                vs1.setUj_ct(vs1.getUj_ct() + vs2.getUj_ct());
                                vs1.setDur_sum(vs1.getDur_sum() + vs2.getDur_sum());
                                return vs1;
                            }
                        },
                        new ProcessWindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context context,
                                                Iterable<VisitorStats> elements,
                                                Collector<VisitorStats> out) throws Exception {
                            
                                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                String start = formatter.format(context.window().getStart());
                                String end = formatter.format(context.window().getEnd());
                            
                                VisitorStats visitorStats = elements.iterator().next();
                                visitorStats.setStt(start);
                                visitorStats.setEdt(end);
                            
                                out.collect(visitorStats);
                            }
                        });
        return reduceStream;
    }
    
    private DataStream<VisitorStats> parseAndUnion(HashMap<String, DataStreamSource<String>> sourceStream) {
        DataStreamSource<String> pageStream = sourceStream.get(TOPIC_DWD_PAGE_LOG);
        DataStreamSource<String> uvStream = sourceStream.get(TOPIC_DWM_UV);
        DataStreamSource<String> jumpStream = sourceStream.get(TOPIC_DWM_USER_JUMP_DETAIL);
        
        //1.pv
        SingleOutputStreamOperator<VisitorStats> pvStatsStream = pageStream
                //将jsonStr变为jsonObject
                .map(json -> {
                    JSONObject object = JSON.parseObject(json);
                    JSONObject common = object.getJSONObject("common");
                    JSONObject page = object.getJSONObject("page");
                
                    return new VisitorStats(
                            "", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 1L, 0L, 0L,
                            page.getLong("during_time"),
                            object.getLong("ts")
                
                    );
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((vs, ts) -> vs.getTs()));
    
        //2.uv
        SingleOutputStreamOperator<VisitorStats> uvStatsStream = uvStream
                //将jsonStr变为jsonObject
                .map(json -> {
                    JSONObject object = JSON.parseObject(json);
                    JSONObject common = object.getJSONObject("common");
                    JSONObject page = object.getJSONObject("page");
                
                    return new VisitorStats(
                            "", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            1L, 0L, 0L, 0L, 0L,
                            object.getLong("ts")
                    );
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((vs, ts) -> vs.getTs()));
    
        //3.jump
        SingleOutputStreamOperator<VisitorStats> jumpStatsStream = jumpStream
                //将jsonStr变为jsonObject
                .map(json -> {
                    JSONObject object = JSON.parseObject(json);
                    JSONObject common = object.getJSONObject("common");
                    JSONObject page = object.getJSONObject("page");
                
                    return new VisitorStats(
                            "", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 0L, 0L, 1L, 0L,
                            object.getLong("ts")
                    );
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((vs, ts) -> vs.getTs()));
    
        //4.进入次数
        SingleOutputStreamOperator<VisitorStats> svStateStream = pageStream
                .flatMap(new FlatMapFunction<String, VisitorStats>() {
                    @Override
                    public void flatMap(String value, Collector<VisitorStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");
                        JSONObject page = jsonObject.getJSONObject("page");
                        String last_page_id = page.getString("last_page_id");
                        if (Strings.isEmpty(last_page_id)) {
                            VisitorStats vs = new VisitorStats(
                                    "", "",
                                    common.getString("vc"),
                                    common.getString("ch"),
                                    common.getString("ar"),
                                    common.getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L,
                                    jsonObject.getLong("ts")
                            );
                            out.collect(vs);
                        }
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                );
        
        return pvStatsStream.union(uvStatsStream,jumpStatsStream,svStateStream);
    }
}
