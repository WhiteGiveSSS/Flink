package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.bean.PaymentInfo;
import com.atguigu.realtime.bean.PaymentWide;
import com.atguigu.realtime.common.Constant.*;
import com.atguigu.realtime.util.CommonUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;

import static com.atguigu.realtime.common.Constant.*;

public class DwmPaymentWide extends BaseAppV2 {
    
    public static void main(String[] args) {
        new DwmPaymentWide().init(4001,1,"DwmPaymentWide","DwmPaymentWide", TOPIC_DWD_PAYMENT_INFO,TOPIC_DWM_ORDER_WIDE);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> sourceStream) throws SQLException, ClassNotFoundException {
        KeyedStream<PaymentInfo, Long> paymentInfoStream = sourceStream
                .get(TOPIC_DWD_PAYMENT_INFO)
                .map(obj -> JSON.parseObject(obj, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((obj,ts) -> CommonUtil.getTs(obj.getCreate_time())))
                .keyBy(PaymentInfo::getOrder_id);
    
        KeyedStream<OrderWide, Long> orderWideStream = sourceStream
                .get(TOPIC_DWM_ORDER_WIDE)
                .map(obj -> JSON.parseObject(obj, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((obj,ts) -> CommonUtil.getTs(obj.getCreate_time())))
                .keyBy(OrderWide::getOrder_id);
        
        //interval join
        paymentInfoStream
                .intervalJoin(orderWideStream)
                //左右连个Time分别属于左右两个表
                //因为支付一般会很晚,所以找前45分钟的,而order基本不需要(5s就够)
                .between(Time.minutes(-45),Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left,right));
                    }
                })
                .map(JSON::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(TOPIC_DWM_PAYMENT_WIDE));
    
    }
}
