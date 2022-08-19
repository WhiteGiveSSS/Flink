package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.app.function.DwmAsyncFunction;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.bean.PaymentWide;
import com.atguigu.realtime.bean.ProductStats;
import com.atguigu.realtime.util.CommonUtil;
import com.atguigu.realtime.util.DimUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.atguigu.realtime.common.Constant.*;

public class DwsProductStatsApp extends BaseAppV2 {
    
    public static void main(String[] args) {
        new DwsProductStatsApp().init(5002, 1, "DwsProductStatsApp", "DwsProductStatsApp",
                TOPIC_DWD_PAGE_LOG, TOPIC_DWD_DISPLAY_LOG,
                TOPIC_DWD_FAVOR_INFO, TOPIC_DWD_CART_INFO,
                TOPIC_DWM_ORDER_WIDE, TOPIC_DWM_PAYMENT_WIDE,
                TOPIC_DWD_ORDER_REFUND_INFO, TOPIC_DWD_COMMENT_INFO);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> sourceStream) throws SQLException, ClassNotFoundException {
        //1.8个流union在一起
        DataStream<ProductStats> unionStream = parseAndUnion(sourceStream);
        //2.聚合
        SingleOutputStreamOperator<ProductStats> reducedStream = reduceStream(unionStream);
        //3.补充维度信息 -> async
        SingleOutputStreamOperator<ProductStats> dimJoinedStream = joinDim(reducedStream);
        //4.写入clickhouse
        writeToClickhouse(dimJoinedStream);
        //5.写入到kafka(后续需要从kafka中读取,无法直接从clickhouse读)
        writeToKafka(dimJoinedStream);
        dimJoinedStream.print();
    }
    
    private void writeToKafka(SingleOutputStreamOperator<ProductStats> dimJoinedStream) {
        dimJoinedStream
                .map(JSON::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(TOPIC_DWS_PRODUCT_STATS));
    }
    
    private void writeToClickhouse(SingleOutputStreamOperator<ProductStats> dimJoinedStream) {
        dimJoinedStream.print();
        dimJoinedStream.addSink(FlinkSinkUtil.getClickhouseSink("gmall2021", "product_stats_2021", ProductStats.class));
    }
    
    private SingleOutputStreamOperator<ProductStats> joinDim(SingleOutputStreamOperator<ProductStats> reducedStream) {
        return AsyncDataStream
                .unorderedWait(
                        reducedStream,
                        new DwmAsyncFunction<ProductStats>() {
                            @Override
                            public void asyncDimJoin(Connection jdbc,
                                                     Jedis jedis,
                                                     ProductStats input,
                                                     ResultFuture<ProductStats> resultFuture) throws Exception {
                                //1.补齐sku信息, name,spu_id,price,tm_id,category3_id
                                JSONObject skuInfo = DimUtil.readDim(jdbc, jedis, DIM_SKU_INFO, input.getSku_id());
                                input.setSku_name(skuInfo.getString("SKU_NAME"));
                                input.setSku_price(skuInfo.getBigDecimal("PRICE"));
                                input.setSpu_id(skuInfo.getLong("SPU_ID"));
                                input.setTm_id(skuInfo.getLong("TM_ID"));
                                input.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                                //2.补齐spu信息, spu_name
                                JSONObject spuInfo = DimUtil.readDim(jdbc, jedis, DIM_SPU_INFO, input.getSpu_id());
                                input.setSpu_name(spuInfo.getString("SPU_NAME"));
                                //3.补齐tm信息
                                JSONObject tmInfo = DimUtil.readDim(jdbc, jedis, DIM_BASE_TRADEMARK, input.getTm_id());
                                input.setTm_name(tmInfo.getString("TM_NAME"));
                                //4.补齐c3信息
                                JSONObject c3Info = DimUtil.readDim(jdbc, jedis, DIM_BASE_CATEGORY3, input.getCategory3_id());
                                input.setCategory3_name(c3Info.getString("NAME"));
                                
                                resultFuture.complete(Collections.singleton(input));
                            }
                        },
                        300,
                        TimeUnit.SECONDS
                );
    }
    
    private SingleOutputStreamOperator<ProductStats> reduceStream(DataStream<ProductStats> unionStream) {
        SingleOutputStreamOperator<ProductStats> reducedStats = unionStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((ps, ts) -> ps.getTs()))
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats ps1, ProductStats ps2) throws Exception {
                                ps1.setDisplay_ct(ps1.getDisplay_ct() + ps2.getDisplay_ct());
                                ps1.setClick_ct(ps1.getClick_ct() + ps2.getClick_ct());
                                ps1.setFavor_ct(ps1.getFavor_ct() + ps2.getFavor_ct());
                                ps1.setCart_ct(ps1.getCart_ct() + ps2.getCart_ct());
                        
                                ps1.setOrder_amount(ps1.getOrder_amount().add(ps2.getOrder_amount()));
                                ps1.setOrder_sku_num(ps1.getOrder_sku_num() + ps2.getOrder_sku_num());
                                ps1.getOrderIdSet().addAll(ps2.getOrderIdSet());
                        
                                ps1.setPayment_amount(ps1.getPayment_amount().add(ps2.getPayment_amount()));
                                ps1.getPaidOrderIdSet().addAll(ps2.getPaidOrderIdSet());
                        
                                ps1.setRefund_amount(ps1.getRefund_amount().add(ps2.getRefund_amount()));
                                ps1.getRefundOrderIdSet().addAll(ps2.getRefundOrderIdSet());
                        
                                ps1.setComment_ct(ps1.getComment_ct() + ps2.getComment_ct());
                                ps1.setGood_comment_ct(ps1.getGood_comment_ct() + ps2.getGood_comment_ct());
                        
                                return ps1;
                            }
                        },
                        new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void process(Long key,
                                                Context context,
                                                Iterable<ProductStats> elements,
                                                Collector<ProductStats> out) throws Exception {
                                ProductStats productStats = elements.iterator().next();
                                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                productStats.setStt(formatter.format(context.window().getStart()));
                                productStats.setEdt(formatter.format(context.window().getEnd()));
                                //计算set中的数量
                                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());
                                
                                out.collect(productStats);
                            }
                        });
        
        return reducedStats;
    }
    
    private DataStream<ProductStats> parseAndUnion(HashMap<String, DataStreamSource<String>> sourceStream) {
        //1.点击量
        SingleOutputStreamOperator<ProductStats> clickStats = sourceStream
                .get(TOPIC_DWD_PAGE_LOG)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        JSONObject page = jsonObject.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        if ("good_detail".equals(pageId)) {
                            ProductStats productStats = new ProductStats();
                            productStats.setSku_id(page.getLong("item"));
                            productStats.setTs(jsonObject.getLong("ts"));
                            productStats.setClick_ct(1L);
                            out.collect(productStats);
                        }
                    }
                });
        //2.曝光量
        SingleOutputStreamOperator<ProductStats> displayStats = sourceStream
                .get(TOPIC_DWD_DISPLAY_LOG)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String itemType = jsonObject.getString("item_type");
                        if ("sku_id".equals(itemType)) {
                            ProductStats productStats = new ProductStats();
                            productStats.setSku_id(jsonObject.getLong("item"));
                            productStats.setTs(jsonObject.getLong("ts"));
                            productStats.setDisplay_ct(1L);
                            out.collect(productStats);
                        }
                    }
                });
        
        //3.收藏量
        SingleOutputStreamOperator<ProductStats> favorStats = sourceStream
                .get(TOPIC_DWD_FAVOR_INFO)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        ProductStats productStats = new ProductStats();
                        productStats.setSku_id(jsonObject.getLong("sku_id"));
                        productStats.setTs(CommonUtil.getTs(jsonObject.getString("create_time")));
                        productStats.setFavor_ct(1L);
                        out.collect(productStats);
                    }
                });
        
        //4.购物车商品数量
        SingleOutputStreamOperator<ProductStats> cartStats = sourceStream
                .get(TOPIC_DWD_CART_INFO)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        ProductStats productStats = new ProductStats();
                        productStats.setSku_id(jsonObject.getLong("sku_id"));
                        productStats.setTs(CommonUtil.getTs(jsonObject.getString("create_time")));
                        productStats.setCart_ct(1L);
                    }
                });
        
        
        //5.订单量
        SingleOutputStreamOperator<ProductStats> orderStats = sourceStream
                .get(TOPIC_DWM_ORDER_WIDE)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                        OrderWide orderWide = JSON.parseObject(value, OrderWide.class);
                        ProductStats productStats = new ProductStats();
                        productStats.setSku_id(orderWide.getSku_id());
                        productStats.setTs(CommonUtil.getTs(orderWide.getCreate_time()));
                        productStats.setOrder_amount(orderWide.getTotal_amount());
                        productStats.setOrder_sku_num(orderWide.getSku_num());
                        //将order_id放入set中,最后获取长度
                        productStats.getOrderIdSet().add(orderWide.getOrder_id());
                        out.collect(productStats);
                    }
                });
        
        //6.支付数量
        SingleOutputStreamOperator<ProductStats> paymentStats = sourceStream
                .get(TOPIC_DWM_PAYMENT_WIDE)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                        PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);
                        ProductStats productStats = new ProductStats();
                        productStats.setSku_id(paymentWide.getSku_id());
                        productStats.setTs(CommonUtil.getTs(paymentWide.getPayment_create_time()));
                        productStats.setPayment_amount(paymentWide.getTotal_amount());
                        productStats.getPaidOrderIdSet().add(paymentWide.getOrder_id());
                        out.collect(productStats);
                    }
                });
        
        
        //7.退款量
        SingleOutputStreamOperator<ProductStats> refundStats = sourceStream
                .get(TOPIC_DWD_ORDER_REFUND_INFO)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        ProductStats productStats = new ProductStats();
                        productStats.setSku_id(jsonObject.getLong("sku_id"));
                        productStats.setTs(CommonUtil.getTs(jsonObject.getString("create_time")));
                        productStats.setRefund_amount(jsonObject.getBigDecimal("refund_amount"));
                        productStats.getRefundOrderIdSet().add(jsonObject.getLong("order_id"));
                        out.collect(productStats);
                    }
                });
        
        //8.评论数
        SingleOutputStreamOperator<ProductStats> commentStats = sourceStream
                .get(TOPIC_DWD_COMMENT_INFO)
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        ProductStats productStats = new ProductStats();
                        productStats.setSku_id(jsonObject.getLong("sku_id"));
                        productStats.setTs(CommonUtil.getTs(jsonObject.getString("create_time")));
                        productStats.setComment_ct(1L);
                        //判断是否为好评(5星和4星)
                        if (FIVE_START_GOOD_COMMENT.equals(jsonObject.getString("appraise"))
                                || FOUR_START_GOOD_COMMENT.equals(jsonObject.getString("appraise"))) {
                            productStats.setGood_comment_ct(1L);
                        }
                        out.collect(productStats);
                    }
                });
        
        return clickStats.union(cartStats, commentStats, paymentStats, favorStats, displayStats, orderStats, refundStats);
    }
}
