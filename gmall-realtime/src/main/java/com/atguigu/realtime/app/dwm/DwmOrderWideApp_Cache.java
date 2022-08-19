package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderDetail;
import com.atguigu.realtime.bean.OrderInfo;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.util.DimUtil;
import com.atguigu.realtime.util.JDBCUtil;
import com.atguigu.realtime.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;

import static com.atguigu.realtime.common.Constant.*;

/*
    为了防止每次读取数据都需要从Phoenix查询,可以将数据缓存
 */
public class DwmOrderWideApp_Cache extends BaseAppV2 {
    
    public static void main(String[] args) {
        new DwmOrderWideApp_Cache().init(3004,1,"DwmOrderWideApp","DwmOrderWideApp",
                TOPIC_DWD_ORDER_INFO, TOPIC_DWD_ORDER_DETAIL);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env, HashMap<String, DataStreamSource<String>> sourceStream) {
        //1.将两张事实表先join
        SingleOutputStreamOperator<OrderWide> factJoinedStream = factTableJoin(sourceStream);
        
        //2.和维度表进行join
        dimTableJoin(factJoinedStream);
    }
    
    private void dimTableJoin(SingleOutputStreamOperator<OrderWide> factJoinedStream) {
        factJoinedStream
                .map(new RichMapFunction<OrderWide, OrderWide>() {
                    
                    Connection connection;
                    Jedis jedis;
                    
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = JDBCUtil.getConnection(PHOENIX_URL, PHOENIX_DRIVER);
                        jedis = RedisUtil.getRedis();
                    }
    
                    @Override
                    public OrderWide map(OrderWide orderWide) throws Exception {
                        
                        //1.user_info
                        JSONObject userInfo = DimUtil.readDim(connection, jedis, DIM_USER_INFO, orderWide.getUser_id());
                        orderWide.setUser_gender(userInfo.getString("GENDER"));
                        orderWide.calcUserAge(userInfo.getString("BIRTHDAY"));
                        
                        //2.province
                        JSONObject provinceInfo = DimUtil.readDim(connection, jedis, DIM_BASE_PROVINCE, orderWide.getProvince_id());
                        orderWide.setProvince_name(provinceInfo.getString("NAME"));
                        orderWide.setProvince_area_code(provinceInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(provinceInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(provinceInfo.getString("ISO_3166_2"));
                        
                        //3.sku_info
                        JSONObject skuInfo = DimUtil.readDim(connection, jedis, DIM_SKU_INFO, orderWide.getSku_id());
                        orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                        orderWide.setOrder_price(skuInfo.getBigDecimal("PRICE"));
                        orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                        orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
    
                        // 4. 读取tm信息
                        JSONObject tmInfo = DimUtil.readDim(connection, jedis, DIM_BASE_TRADEMARK, orderWide.getTm_id());
                        orderWide.setTm_name(tmInfo.getString("TM_NAME"));
    
                        // 5. 读取spu信息
                        JSONObject spuInfo = DimUtil.readDim(connection, jedis, DIM_SPU_INFO, orderWide.getSpu_id());
                        orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));
    
                        // 6. 读取c3信息
                        JSONObject c3Info = DimUtil.readDim(connection, jedis, DIM_BASE_CATEGORY3, orderWide.getCategory3_id());
                        orderWide.setCategory3_name(c3Info.getString("NAME"));
                        
                        return orderWide;
                    }
    
                         @Override
                         public void close() throws Exception {
                             if (connection != null) {
                                 connection.close();
                             }
                             if (jedis != null) {
                                 jedis.close();
                             }
                         }
                     }
                
                );
    }
    
    private SingleOutputStreamOperator<OrderWide> factTableJoin(HashMap<String, DataStreamSource<String>> sourceStream) {
        KeyedStream<OrderInfo, Long> orderInfoLongKeyedStream = sourceStream
                .get(TOPIC_DWD_ORDER_INFO)
                .map(str -> JSON.parseObject(str, OrderInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((info, ts) -> info.getCreate_ts()))
                .keyBy(OrderInfo::getId);
    
    
        KeyedStream<OrderDetail, Long> orderDetailLongKeyedStream = sourceStream
                .get(TOPIC_DWD_ORDER_DETAIL)
                .map(str -> JSON.parseObject(str, OrderDetail.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((info, ts) -> info.getCreate_ts()))
                .keyBy(OrderDetail::getOrder_id);
        
        return orderInfoLongKeyedStream
                .intervalJoin(orderDetailLongKeyedStream)
                .between(Time.minutes(-5),Time.minutes(5)) //按照时间来join,一条记录可以匹配另一张表在一段时间内的记录
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left,
                                               OrderDetail right,
                                               Context ctx,
                                               Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left,right));
                    }
                });
                
    }
}
