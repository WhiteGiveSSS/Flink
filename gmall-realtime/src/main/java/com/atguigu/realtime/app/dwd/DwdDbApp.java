package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class DwdDbApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DwdDbApp().init(2002,1,"DwdDbApp","DwdDbApp", Constant.TOPIC_ODS_DB);
    }
    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        //1.通过CDC读取从MySQL配置数据
        SingleOutputStreamOperator<TableProcess> tpStream = readCDCData(env);
        //2.消费kafka数据,进行清洗 -> ETL
        SingleOutputStreamOperator<JSONObject> etlData = ETLStream(source);
        //3.connect两个流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connectStream(tpStream, etlData);
        //4.动态分流
        Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> distribute = dynamicDistribute(connectedStream);
        //5.写入到kafka
        sendToKafka(distribute.f0);
        //6.写入到HBASE
        sendToHBase(distribute.f1);
    }
    
    private void sendToHBase(DataStream<Tuple2<JSONObject, TableProcess>> data) {
        //使用Phoenix写入数据
        data
                .keyBy(t -> t.f1.getSinkTable())  // 按照sink_table进行keyBy提高写入的效率
                .addSink(FlinkSinkUtil.getHBaseSink());
        //1.在Phoenix中建表,只在第一条数据输入时建表(状态)

        //2.向Phoenix写入数据(使用SQL动态写入,根据不同的表拼出不同的SQL)

    }
    
    private void sendToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> data) {
        data.addSink(FlinkSinkUtil.getKafkaSink());
    }
    
    private Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> dynamicDistribute(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream) {
        
        OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>("Hbase") {};
        //输入和输出一致,只是分流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream = connectedStream
                .process(new ProcessFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(Tuple2<JSONObject, TableProcess> value,
                                               Context ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //数据流只需要data
                        JSONObject data = value.f0.getJSONObject("data");
                        //配置流
                        TableProcess tableProcess = value.f1;
                        //过滤数据流中在配置流中没有的字段 -> 提升效率
                        columnFilter(data, tableProcess);
                        String sinkType = tableProcess.getSinkType();
                        if (sinkType.equals(TableProcess.SINK_TYPE_KAFKA)) {
                            //kafka -> 主流
                            out.collect(Tuple2.of(data, tableProcess));
                        } else if (sinkType.equals(TableProcess.SINK_TYPE_HBASE)) {
                            ctx.output(hbaseTag, Tuple2.of(data, tableProcess));
                        }
                    }
                
                    private void columnFilter(JSONObject data, TableProcess tableProcess) {
                        List<String> columnList = Arrays.asList(tableProcess.getSinkColumns().split(","));
                        Set<String> keySet = data.keySet();
                        keySet.removeIf(key -> !columnList.contains(key));
                    }
                });
        DataStream<Tuple2<JSONObject, TableProcess>> hbaseStream = kafkaStream.getSideOutput(hbaseTag);
        return Tuple2.of(kafkaStream,hbaseStream);
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream(SingleOutputStreamOperator<TableProcess> tpStream, SingleOutputStreamOperator<JSONObject> etlData) {
        MapStateDescriptor<String, TableProcess> tpState = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        //1.把配置表流设置为广播流,和广播状态关联
        //广播状态本质就是一个map -> key = (表名 + 操作类型)
        BroadcastStream<TableProcess> tpStateStream = tpStream.broadcast(tpState);
        //2.connect数据流和配置流(main -> data)
        return etlData
                .connect(tpStateStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject,TableProcess>>() {
                    //单独处理数据流
                    @Override
                    public void processElement(JSONObject value,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(tpState);
    
                        String key = value.getString("table") + ":" + value.getString("type").replaceAll("bootstrap-", "");
                        TableProcess tableProcess = broadcastState.get(key);
                        //如果有相关配置就collect
                        if (tableProcess != null) {
                            out.collect(Tuple2.of(value,tableProcess));
                        }
                    }
    
                    //单独处理广播流
                    @Override
                    public void processBroadcastElement(TableProcess value,
                                                        Context ctx,
                                                        Collector<Tuple2<JSONObject,
                                                                TableProcess>> out) throws Exception {
                        //更新广播状态
                        //1.获取广播状态
                        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(tpState);
                        //2.写入广播状态
                        broadcastState.put((value.getSourceTable() + ":" + value.getOperateType()),value);
                    }
                });
    }
    
    private SingleOutputStreamOperator<JSONObject> ETLStream(DataStreamSource<String> source) {
        return source
                .map(JSON::parseObject)
                .filter(obj ->
                        obj.getString("database") != null
                                && obj.getString("table") != null
                                && obj.getString("type") != null
                                && (obj.getString("type").contains("insert") || obj.get("type").equals("update"))
                                && obj.getString("data") != null
                                && obj.getString("data").length() > 2
                );
    }
    
    private SingleOutputStreamOperator<TableProcess> readCDCData(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        //创建一个表和MySQL配置表进行关联
        tEnv.executeSql("CREATE TABLE `table_process` (\n" +
                "  `source_table` string,\n" +
                "  `operate_type` string ,\n" +
                "  `sink_type` string ,\n" +
                "  `sink_table` string ,\n" +
                "  `sink_columns` string ,\n" +
                "  `sink_pk` string ,\n" +
                "  `sink_extend` string ,\n" +
                "  PRIMARY KEY (`source_table`,`operate_type`) not enforced" +
                ")with(" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'hadoop162',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'aaaaaa',\n" +
                " 'database-name' = 'gmall2021_realtime',\n" +
                " 'table-name' = 'table_.*', " +
                " 'debezium.snapshot.mode'='initial' " +
                ")");
        
        Table tpTable = tEnv
                .sqlQuery("select " +
                        " source_table sourceTable, " +
                        " sink_type sinkType, " +
                        " operate_type operateType, " +
                        " sink_table sinkTable, " +
                        " sink_columns sinkColumns," +
                        " sink_pk sinkPk, " +
                        " sink_extend sinkExtend " +
                        "from table_process");
        
        return tEnv
                .toRetractStream(tpTable, TableProcess.class)
                //返回值的第一条是true,这个表示撤回是否成功,以后不需要它
                .filter(t -> t.f0)
                .map(t -> t.f1);
                
                
    }
}
