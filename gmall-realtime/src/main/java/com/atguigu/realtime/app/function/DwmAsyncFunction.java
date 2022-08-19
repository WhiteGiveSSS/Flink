package com.atguigu.realtime.app.function;

import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.DimUtil;
import com.atguigu.realtime.util.JDBCUtil;
import com.atguigu.realtime.util.RedisUtil;
import com.atguigu.realtime.util.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadPoolExecutor;

import static com.atguigu.realtime.common.Constant.*;


public abstract class DwmAsyncFunction<T> extends RichAsyncFunction<T,T> {
    
    ThreadPoolExecutor pool;
    Connection jdbc;
    
    //定义一个抽象方法交给具体实现者去实现
    public abstract void asyncDimJoin(Connection jdbc, Jedis jedis, T input, ResultFuture<T> resultFuture) throws Exception;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        jdbc = JDBCUtil.getConnection(PHOENIX_URL, PHOENIX_DRIVER);
        pool = ThreadPoolUtil.getThreadPool();
    }
    
    //每来一条数据,调用一次方法 参数: 1.输出 2.收集输出,直接交给下一阶段
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        
        pool.execute(new Runnable() {
            @SneakyThrows
            @Override
            //dim查询
            //一个异步请求需要一个redis客户端,否则容易串流(Phoenix只需要一个即可)
            public void run() {
                Jedis jedis = RedisUtil.getRedis();
                asyncDimJoin(jdbc,jedis,input,resultFuture);
                jedis.close();
            }
        });
    }
    
    @Override
    public void close() throws Exception {
        if (jdbc != null) {
            jdbc.close();
        }
        
        if (pool != null) {
            pool.shutdown();
        }
    }
}
