package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    //不需要每次调用都建立一次链接
    private static JedisPool pool;
    
    public static Jedis getRedis() {
    
        if (pool == null) {
            synchronized (RedisUtil.class) {
                if (pool == null) {
                    JedisPoolConfig conf = new JedisPoolConfig();
                    conf.setMaxTotal(1000);
                    conf.setMaxIdle(100);
                    conf.setMinIdle(10);
                    conf.setMaxWaitMillis(1000 * 60); // 从连接池获取对象的时候, 最多等待的时间
                    conf.setTestOnBorrow(true);
                    conf.setTestOnReturn(true);
                    conf.setTestOnCreate(true);
                    pool = new JedisPool(conf, "hadoop162", 6379);
                }
            }
        }
        return pool.getResource();
    }
}
