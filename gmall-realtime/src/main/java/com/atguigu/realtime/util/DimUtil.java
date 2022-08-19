package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

import static com.atguigu.realtime.common.Constant.*;

public class DimUtil {
    
    public static JSONObject readFromPhoenix(Connection connection, String table, Object id) throws Exception {
        String sql = "select * from " + table + " where id=?";
        
        //执行各种SQL,结果返回多行
        List<JSONObject> list = JDBCUtil.queryList(connection, sql, new Object[]{id.toString()}, JSONObject.class);
        
        assert list != null;
        if (list.size() != 0) {
            return list.get(0);
        }
        return new JSONObject();
    }
    
    public static JSONObject readDim(Connection connection, Jedis jedis, String table, Object id) throws Exception {
        //因为将id作为key的数据量很大,所以选取一个单独的redis库
        //在redis查询时记得先选库！！！
        jedis.select(1);
        //1.去cache中读取
        JSONObject dim = readDimFromRedis(jedis, table, id);
        //2.cache中没有去Phoenix中读取
        if (dim == null) {
            dim = readFromPhoenix(connection, table, id);
            //3.将读到到数据更新到redis
            writeDimToCache(jedis, table, id, dim);
        }
        return dim;
    }
    
    private static void writeDimToCache(Jedis jedis, String table, Object id, JSONObject dim) {
        String key = table + ":" + id;
        String value = dim.toJSONString();
        //set只有key和value,setex可以用设置过期时间
        jedis.setex(key, DIM_EXPIRE_SECOND, value);
    }
    
    private static JSONObject readDimFromRedis(Jedis jedis, String table, Object id) {
        JSONObject dim = null;
        
        String key = table + ":" + id;
        String value = jedis.get(key);
        if (value != null) {
            dim = JSON.parseObject(value);
            //每次从cache读取时都重置过期时间
            jedis.expire(key,DIM_EXPIRE_SECOND);
        }
        
        return dim;
    }
}
