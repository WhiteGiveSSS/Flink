package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.common.Constant;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {
    
    public static void main(String[] args) throws Exception {
        Connection connection = getConnection(Constant.MYSQL_URL,Constant.MYSQL_DRIVER);
        List<JSONObject> l = queryList(connection, "select * from user_info where id='1'", null, JSONObject.class);
        for (JSONObject jsonObject : l) {
            System.out.println(jsonObject);
        }
    }
    
    public static Connection getConnection(String url, String driver) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        return DriverManager.getConnection(url);
    }
    
    //执行给定的SQL,并将结果返回到list
    public static <T> List<T> queryList(Connection conn,
                                        String sql,
                                        Object[] args,
                                        Class<T> tClass) throws Exception {
        PreparedStatement ps = conn.prepareStatement(sql);
        // 1. 先把sql中的占位符进行赋值  args 的长度是几就表示sql中有几个占位符
        for (int i = 0; args != null && i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
    
        ArrayList<T> result = new ArrayList<>();
        // 2. 执行sql语句
        ResultSet resultSet = ps.executeQuery();
        // 通过 resultSet 获取相关的元数据类得到
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            // 3. 遍历到每行数据, 把这些数据封装到 T 类型的对象中
            T t = tClass.newInstance();  // 3.1 利用反射的方式, 创建 t类型的对象
            // 知道属性名和属性只  setAge(10)
            // 遍历每一列
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnLabel(i + 1);// 列的索引是从1开始
                Object value = resultSet.getObject(columnName);
                BeanUtils.setProperty(t, columnName, value);
            }
            result.add(t);
        }
    
        return result;
    }
}
