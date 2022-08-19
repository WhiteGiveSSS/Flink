package com.atguigu.realtime.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/25 10:26
 */
public class CommonUtil {
    
    public static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<T>();
        
        it.forEach(list::add);
        
        return list;
    }
    
    public static Long getTs(String create_time) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(create_time).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
