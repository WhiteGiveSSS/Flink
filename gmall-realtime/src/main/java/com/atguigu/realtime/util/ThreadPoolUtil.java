package com.atguigu.realtime.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
    ThreadPoolExecutor
        参数: 1.最多可并行运行线程数 2.最多可接受线程数 3.空闲线程保留时间 4.时间单位 5.阻塞队列->超出数量的线程所在的队列
 */
public class ThreadPoolUtil {
    
    public static ThreadPoolExecutor getThreadPool() {
        return new ThreadPoolExecutor(100,300,300, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));
    }
}
