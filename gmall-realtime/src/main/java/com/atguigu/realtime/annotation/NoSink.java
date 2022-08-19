package com.atguigu.realtime.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//只作用在属性上
@Target(ElementType.FIELD)
//在运行时用(反射)
@Retention(RetentionPolicy.RUNTIME)
public @interface NoSink {
}
