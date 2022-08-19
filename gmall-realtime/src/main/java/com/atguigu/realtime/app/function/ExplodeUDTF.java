package com.atguigu.realtime.app.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("row<source string, ct bigint>"))
public class ExplodeUDTF extends TableFunction<Row> {
    public void eval(Long click, Long order, Long cart){
        if (click > 0) {
            collect(Row.of("click",click));
        }
        if (order > 0) {
            collect(Row.of("order",order));
        }
        if (cart > 0) {
            collect(Row.of("cart",cart));
        }
    }
}
