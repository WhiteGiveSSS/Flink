package com.atguigu.realtime.app.function;

import com.atguigu.realtime.util.IKUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Collection;

@FunctionHint(output = @DataTypeHint("row<word string>"))
public class KeyWordUDTF extends TableFunction<Row> {
    public void eval(String keyWord){
        Collection<String> keyWords = IKUtil.analyzer(keyWord);
        for (String word : keyWords) {
            collect(Row.of(word));
        }
    }
}
