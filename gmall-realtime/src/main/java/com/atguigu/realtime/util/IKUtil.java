package com.atguigu.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;

public class IKUtil {
    public static void main(String[] args) {
        System.out.println(analyzer("我是中国人"));
    }
    public static Collection<String> analyzer(String text){
        Collection<String> res = new HashSet<>();
        
        try {
            IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(text), true);
            Lexeme next = ikSegmenter.next();
            while (next != null){
                String word = next.getLexemeText();
                res.add(word);
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return res;
    }
}
