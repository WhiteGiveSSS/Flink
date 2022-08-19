package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
//@ResponseBody
@RestController //等价于Controller + ResponseBody
@Slf4j
public class logger {
    @RequestMapping("/applog")
    public String doLog(@RequestParam("param") String log){
        writeToFile(log);
        sendToKafka(log);
        return "ok";
    }
    
    @Autowired
    KafkaTemplate<String,String> kafka;
    private void sendToKafka(String log) {
        kafka.send("ods_log", log);
    }
    
    private void writeToFile(String strLog){
        log.info(strLog);
    }
}
