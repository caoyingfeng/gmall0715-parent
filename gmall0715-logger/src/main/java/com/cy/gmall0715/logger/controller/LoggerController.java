package com.cy.gmall0715.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cy.gmall0715.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author cy
 * @create 2019-12-13 14:19
 */
@RestController //Controller+responsebody
@Slf4j //编译时生成log对象，Lombok通知idea之后会生成log，所以不会报错
public class LoggerController {

    @Autowired //Autowired通过容器将对象new出来后注入
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    //@ResponseBody //返回的是字符串，不是页面
    public String log(@RequestParam("logString") String logString){
        //1.加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        //2.落盘成文件
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);

        //3.发送到kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "sucess";
    }
}
