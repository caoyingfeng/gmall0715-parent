package com.cy.gmall0715.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cy.gmall0715.publisher.service.PublisherService;

import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author cy
 * @create 2019-12-28 16:00
 */
@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){
        Long dauCount = publisherService.getDauCount(date);
        List<Map> totalList = new ArrayList<>();
        Map dauMap = new HashMap();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauCount);

        totalList.add(dauMap);

        Map newMidMap = new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",200);

        totalList.add(newMidMap);

        Map orderMap = new HashMap();
        Double orderAmount = publisherService.getOrderAmount(date);
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        orderMap.put("value",orderAmount);

        totalList.add(orderMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String realtimeHourDate(@RequestParam("id") String id,@RequestParam("date") String date){

        if("dau".equals(id)){
            Map dauHoursToday = publisherService.getDauHourCount(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today",dauHoursToday);

            String yesterdayDateString = getYesterday(date);

            Map dauHourYesterday = publisherService.getDauHourCount(yesterdayDateString);
            jsonObject.put("yesterday",dauHourYesterday);
            return jsonObject.toJSONString();
        }else if("order_amount".equals(id)){
            Map orderHourAmountTd = publisherService.getOrderHourAmount(date);

            String yesterday = getYesterday(date);
            Map orderHourAmountYD = publisherService.getOrderHourAmount(yesterday);

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today",orderHourAmountTd);
            jsonObject.put("yesterday",orderHourAmountYD);

            return jsonObject.toJSONString();
        }
        else{
            return null;
        }
    }

    private String getYesterday(String today){
        SimpleDateFormat formator = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = null;
        try {
            Date tdDate = formator.parse(today);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            yesterday = formator.format(ydDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterday;
    }
}
