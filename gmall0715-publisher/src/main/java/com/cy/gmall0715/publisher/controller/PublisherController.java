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

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String realtimeHourDate(@RequestParam("id") String id,@RequestParam("date") String date){

        if("dau".equals(id)){
            Map dauHoursToday = publisherService.getDauHourCount(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today",dauHoursToday);

            String yesterdayDateString = "";
            try {
                Date todayDate = new SimpleDateFormat("yyyy-MM-dd").parse(date);
                Date yesterdayDate = DateUtils.addDays(todayDate, -1);
                yesterdayDateString = new SimpleDateFormat("yyyy-MM-dd").format(yesterdayDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            Map dauHourYesterday = publisherService.getDauHourCount(yesterdayDateString);
            jsonObject.put("yesterday",dauHourYesterday);
            return jsonObject.toJSONString();
        }else{
            return null;
        }
    }
}
