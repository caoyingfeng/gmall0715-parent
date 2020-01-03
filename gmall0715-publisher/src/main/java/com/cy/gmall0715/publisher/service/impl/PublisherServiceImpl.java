package com.cy.gmall0715.publisher.service.impl;

import com.cy.gmall0715.publisher.mapper.DauMapper;
import com.cy.gmall0715.publisher.mapper.OrderMapper;
import com.cy.gmall0715.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author cy
 * @create 2019-12-29 13:08
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauCount(String date) {
        return dauMapper.selectDauCount(date);
    }

    @Override
    public Map getDauHourCount(String date) {
        HashMap dauHourMap = new HashMap();
        List<Map> dauHourList = dauMapper.selectDauCountHour(date);
        //结构转换，将hour和ct两个字段的值拼接在一起
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("hour"),map.get("ct"));
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderHourAmount(String date) {
        HashMap orderHourMap = new HashMap<>();
        List<Map> orderHourList = orderMapper.selectOrderAmountHour(date);

        for (Map map : orderHourList) {
            orderHourMap.put(map.get("CREATE_HOUR"),map.get("ORDER_AMOUNT"));
        }
        return orderHourMap;
    }
}
