package com.cy.gmall0715.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author cy
 * @create 2019-12-31 8:27
 */
public interface OrderMapper {
    public Double selectOrderAmount(String date);

    public List<Map> selectOrderAmountHour(String date);
}
