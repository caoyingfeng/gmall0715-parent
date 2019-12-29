package com.cy.gmall0715.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author cy
 * @create 2019-12-28 15:50
 */
public interface DauMapper {
    public Long selectDauCount(String date);

    public List<Map> selectDauCountHour(String date);
}
