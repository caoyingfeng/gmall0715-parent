package com.cy.gmall0715.publisher.service.impl;

import com.cy.gmall0715.publisher.mapper.DauMapper;
import com.cy.gmall0715.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author cy
 * @create 2019-12-29 13:08
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDauCount(String date) {
        return dauMapper.selectDauCount(date);
    }
}
