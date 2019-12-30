package com.cy.gmall0715.publisher.service;

import java.util.Map;

/**
 * @author cy
 * @create 2019-12-28 15:58
 */
public interface PublisherService {

    public Long getDauCount(String date);

    public Map getDauHourCount(String date);
}
