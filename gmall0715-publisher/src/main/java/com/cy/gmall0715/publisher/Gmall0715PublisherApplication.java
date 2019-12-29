package com.cy.gmall0715.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.cy.gmall0715.publisher.mapper") //扫描指定包下的接口，用于注入,mybatis
public class Gmall0715PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0715PublisherApplication.class, args);
    }

}
