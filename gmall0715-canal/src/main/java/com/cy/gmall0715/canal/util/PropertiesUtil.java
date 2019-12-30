package com.cy.gmall0715.canal.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author cy
 * @create 2019-12-30 18:45
 */
public class PropertiesUtil {
    public static Properties load(String propertieName) throws IOException {
        Properties properties = new Properties();
        properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(propertieName),"UTF-8"));
        return properties;
    }
}
