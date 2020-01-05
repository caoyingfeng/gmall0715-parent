package com.cy.gmall0715.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.cy.gmall0715.canal.util.KafkaSender;
import com.cy.gmall0715.common.constant.GmallConstant;

import java.util.List;

/**
 * @author cy
 * @create 2019-12-30 14:42
 */
public class CanalHandler {
    CanalEntry.EventType eventType;
    String tableName;
    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;

    }

    public void handle() {
        //订单表 根据表名和操作类型不同发送到kafka
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT == eventType && rowDataList.size()>0) {
            sendToKafka(GmallConstant.KAFKA_TOPIC_ORDER);
        }else if("order_detail".equals(tableName) && CanalEntry.EventType.INSERT == eventType && rowDataList.size()>0){   //用户 商品 订单明细
            sendToKafka(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);
        }else if("user_info".equals(tableName) && (CanalEntry.EventType.INSERT == eventType || CanalEntry.EventType.UPDATE == eventType) && rowDataList.size()>0){   //用户 商品 订单明细
            sendToKafka(GmallConstant.KAFKA_TOPIC_USER);
        }
   }

    public void sendToKafka(String topic){
        //得到每一行
        for (CanalEntry.RowData rowData : rowDataList) {
            //取出执行sql语句之后的数据
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            //得到每一行的每一列
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "--->" + column.getValue());
                jsonObject.put(column.getName(),column.getValue());
            }
            //每行发一次到kafka
            KafkaSender.send(topic,jsonObject.toJSONString());
        }
    }
}
