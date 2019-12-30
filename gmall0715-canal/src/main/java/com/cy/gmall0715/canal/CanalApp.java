package com.cy.gmall0715.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author cy
 * @create 2019-12-30 14:17
 */
public class CanalApp {
    public static void main(String[] args) {
        //todo 1 连接服务器端,一个客户端只能连接1个destination(即example)
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "", "");

        //todo 2 利用连接器抓取数据
        while(true){
            canalConnector.connect();
            //抓取gmall0715数据库里的所有表
            canalConnector.subscribe("gmall0715.*");
            //抓100条sql执行的数据结果.
            //一个message=一次抓取    一个message包含多个entry,每条sql执行的结果放在一个entry里，每个entry包含多条行记录数据,每行又是由多个列(字段)组成
            // binlog_format=row   row 以sql为单位进行存储相应的数据
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if(entries.size()==0){
                System.out.println("没有数据，休息5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                //todo 3 抓取数据后，提取数据
                //一个entry 代表一个sql执行的结果集,
                for (CanalEntry.Entry entry : entries) {
                    //业务数据 StoreValue
                    //entry中还会包括其他写操作，比如事务开启/关闭语句等,只需要ROWDATA
                    if(entry.getEntryType()== CanalEntry.EntryType.ROWDATA){
                        //取出序列化后的值集合
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange=null;
                        try {
                            //反序列化
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        if(rowChange!=null){
                            //得到行数据List
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            //取出表名
                            String tableName = entry.getHeader().getTableName();
                            //操作类型，insert update delete...
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                            canalHandler.handle();
                        }
                    }else{
                        continue;
                    }


                    //业务数据

                }
            }
        }
    }
}
