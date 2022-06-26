package com.junyao.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.junyao.constants.GmallConstants;
import com.junyao.utils.MyKafkaSend;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author wjy
 * @create 2022-06-26 15:36
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        //监控binlog变化
        while (true){
            //获取链接
            canalConnector.connect();
            //选择订阅的数据库
            canalConnector.subscribe("gmall2022.*");
            //获取多个sql执行结果
            Message message = canalConnector.get(100);
            //获取一个sql执行的结果
            List<CanalEntry.Entry> entries = message.getEntries();
            //通过判断list集合中是否有数据，进而判断mysql中是否有数据变化
            if (entries.size()<=0){
                //没有数据变化
                System.out.println("没有数据，休息一会...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                //有数据变化，获取每一个entry
                for (CanalEntry.Entry entry : entries) {
                    //获取表名
                    String tableName = entry.getHeader().getTableName();
                    //获取EntryType
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //根据entry类型判断，获取到序列化数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //取出序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //对数据进行反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //取封装每一行数据的list集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //对数据进行处理
                        handle(tableName,eventType,rowDatasList);
                    }

                }

            }
        }
    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //通过表名和类型来判断获取那张表中哪种类型的数据
        if ("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            //获取每一行的数据
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                JSONObject jsonObject = new JSONObject();

                //获取每一行中每一列的苏剧
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                //在控制台打印测试
                System.out.println(jsonObject.toJSONString());

                //将数据发送到Kafka
                MyKafkaSend.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }


        }

    }
}
