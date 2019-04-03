package com.alibaba.otter.canal.test;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class HandleData {

    private static Logger log = LoggerFactory.getLogger(HandleData.class);

    public static void handleEntry(List<Entry> entries){

        for(Entry entry : entries){
            // 获取日志行
            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 获取执行事件类型
            EventType eventType = rowChage.getEventType();

            // 日志打印，数据明细
            log.info(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s", entry
                    .getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(), entry.getHeader()
                    .getSchemaName(), entry.getHeader().getTableName(), eventType));

            // 获取表名
            String tableName = entry.getHeader().getTableName();


            // 遍历日志行，执行任务
            for (RowData rowData : rowChage.getRowDatasList()) {
                Map<String, Object> data;

                // 删除操作
                if (eventType == EventType.DELETE) {

                    // 解析数据
                    data = DataUtils.parseData(tableName, "delete", rowData.getBeforeColumnsList());

                    // 插入操作
                } else if (eventType == EventType.INSERT) {

                    // 解析数据
                    data = DataUtils.parseData(tableName, "insert", rowData.getAfterColumnsList());

                    // 更新操作
                } else {

                    // 解析数据
                    data = DataUtils.parseData(tableName, "update", rowData.getAfterColumnsList());
                }

                // 数据解析成功
                if (data != null && data.size() > 0) {


                    // 内容转接json格式发送
                    String json = JSONObject.toJSONString(data);
                    log.info("解析的数据",json);
                    try {
                        //将数据推送给RocketMq
                        log.info("将数据发送给RocketMQ start...");
                        log.info("topic : canal_"+tableName+"_topic");
                        log.info("data : "+ json.toString());
                        //Productor.send("canal_" + tableName = "_topic", json.toString(), tableName + "|" + data.get("canal_kafka_key"));
                    } catch (Exception e) {
                        System.out.println("rocketmq发送异常：" + e.getMessage());
                        e.printStackTrace();
                    }

                    log.info("数据成功发送RocketMQ");
                }
            }
        }



    }
}
