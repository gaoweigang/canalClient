package com.alibaba.otter.canal.test;

import com.alibaba.otter.canal.protocol.CanalEntry.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataUtils {

    private static Logger log = LoggerFactory.getLogger(HandleData.class);

    public static Map<String, Object> parseData(String tableName, String type, List<Column> columns){

        Map<String, Object> result = new HashMap<String, Object>();
        try {
            int index = 0;
            for (Column column : columns) {
                String value = column.getIsNull() ? null : column.getValue();

                // kafka在消息为10K时吞吐量达到最大
                if (value != null && value.length() > 10240) {
                    value = value.substring(0, 10240);
                }
                if (index == 0) {
                    result.put("canal_kafka_key", value);
                }
                result.put(column.getName(), value);
                index++;
            }
            result.put("operate_type", type);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;

    }
}
