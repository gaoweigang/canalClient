package com.alibaba.otter.canal.test;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;

public class CanalTest {

    public static void main(String[] args) {
        //创建连接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1", 11111), "articlesubscibe", null, null);
        //连接Canal Server,获取数据
        connector.connect();//连接
        connector.subscribe();//订阅
        connector.rollback();
        System.out.println("数据同步工程启动成功，开始获取数据");
        while(true){
            //获取指定数量的数据
            Message message = connector.getWithoutAck(1000);
            //数据批号
            long batchId = message.getId();
            //获取该批次数据的数量
            int size = message.getEntries().size();

            if(batchId == -1 || size ==0){//没获取到数据
                //等待1秒后重新获取
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //确认提交
                connector.ack(batchId);
            }else{
                // 处理数据
                HandleData.handleEntry(message.getEntries());
                // 提交确认
                connector.ack(batchId);
            }

        }
    }
}
