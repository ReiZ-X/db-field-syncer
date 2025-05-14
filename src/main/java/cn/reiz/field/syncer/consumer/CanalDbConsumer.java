package cn.reiz.field.syncer.consumer;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.reiz.field.syncer.sync.config.SyncProperties;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.apache.rocketmq.spring.autoconfigure.RocketMQProperties;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *  @author LuYi
 */
public class CanalDbConsumer {

    private RocketMQProperties rocketMQProperties;
    private SyncProperties.SyncDbConfig syncDbConfig;
    //
    private RocketMQCanalConnector connector;

    private volatile boolean running = false;


    public CanalDbConsumer(RocketMQProperties rocketMQProperties, SyncProperties.SyncDbConfig syncDbConfig) {
        this.rocketMQProperties = rocketMQProperties;
        this.syncDbConfig = syncDbConfig;
    }

    public void start() {
        if (!running) {
            connector = new RocketMQCanalConnector(
                    rocketMQProperties.getNameServer(),
                    syncDbConfig.getMqTopic(),
                    syncDbConfig.getSyncId(),
                    500,
                    true);
            connector.connect();
            connector.subscribe();
            running = true;
        }
        new Thread(() -> {
            while (running) {
                List<FlatMessage> messages = connector.getFlatList(1000L, TimeUnit.MILLISECONDS);
                for (FlatMessage message : messages) {
                    long batchId = message.getId();
                    if (batchId == -1) {
                        ThreadUtil.sleep(1000);
                    } else {
                        SpringUtil.getBean(MessageProcessor.class).process(message, syncDbConfig);
                    }
                }
                // 提交确认
                connector.ack();
            }
            connector.unsubscribe();
            connector.disconnect();
        }).start();
    }

    public void stop() {
        running = false;
    }


}
