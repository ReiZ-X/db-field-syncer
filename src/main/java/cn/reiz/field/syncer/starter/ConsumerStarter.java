package cn.reiz.field.syncer.starter;

import cn.reiz.field.syncer.consumer.CanalDbConsumer;
import cn.reiz.field.syncer.sync.config.SyncProperties;
import org.apache.rocketmq.spring.autoconfigure.RocketMQProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 *  @author LuYi
 */
@Component
public class ConsumerStarter {
    @Resource
    private SyncProperties syncProperties;
    @Resource
    private RocketMQProperties rocketMQProperties;
    private List<CanalDbConsumer> canalDbConsumers = new ArrayList<>();

    @PostConstruct
    public void init() {
        List<SyncProperties.SyncDbConfig> syncDbConfigs = syncProperties.getSyncDbConfigs();
        for (SyncProperties.SyncDbConfig syncDbConfig : syncDbConfigs) {
            CanalDbConsumer canalDbConsumer = new CanalDbConsumer(rocketMQProperties, syncDbConfig);
            canalDbConsumers.add(canalDbConsumer);
            canalDbConsumer.start();
        }
    }

    @PreDestroy
    public void destroy() {
        for (CanalDbConsumer canalDbConsumer : canalDbConsumers) {
            canalDbConsumer.stop();
        }
    }
}


