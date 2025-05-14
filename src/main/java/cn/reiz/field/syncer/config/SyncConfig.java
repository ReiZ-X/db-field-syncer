package cn.reiz.field.syncer.config;

import cn.reiz.field.syncer.sync.config.SyncProperties;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *  @author LuYi
 */
@Data
@Configuration
public class SyncConfig {

    @Bean
    @ConfigurationProperties(prefix = "sync.config")
    public SyncProperties syncProperties() {
        return new SyncProperties();
    }


}
