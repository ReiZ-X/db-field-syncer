package cn.reiz.field.syncer.repository;

import cn.reiz.field.syncer.sync.config.SyncProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  @author LuYi
 */
@Component
public class DataSourceFactory {

    private ConcurrentHashMap<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();

    public synchronized DataSource getDataSource(SyncProperties.SyncDbConfig syncDbConfig) {
        if (!dataSourceMap.containsKey(syncDbConfig.getSyncId())) {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(syncDbConfig.getTargetDbJdbcUrl());
            hikariConfig.setUsername(syncDbConfig.getTargetDbUsername());
            hikariConfig.setPassword(syncDbConfig.getTargetDbPassword());
            hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
            hikariConfig.setMaximumPoolSize(10);
            hikariConfig.setMinimumIdle(1);
            dataSourceMap.put(syncDbConfig.getSyncId(), new HikariDataSource(hikariConfig));
        }
        return dataSourceMap.get(syncDbConfig.getSyncId());
    }


}
