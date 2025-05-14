package cn.reiz.field.syncer.sync.config;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 *  @author LuYi
 */
@Data
public class SyncProperties {
    List<SyncDbConfig> syncDbConfigs;


    @Data
    public static class SyncDbConfig {
        public String syncId;
        public String mqTopic;
        public String srcDbName;
        public String targetDbJdbcUrl;
        public String targetDbUsername;
        public String targetDbPassword;
        public List<SyncTableConfig> tableConfigs;
    }

    @Data
    public static class SyncTableConfig {
        public String srcTableKey;
        public String srcTableName;
        public String targetTableName;
        public String targetTableMappingKey;
        public Map<String, String> columnMapping;
    }

}
