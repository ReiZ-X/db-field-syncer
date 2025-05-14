package cn.reiz.field.syncer.consumer;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.reiz.field.syncer.repository.DataUpdater;
import cn.reiz.field.syncer.sync.config.SyncProperties;
import com.alibaba.otter.canal.protocol.FlatMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  @author LuYi
 */
@Slf4j
@Component
public class MessageProcessor {

    @Resource
    private DataUpdater dataUpdater;

    public void process(FlatMessage message, SyncProperties.SyncDbConfig syncDbConfig) {
        Map<String, SyncProperties.SyncTableConfig> tableConfigMap = syncDbConfig.getTableConfigs().stream()
                .collect(Collectors.toMap(SyncProperties.SyncTableConfig::getSrcTableName, c -> c));
        //
        String dbName = message.getDatabase();
        String tableName = message.getTable();
        //
        String type = message.getType();
        SyncProperties.SyncTableConfig syncTableConfig = tableConfigMap.get(tableName);
        String targetTableName = syncTableConfig.getTargetTableName();
        Map<String, String> columnMapping = syncTableConfig.getColumnMapping();
        try {
            List<Map<String, String>> data = message.getData();
            switch (type) {
                case "INSERT": {
                    for (Map<String, String> row : data) {
                        Map<String, String> fieldValueMap = toInsertRow(syncTableConfig, columnMapping, row);
                        if (!fieldValueMap.isEmpty()) {
                            dataUpdater.insert(syncDbConfig, targetTableName, fieldValueMap);
                        }
                    }
                    break;
                }
                case "UPDATE": {
                    List<Map<String, String>> oldData = message.getOld();
                    for (int i = 0; i < oldData.size(); i++) {
                        Map<String, String> oldRow = oldData.get(i);
                        Map<String, String> nowRow = data.get(i);
                        String keyFieldName = syncTableConfig.getTargetTableMappingKey();
                        String keyFieldValue = nowRow.get(syncTableConfig.getSrcTableKey());
                        //如果目标表不存在对应的记录进行更新，那么就插入
                        Map<String, String> insertRow = toInsertRow(syncTableConfig, columnMapping, nowRow);
                        //
                        Map<String, String> fieldValueMap = new HashMap<>(columnMapping.size());
                        for (Map.Entry<String, String> mapping : columnMapping.entrySet()) {
                            String columnName = mapping.getKey();
                            String targetColumnName = mapping.getValue();
                            if (oldRow.containsKey(columnName)) {
                                String oldValue = oldRow.get(columnName);
                                String newValue = nowRow.get(columnName);
                                if (ObjectUtil.notEqual(oldValue, newValue)) {
                                    fieldValueMap.put(targetColumnName, newValue);
                                }
                            }
                        }
                        if (!fieldValueMap.isEmpty()) {
                            dataUpdater.update(syncDbConfig, targetTableName, keyFieldName, keyFieldValue, fieldValueMap, insertRow);
                        }
                    }
                    break;
                }
                case "DELETE": {
                    //TODO
                    break;
                }
                default: {
                    log.error("不处理的事件类型，{}", type);
                    break;
                }
            }
        } catch (Exception e) {
            log.error("处理canal entry 失败, dbName:{}, tableName:{}", dbName, tableName, e);
        }
    }


    private Map<String, String> toInsertRow(SyncProperties.SyncTableConfig syncTableConfig,
                                            Map<String, String> columnMapping,
                                            Map<String, String> row) {
        Map<String, String> fieldValueMap = new HashMap<>(columnMapping.size());
        for (Map.Entry<String, String> entry : row.entrySet()) {
            String columnName = entry.getKey();
            String columnValue = entry.getValue();
            String targetColumnName = columnMapping.get(columnName);
            if (syncTableConfig.getSrcTableKey().equals(columnName)) {
                fieldValueMap.put(syncTableConfig.getTargetTableMappingKey(), columnValue);
                continue;
            }
            if (!columnMapping.containsKey(columnName)) {
                continue;
            }
            if (StrUtil.isNotEmpty(targetColumnName)) {
                fieldValueMap.put(targetColumnName, columnValue);
            }
        }
        return fieldValueMap;
    }
}