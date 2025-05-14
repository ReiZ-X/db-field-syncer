package cn.reiz.field.syncer.repository;

import cn.reiz.field.syncer.sync.config.SyncProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  @author LuYi
 */
@Slf4j
@Component
public class DataUpdater {

    @Resource
    private DataSourceFactory dataSourceFactory;

    public void insert(SyncProperties.SyncDbConfig syncDbConfig, String tableName, Map<String, String> fieldValueMap) {
        DataSource dataSource = dataSourceFactory.getDataSource(syncDbConfig);
        String insertSql = null;
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            insertSql = generateInsertSql(tableName, fieldValueMap);
            statement.execute(insertSql);
        } catch (SQLException e) {
            log.error("Insert data error. tableName:{}, fieldValueMap:{}, insertSQL:{}", tableName, fieldValueMap, insertSql, e);
        }
    }

    public void update(SyncProperties.SyncDbConfig syncDbConfig, String tableName,
                       String keyFieldName, String keyFieldValue,
                       Map<String, String> fieldValueMap, Map<String, String> insertRow) {
        //分布式锁 TODO
        String lockKey = String.join(":", tableName, keyFieldName, keyFieldValue);

        //
        DataSource dataSource = dataSourceFactory.getDataSource(syncDbConfig);
        String sql = null;
        try (Connection connection = dataSource.getConnection()) {
            //判断是否存在，如果不存在，则插入
            sql = generateSelectSql(tableName, keyFieldName, keyFieldValue);
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            if (!resultSet.next()) {
                insert(syncDbConfig, tableName, insertRow);
                return;
            }
            //
            Statement statement = connection.createStatement();
            sql = generateUpdateSql(tableName, keyFieldName, keyFieldValue, fieldValueMap);
            statement.execute(sql);
        } catch (SQLException e) {
            log.error("Update data error. tableName:{}, keyFieldName:{}, keyFieldValue:{}, fieldValueMap:{}, updateSQL:{}", tableName, keyFieldName, keyFieldValue, fieldValueMap, sql, e);
        }
    }

    private String generateSelectSql(String tableName, String keyFieldName, String keyFieldValue) {
        String sql = "SELECT 1 FROM %s WHERE %s = '%s'";
        return String.format(sql, tableName, keyFieldName, keyFieldValue);
    }

    private String generateInsertSql(String tableName, Map<String, String> fieldValueMap) {
        String sql = "INSERT INTO %s(%s) VALUES(%s)";
        String fields = fieldValueMap.keySet().stream().map(f -> "`" + f + "`").collect(Collectors.joining(","));
        String values = fieldValueMap.values().stream().map(v -> "'" + v + "'").collect(Collectors.joining(","));
        return String.format(sql, tableName, fields, values);
    }

    private String generateUpdateSql(String tableName, String keyFieldName, String keyFieldValue, Map<String, String> fieldValueMap) {
        String sql = "UPDATE %s SET %s WHERE %s = '%s'";
        StringBuilder setFields = new StringBuilder();
        fieldValueMap.entrySet().stream().forEach(entry -> {
            setFields
                    .append("`" + entry.getKey() + "`")
                    .append("=")
                    .append(entry.getValue() == null ? "null" : "'" + entry.getValue() + "'")
                    .append(",");
        });
        if (setFields.toString().endsWith(",")) {
            setFields.deleteCharAt(setFields.length() - 1);
        }
        sql = String.format(sql, tableName, setFields, keyFieldName, keyFieldValue);
        return sql;
    }
}
