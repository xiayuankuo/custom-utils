
package org.example.connector;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.example.exception.InterruptSqlException;
import org.example.function.JdbcStatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author yuankuo.xia
 * @Date 2024/5/27
 */
public class DynamicsExecutor implements Serializable {

    Logger LOG = LoggerFactory.getLogger(DynamicsExecutor.class);

    private static final long serialVersionUID = 1L;

    private final String[] fields;

    private final String sql;

    private final int maxRetries;

    private final List<JSONObject> failedData;

    public final AtomicLong timeout;
    private final AtomicInteger batchCount;

    private final List<JSONObject> batches;

    public int incrementAndGet(){
        return batchCount.incrementAndGet();
    }

    public int getBatchCount(){
        return batchCount.get();
    }

    public String getSql() {
        return sql;
    }

    public List<JSONObject> getFailedData() {
        return failedData;
    }

    public DynamicsExecutor(String table, String[] fields, ClickHouseOptions options) throws SQLException{
        this.fields = fields;
        this.sql = formatSql(table, fields);
        this.maxRetries = options.getMaxRetries();
        this.batchCount = new AtomicInteger(0);
        this.timeout = new AtomicLong(0);
        this.batches = new ArrayList<>(65536);
        this.failedData = new ArrayList<>(65536);
    }

    private String formatSql(String table, String[] fields) throws SQLException{
        if (fields != null && fields.length > 0) {
            String sqlPrepare = "insert into %s(%s) values (%s)";
            String columns = String.join(",", Arrays.asList(fields));
            String prepare = Arrays.stream(fields).map(field -> "?").collect(Collectors.joining(","));
            return String.format(sqlPrepare, table, columns, prepare);
        }
        throw new SQLException("Column is empty!");
    }

    public void addToBatch(JSONObject record) {
        batches.add(record);
    }

    public void executeBatch(List<PreparedStatement> statements, ClickHousePartitioner partitioner) throws Exception{
        timeout.set(System.currentTimeMillis());
        JSONObject[] dataArr = null;
        if(batches.size() > 0 || failedData.size() > 0) {
            synchronized (this) {
                if(failedData.size() > 0){
                    batches.addAll(failedData);
                    failedData.clear();
                }
                if(batches.size() > 0) {
                    dataArr = new JSONObject[batches.size()];
                    System.arraycopy(batches.toArray(), 0, dataArr, 0, batches.size());
                    batches.clear();
                }
            }

            if (dataArr != null) {
                PreparedStatement statement = statements.get(partitioner.select(null, statements.size()));
                try {
                    for (JSONObject jsonObject : dataArr) {
                        for (int i = 0; i < fields.length; i++) {
                            JdbcStatementBuilder.statementSetting(
                                    statement,
                                    jsonObject.get(fields[i]),
                                    i);
                        }
                        statement.addBatch();
                    }
                    attemptExecuteBatch(statement, maxRetries);
                }catch (Exception e) {
                    try {
                        if (statement.getConnection().isValid(2 * 1000)) {
                            throw new InterruptSqlException(e);
                        }
                    }catch (SQLException ignored) {
                        LOG.error("连接测试失败");
                    }
                    statements.remove(statement);
                    if(statements.size() > 0) {
                        statement = statements.get(partitioner.select(null, statements.size()));
                        attemptExecuteBatch(statement, maxRetries);
                        //todo 仅仅进行2次重试，集群环境可以考虑进一步优化
                    }
                }
            }
        }
    }

    private void attemptExecuteBatch(PreparedStatement stmt, int maxRetries)
            throws SQLException {
        for (int i = 0; i <= maxRetries; i++) {
            try {
                stmt.executeBatch();
                return;
            } catch (Exception exception) {
                LOG.error("ClickHouse executeBatch error, retry times = {}", i, exception);
                if (i >= maxRetries) {
                    throw new SQLException(
                            String.format(
                                    "Attempt to execute batch failed, exhausted retry times = %d",
                                    maxRetries),
                            exception);
                }
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new SQLException(
                            "Unable to flush; interrupted while doing another attempt", ex);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "ClickHouseBatchExecutor{"
                + "sql='"
                + sql
                + '\''
                + '}';
    }
}
