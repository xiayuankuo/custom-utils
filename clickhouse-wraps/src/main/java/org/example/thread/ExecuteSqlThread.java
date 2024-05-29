package org.example.thread;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.clickhouse.internal.connection.ClickHouseConnectionProvider;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.example.connector.DynamicsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yuankuo.xia
 * @Date 2024/5/28
 */
public class ExecuteSqlThread implements Runnable{
    private final static Logger log = LoggerFactory.getLogger(ExecuteSqlThread.class);

    private final ClickHouseOptions options;

    private final DynamicsExecutor dynamicsExecutor;

    private final ClickHousePartitioner partitioner;

    private final List<Exception> errors;

    public ExecuteSqlThread(
            ClickHouseOptions options,
            DynamicsExecutor dynamicsExecutor,
            ClickHousePartitioner partitioner,
            List<Exception> error){
        this.options = options;
        this.dynamicsExecutor = dynamicsExecutor;
        this.partitioner = partitioner;
        this.errors = error;
    }
    @Override
    public void run() {
        Tuple2<List<PreparedStatement>, Long> tuple2 = ExecuteSqlThreadPool.threadLocal.get();
        try{
            if(tuple2 == null) {
                init();
                tuple2 = ExecuteSqlThreadPool.threadLocal.get();
            }
            if(System.currentTimeMillis() - tuple2.f1 > 10*60*1000){
                tryRebuild(tuple2);
                tuple2.setField(System.currentTimeMillis(), 1);
            }
            dynamicsExecutor.executeBatch(tuple2.f0, partitioner);
        } catch (Exception e) {
            synchronized (errors) {
                errors.add(new Exception(e.getLocalizedMessage() + dynamicsExecutor.toString()));
            }
            log.error("执行insert sql 异常", e);
        }
    }

    private void init() throws Exception{
        init(new ClickHouseConnectionProvider(options));
    }

    private void init(ClickHouseConnectionProvider provider) throws Exception{
        log.info("创建clickhouse连接....");
        List<PreparedStatement> statements = new ArrayList<>();
        for(ClickHouseConnection connection : provider.getOrCreateShardConnections()){
            statements.add(connection.prepareStatement(dynamicsExecutor.getSql()));
        }
        ExecuteSqlThreadPool.threadLocal.set(new Tuple2<>(statements, System.currentTimeMillis()));
    }

    private BalancedClickhouseDataSource getDataSource(){
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(this.options.getUsername().orElse(null));
        properties.setPassword(this.options.getPassword().orElse(null));
        BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource(options.getUrl(), properties);
        dataSource.actualize();
        return dataSource;
    }

    private void tryRebuild(Tuple2<List<PreparedStatement>, Long> tuple2) throws Exception {
        ClickHouseConnectionProvider provider = new ClickHouseConnectionProvider(options);
        if (tuple2.f0.size() != getDataSource().getAllClickhouseUrls().size()
                || getDataSource().hasDisabledUrls()) {
            for (PreparedStatement preparedStatement : tuple2.f0) {
                try {
                    preparedStatement.getConnection().close();
                } catch (Exception e) {
                    log.error("关闭连接异常： ", e);
                }
                try {
                    preparedStatement.close();
                } catch (Exception e) {
                    log.error("关闭statement异常： ", e);
                }
            }
            init(provider);
        } else {
            tuple2.setField(System.currentTimeMillis(), 1);
        }
    }
}
