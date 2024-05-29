package org.example.connector;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.clickhouse.internal.options.ClickHouseOptions;
import org.apache.flink.connector.clickhouse.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.connector.clickhouse.internal.partitioner.ShufflePartitioner;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;
import org.example.function.ThrowingFunction;
import org.example.thread.ExecuteSqlThread;
import org.example.thread.ExecuteSqlThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * @author yuankuo.xia
 * @Date 2024/5/27
 */
public class DynamicsSchemaOutputFormat extends RichOutputFormat<JSONObject> implements Flushable {

    private static final Logger log = LoggerFactory.getLogger(DynamicsSchemaOutputFormat.class);

    private static final long serialVersionUID = 1L;

    protected transient volatile boolean closed = false;

    protected transient volatile List<Exception> exceptions;

    private final ClickHouseOptions options;

    private final ClickHousePartitioner partitioner;

    private final Function<JSONObject, String> tableGetter;

    private transient ConcurrentHashMap<String, DynamicsExecutor> executors;
    protected transient ScheduledExecutorService schedulerFlush;
    protected transient ScheduledFuture<?> scheduledFutureFlush;
    protected transient ScheduledExecutorService schedulerClear;
    protected transient ScheduledFuture<?> scheduledFutureClear;
    private transient ExecuteSqlThreadPool threadPool;

    public DynamicsSchemaOutputFormat(ClickHouseOptions options, Function<JSONObject, String> tableGetter){
        this.options = options;
        this.partitioner = new ShufflePartitioner();
        this.tableGetter = tableGetter;
    }

    @Override
    public void open(int i, int i1) throws IOException {
        this.executors = new ConcurrentHashMap<>(32);
        this.exceptions = new ArrayList<>(16);
        this.threadPool = ExecuteSqlThreadPool.schedule(10, 20);
        this.scheduledFlush(options.getFlushInterval().toMillis(), "flush");
        this.scheduledClear(10*60*1000, "clear");
    }

    public void scheduledFlush(long intervalMillis, String executorName) {
        Preconditions.checkArgument(intervalMillis > 0, "flush interval must be greater than 0");
        schedulerFlush = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory(executorName));
        scheduledFutureFlush =
                schedulerFlush.scheduleWithFixedDelay(
                        () -> {
                            synchronized (this) {
                                if (!closed) {
                                    try {
                                        flushAll();
                                    } catch (Exception e) {
                                        exceptions.add(e);
                                    }
                                }
                            }
                        },
                        intervalMillis,
                        intervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    public void scheduledClear(long intervalMillis, String executorName) {
        Preconditions.checkArgument(intervalMillis > 0, "flush interval must be greater than 0");
        schedulerClear = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory(executorName));
        scheduledFutureClear =
                schedulerClear.scheduleWithFixedDelay(
                        () -> {
                            synchronized (this) {
                                if (!closed) {
                                    Set<String> removes = new HashSet<>();
                                    executors.forEach((k, v) -> {
                                        if(System.currentTimeMillis() - v.timeout.get() >= 10*60*1000){
                                            removes.add(k);
                                        }
                                    });
                                    removes.forEach(k -> executors.remove(k));
                                }
                            }
                        },
                        intervalMillis,
                        intervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public void writeRecord(JSONObject jsonObject) throws IOException {
        String table = tableGetter.apply(jsonObject);
        DynamicsExecutor executor = executors.computeIfAbsent(table, consumerWrapper(key -> new DynamicsExecutor(table, jsonObject.keySet().toArray(new String[]{}), options)));
        executor.addToBatch(jsonObject);
        if(executor.incrementAndGet() > options.getBatchSize()){
            flush(executor);
        }
    }

    @Override
    public void flush() throws IOException {
        this.flushAll();
    }

    public void flushAll(){
        checkFlushException();
        for(DynamicsExecutor executor : executors.values()){
            threadPool.execute(new ExecuteSqlThread(options, executor, partitioner, exceptions));
        }
    }

    public synchronized void checkpointFlushAll() throws IOException{
        flushAll();
        long timeout = 0;
        long sleep = 1000;
        while(threadPool.runningNum.get() > 0){
            timeout += sleep;
            try{
                Thread.sleep(sleep);
            }catch (Exception e){
                log.error("sleep error in flush all!");
            }
            if(timeout > 60*1000){
                throw new IOException("刷写数据超时");
            }
        }
        if(exceptions.size() > 0){
            exceptions.forEach(e -> log.error(e.getLocalizedMessage(), e));
            throw new IOException("写数据库异常");
        }
    }

    public void flush(@Nonnull DynamicsExecutor executor) throws IOException {
        checkFlushException();
        threadPool.execute(new ExecuteSqlThread(options, executor, partitioner, exceptions));
    }

    public void checkFlushException() {
        if (exceptions.size() > 0) {
            exceptions.forEach(e -> log.error(e.getLocalizedMessage(), e));
            throw new RuntimeException("Flush exception found.");
        }
    }

    public void closeOutputFormat() throws IOException{
        if(!closed) {
            flushAll();//TODO 待测试
            try{
                if(scheduledFutureFlush != null && !scheduledFutureFlush.isCancelled()){
                    this.scheduledFutureFlush.cancel(false);
                    this.schedulerFlush.shutdown();
                }
            }catch (Exception e){
                log.error("flush thread shutdown failed! {}", e.getLocalizedMessage(), e);
            }
            try{
                if(scheduledFutureClear != null && !scheduledFutureClear.isCancelled()){
                    this.scheduledFutureClear.cancel(false);
                    this.schedulerClear.shutdown();
                }
            }catch (Exception e){
                log.error("flush thread shutdown failed! {}", e.getLocalizedMessage(), e);
            }
            if (!threadPool.isShutdown()) {
                threadPool.shutdown();
            }
        }
    }

    @Override
    public void configure(Configuration configuration) {

    }

    /**
     * 异常转换为runtime
     */
    static <T, V> Function<T, V> consumerWrapper(ThrowingFunction<T, V, Exception> throwingFunction) {
        return  k -> {
            try {
                return throwingFunction.apply(k);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public void close() throws IOException {
        closeOutputFormat();
    }
}
