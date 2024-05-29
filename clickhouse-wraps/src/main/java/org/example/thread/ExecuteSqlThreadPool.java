package org.example.thread;

import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yuankuo.xia
 * @Date 2024/5/28
 */
public class ExecuteSqlThreadPool extends ThreadPoolExecutor{

    public final AtomicInteger runningNum = new AtomicInteger(0);

    public static final ThreadLocal<Tuple2<List<PreparedStatement>, Long>> threadLocal = new ThreadLocal<>();

    public ExecuteSqlThreadPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory,
            RejectedExecutionHandler handler) {
        super(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                workQueue,
                threadFactory,
                handler);
    }

    /**
     * 固定长度，无缓冲线程的线程池
     * @param corePoolSize   线程池长度
     * @param blockingQueueSize 缓冲队列长度
     */
    public static ExecuteSqlThreadPool schedule(int corePoolSize, int blockingQueueSize){
        return new ExecuteSqlThreadPool(corePoolSize,
                corePoolSize,  //固定长度，无缓冲线程
                0,    //无需存活时间
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(Math.max(blockingQueueSize, 0)),   //无缓冲队列
                new ExecuteSqlThreadFactory(),
                new CallerRunsPolicy());   //拒绝策略，由调用线程执行
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        runningNum.incrementAndGet();
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        runningNum.decrementAndGet();
    }
}
