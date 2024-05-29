package org.example.thread;

import java.util.concurrent.ThreadFactory;

/**
 * @author yuankuo.xia
 * @Date 2024/5/28
 */
public class ExecuteSqlThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r);
    }
}
