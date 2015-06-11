package org.wzj.fmq.core;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wens on 15-6-11.
 */
public class Threads {

    public static ThreadFactory make(String name ){
        return new ThreadFactory() {
            AtomicLong counter  = new AtomicLong( -1 ) ;
             @Override
            public Thread newThread(Runnable r) {
                return new Thread(name + "-" + counter.incrementAndGet() );
            }
        } ;
    }
}
