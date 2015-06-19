package org.wzj.fmq.core.store;

/**
 * Created by wens on 15-6-16.
 */
public interface Lifecycle {

    void init();

    void start() throws Exception;


    void shutdown();
}
