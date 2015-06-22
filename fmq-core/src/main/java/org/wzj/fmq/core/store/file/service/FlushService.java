package org.wzj.fmq.core.store.file.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.common.ServiceThread;
import org.wzj.fmq.core.store.Lifecycle;

/**
 * Created by wens on 15-6-17.
 */
public class FlushService extends ServiceThread implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private ServiceManager serviceManager;

    public FlushService(ServiceManager serviceManager) {
        super("flush-service");
        this.serviceManager = serviceManager;
    }


    @Override
    public void executeTask() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
