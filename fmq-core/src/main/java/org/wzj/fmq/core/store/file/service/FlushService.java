package org.wzj.fmq.core.store.file.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.common.ServiceThread;
import org.wzj.fmq.core.store.file.Lifecycle;

/**
 * Created by wens on 15-6-17.
 */
public class FlushService extends ServiceThread implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);



    private ServiceManager serviceManager;

    public FlushService(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }



    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStoped()) {
            try {
                int interval = this.serviceManager.getMessageStoreConfig().getFlushInterval();
                this.waitForRunning(interval);
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }


    @Override
    public String getServiceName() {
        return FlushService.class.getSimpleName();
    }


    @Override
    public long getJointime() {
        return 1000 * 60;
    }

    @Override
    public void init() {

    }
}
