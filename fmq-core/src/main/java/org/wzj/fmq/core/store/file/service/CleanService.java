package org.wzj.fmq.core.store.file.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.common.ServiceThread;
import org.wzj.fmq.core.store.file.Lifecycle;

/**
 * Created by wens on 15-6-17.
 */
public class CleanService extends ServiceThread implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);


    private ServiceManager serviceManager;

    public CleanService(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }




    public void run() {
        log.info(this.getServiceName() + " service started");
        int cleanResourceInterval =
                this.serviceManager.getMessageStoreConfig().getCleanResourceInterval();
        while (!this.isStoped()) {
            try {
                //todo list

                this.waitForRunning(cleanResourceInterval);
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }


    @Override
    public String getServiceName() {
        return CleanService.class.getSimpleName();
    }

    @Override
    public void init() {

    }
}
