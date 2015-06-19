package org.wzj.fmq.core.store.file.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.common.ServiceThread;
import org.wzj.fmq.core.store.file.Lifecycle;
import org.wzj.fmq.core.store.file.StoreMessagePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by wens on 15-6-17.
 */
public class DispatchMessageService extends ServiceThread implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private volatile BlockingQueue<StoreMessagePosition> requestQueue ;

    private ServiceManager serviceManager;


    public DispatchMessageService(ServiceManager serviceManager, int putMsgIndexHigthWater) {
        this.serviceManager = serviceManager;
        putMsgIndexHigthWater *= 1.5;
        requestQueue = new ArrayBlockingQueue<StoreMessagePosition>(putMsgIndexHigthWater) ;
    }


    public void putRequest( StoreMessagePosition dispatchRequest) {
        try {
            requestQueue.put(dispatchRequest);
        } catch (InterruptedException e) {
            //
        }
    }



    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStoped() && requestQueue.size() == 0) {
            try {
                this.waitForRunning(0);

                StoreMessagePosition storeMessagePosition = requestQueue.take();

                this.serviceManager.getIndexService().putMessagePositionInfo(storeMessagePosition.getTopic(),
                        storeMessagePosition.getDataQueueOffset(), storeMessagePosition.getMsgSize(),
                        storeMessagePosition.getCreateTimestamp());

            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }


    @Override
    public String getServiceName() {
        return DispatchMessageService.class.getSimpleName();
    }

    public void putDispatchRequest(final StoreMessagePosition dispatchRequest) {
        putRequest(dispatchRequest);
    }

    @Override
    public void init() {

    }
}
