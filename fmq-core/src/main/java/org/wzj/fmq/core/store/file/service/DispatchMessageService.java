package org.wzj.fmq.core.store.file.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.common.ServiceThread;
import org.wzj.fmq.core.store.Lifecycle;
import org.wzj.fmq.core.store.file.StoreMessagePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by wens on 15-6-17.
 */
public class DispatchMessageService extends ServiceThread{

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private volatile BlockingQueue<StoreMessagePosition> requestQueue ;

    private ServiceManager serviceManager;

    private StoreMessagePosition poison = new StoreMessagePosition(-1) ;




    public DispatchMessageService(ServiceManager serviceManager, int putMsgIndexHigthWater) {
        super("dispatch-message");
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


    @Override
    public void executeTask() {
        try {
            StoreMessagePosition storeMessagePosition = requestQueue.take();

            if(storeMessagePosition.getMsgSize() == -1 ){
                synchronized (storeMessagePosition){
                    storeMessagePosition.notify();
                }
                return ;
            }

            log.debug("dispatch -> {}" , storeMessagePosition );

            this.serviceManager.getIndexService().putMessagePositionInfo(storeMessagePosition.getTopic(),
                    storeMessagePosition.getDataQueueOffset(), storeMessagePosition.getMsgSize(),
                    storeMessagePosition.getCreateTimestamp());

        } catch (Exception e) {
            //
        }
    }



    public void putDispatchRequest(final StoreMessagePosition dispatchRequest) {
        putRequest(dispatchRequest);
    }


    @Override
    public void shutdown() {
        try {
            this.requestQueue.put(poison);
            synchronized (poison){
                poison.wait();
            }
        } catch (InterruptedException e) {
            //
        }
        super.shutdown();

    }
}
