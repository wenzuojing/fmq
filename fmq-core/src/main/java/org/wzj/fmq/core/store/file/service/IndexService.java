package org.wzj.fmq.core.store.file.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.file.Lifecycle;
import org.wzj.fmq.core.store.file.queue.IndexQueue;
import org.wzj.fmq.core.store.file.queue.IndexQueueManager;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wens on 15-6-17.
 */
public class IndexService implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private ConcurrentHashMap<String/* topic */, IndexQueueManager> indexQueueManagers;

    private ServiceManager serviceManager;

    public IndexService(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }


    public void putMessagePositionInfo(String topic, long dataOffset, int msgSize, long createTimestamp ) {

        IndexQueueManager indexQueueManager = indexQueueManagers.get(topic);

        if(indexQueueManager == null ){
            indexQueueManager = new IndexQueueManager(topic,serviceManager.getMessageStoreConfig().getStorePathIndex() , serviceManager.getMessageStoreConfig().getIndexQueueFileSize(),this.serviceManager ) ;
        }

        indexQueueManager.buildMessageIndex(dataOffset , msgSize ,  createTimestamp) ;


    }

    public void recoverNormally() {

    }


    public void recoverAbnormally() {
    }

    public IndexQueueManager findIndexIndexQueueManager(String topic) {
        IndexQueueManager manager = this.indexQueueManagers.get(topic);
        if (manager != null) {
            return manager;
        }
        return null;
    }


    @Override
    public void init() {
        File topicDir = new File(this.serviceManager.getMessageStoreConfig().getStorePathIndex());
        File[] files = topicDir.listFiles();
        if (files != null) {
            for (File file : files) {
                String topic = file.getName();
                IndexQueueManager indexQueueManager = new IndexQueueManager(//
                        topic,//
                        this.serviceManager.getMessageStoreConfig().getStorePathIndex(),//
                        this.serviceManager.getMessageStoreConfig().getIndexQueueFileSize(),//
                        this.serviceManager);
                indexQueueManager.init();
                indexQueueManagers.put(topic, indexQueueManager);
            }
        }
    }

    @Override
    public void start()  {

    }

    @Override
    public void shutdown() {

    }

    public long getMaxIndex(String topic) {
        IndexQueueManager indexQueueManager = indexQueueManagers.get(topic);

        if(indexQueueManager != null ){
            return indexQueueManager.getMaxIndex() ;
        }

        return 0 ;
    }

    public long getMinIndex(String topic) {
        IndexQueueManager indexQueueManager = indexQueueManagers.get(topic);

        if(indexQueueManager != null ){
            return indexQueueManager.getMaxIndex() ;
        }

        return -1 ;
    }

    public long getIndexByTime(String topic, long timestamp) {

        IndexQueueManager indexQueueManager = indexQueueManagers.get(topic);

        if(indexQueueManager != null ){
            IndexQueue indexQueue = indexQueueManager.getQueueByTime(timestamp);

            if(indexQueue != null ){
                return indexQueue.getIndexByTime(timestamp) ;
            }
        }


        return -1 ;
    }

    public long getTotalMessageNum(String topic) {
        return serviceManager.getIndexService().getTotalMessageNum(topic);
    }
}
