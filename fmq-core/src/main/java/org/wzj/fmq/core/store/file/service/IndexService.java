package org.wzj.fmq.core.store.file.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.Lifecycle;
import org.wzj.fmq.core.store.file.queue.IndexQueue;
import org.wzj.fmq.core.store.file.queue.IndexQueueManager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wens on 15-6-17.
 */
public class IndexService implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private Map<String/* topic */, IndexQueueManager> indexQueueManagers = new ConcurrentHashMap<>(100);


    private ServiceManager serviceManager;

    public IndexService(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }


    public void putMessagePositionInfo(String topic, long dataOffset, int msgSize, long createTimestamp , long sequence ) {

        IndexQueueManager indexQueueManager = indexQueueManagers.get(topic);

        if(indexQueueManager == null ){
            indexQueueManager = new IndexQueueManager(topic,serviceManager.getMessageStoreConfig().getStorePathIndex() + File.separator + topic , serviceManager.getMessageStoreConfig().getIndexQueueFileSize() ) ;
        }

        indexQueueManager.buildMessageIndex( dataOffset , msgSize ,  createTimestamp , sequence ) ;


    }

    public void recoverNormally() {

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
                        this.serviceManager.getMessageStoreConfig().getIndexQueueFileSize());
                indexQueueManager.init();
                indexQueueManagers.put(topic, indexQueueManager);
            }
        }

        Map<String,Long> sequences = new HashMap<>(100) ;

        for(String topic : indexQueueManagers.keySet() ){
            sequences.put(topic, indexQueueManagers.get(topic).getMaxSequence() ) ;
        }
        this.serviceManager.getMessageStoreService().initSequences(sequences);
    }

    @Override
    public void start()  {

    }

    @Override
    public void shutdown() {

    }

    public long getMaxSequence(String topic) {
        IndexQueueManager indexQueueManager = indexQueueManagers.get(topic);

        if(indexQueueManager != null ){
            return indexQueueManager.getMaxSequence() ;
        }

        return 0 ;
    }

    public long getMinSequence(String topic) {
        IndexQueueManager indexQueueManager = indexQueueManagers.get(topic);

        if(indexQueueManager != null ){
            return indexQueueManager.getMinSequence() ;
        }

        return -1 ;
    }

    public long getSequenceByTime(String topic, long timestamp) {

        IndexQueueManager indexQueueManager = indexQueueManagers.get(topic);

        if(indexQueueManager != null ){
            IndexQueue indexQueue = indexQueueManager.getQueueByTime(timestamp);

            if(indexQueue != null ){
                return indexQueue.getSequenceByTime(timestamp) ;
            }
        }
        return -1 ;
    }

    public long getTotalMessageNum(String topic) {
        return getMaxSequence(topic) - getMinSequence(topic);
    }



}
