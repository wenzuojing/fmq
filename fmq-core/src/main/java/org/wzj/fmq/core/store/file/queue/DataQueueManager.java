package org.wzj.fmq.core.store.file.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.StoreMessage;
import org.wzj.fmq.core.store.file.AppendMessageResult;
import org.wzj.fmq.core.util.Utils;

import java.io.File;
import java.util.Arrays;

/**
 * Created by wens on 15-6-18.
 */
public class DataQueueManager extends AbstractQueueManager<DataQueue> {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);


    private int maxMessageSize ;

    public DataQueueManager(String storePath, int mapedFileSize , int maxMessageSize ) {
        super(storePath, mapedFileSize);
        this.maxMessageSize =  maxMessageSize ;
    }

    @Override
    public void init() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            Arrays.sort(files);
            for (File file : files) {
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                }
                try {
                    DataQueue fileQueue = new DataQueue(file.getPath(), mappedFileSize ,this.maxMessageSize );
                    this.loadedQueues.add(fileQueue);
                    log.info("load " + file.getPath() + " OK");
                } catch (Exception e) {
                    log.error("load file " + file + " error", e);
                }
            }
        }
    }


    @Override
    public long recoverNormally(long fromOffset) {

        if (!loadedQueues.isEmpty()) {
            DataQueue dataQueue = getLastQueue();
            long processOffset = dataQueue.recover();


            setCommittedWhere(processOffset);
            truncateDirtyFiles(processOffset);
        }

        return 0 ;

    }



    @Override
    public DataQueue createNewQueue() {

        try {
            readWriteLock.writeLock().lock();
            long fromOffset = 0;
            DataQueue lastQueue = getLastQueue();

            if (lastQueue != null) {

                if(!lastQueue.isFull()){
                    return lastQueue ;
                }

                fromOffset = lastQueue.getFromOffset() + mappedFileSize;
            }

            DataQueue dataQueue = new DataQueue(storePath + File.separator + Utils.long2fileName(fromOffset), mappedFileSize , this.maxMessageSize );

            dataQueue.init();
            dataQueue.start();
            this.loadedQueues.add(dataQueue);
            return dataQueue;
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }

    public AppendMessageResult appendMessage(StoreMessage msg) {
        AppendMessageResult result;
        DataQueue dataQueue = getLastQueue();
        if (null == dataQueue || dataQueue.isFull() ) {
            dataQueue = createNewQueue();
        }
        result = dataQueue.appendMessage(msg );
        return result;
    }

    private void reindex(long fromOffset, long processOffset) {

    }

}
