package org.wzj.fmq.core.store.file.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.file.StoreMessagePosition;
import org.wzj.fmq.core.store.file.service.ServiceManager;
import org.wzj.fmq.core.util.Utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by wens on 15-6-18.
 */
public class IndexQueueManager extends AbstractQueueManager<IndexQueue> {


    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private final String topic;
    private final String storePath;
    private final int mapedFileSize;


    public IndexQueueManager(String topic,String storePath,int mapedFileSize ) {
        super(storePath + File.separator + topic, mapedFileSize);
        this.storePath = storePath;
        this.mapedFileSize = mapedFileSize;
        this.topic = topic;
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
                    IndexQueue fileQueue = new IndexQueue(file.getPath(), mappedFileSize);
                    fileQueue.init();
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
        throw new UnsupportedOperationException() ;
    }


    @Override
    public IndexQueue createNewQueue() {

        try {
            readWriteLock.writeLock().lock();
            long fromOffset = 0;
            IndexQueue lastQueue = getLastQueue();

            if (lastQueue != null) {

                if( !lastQueue.isFull() ){
                    return lastQueue ;
                }

                fromOffset = lastQueue.getFromOffset() + mappedFileSize;
            }

            IndexQueue indexQueue = new IndexQueue(storePath + File.separator + Utils.long2fileName(fromOffset), mappedFileSize );

            indexQueue.init();
            indexQueue.start();
            this.loadedQueues.add(indexQueue);
            return indexQueue;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }


    public long getTotalMessageNum() {
        return getMaxSequence() - getMinSequence() ;
    }

    public StoreMessagePosition indexStoreMessagePosition(long sequence) {

        if(sequence < getMinSequence() || sequence > getMaxSequence() ){
            return null ;
        }

        for (int i = loadedQueues.size() - 1 ; i >= 0 ; i-- ){
            if( sequence >= loadedQueues.get(i).getMinSequence() && sequence <= loadedQueues.get(i).getMaxSequence() ){
                return loadedQueues.get(i).indexFor(sequence) ;
            }
        }

        return null;
    }

    public long getMinSequence() {

        IndexQueue firstQueue = getFirstQueue();

        if(firstQueue != null ){
            return firstQueue.getMinSequence() ;
        }

        return 0 ;
    }

    public long getMaxSequence() {
        IndexQueue lastQueue = getLastQueue();

        if(lastQueue != null ){
            return lastQueue.getMaxSequence() ;
        }
        return 0;
    }

    public void buildMessageIndex(long dataOffset, int msgSize, long createTimestamp ,long sequence ) {

        try {

            long fromOffset = sequence / ( mapedFileSize / IndexQueue.INDEX_UNIT_SIZE  -1 ) * mapedFileSize  ;

            readWriteLock.writeLock().lock();
            IndexQueue lastQueue = getLastQueue();

            if(lastQueue == null || lastQueue.isFull() ){
                lastQueue = createNewQueue() ;
            }

            lastQueue.appendMessageIndex(dataOffset, msgSize, createTimestamp) ;
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }

    public IndexQueue getQueueByTime(final long timestamp) {

        Iterator<IndexQueue> iterator = loadedQueues.iterator();
        while (iterator.hasNext()) {
            IndexQueue indexQueue = iterator.next();
            if (timestamp >= indexQueue.getFromTimestamp()) {
                return indexQueue;
            }
        }

        return null;
    }
}
