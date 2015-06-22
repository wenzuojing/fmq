package org.wzj.fmq.core.store.file.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.Lifecycle;
import org.wzj.fmq.core.store.file.DefaultMappedFileImpl;
import org.wzj.fmq.core.store.file.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-22.
 */
public class CheckpointService implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private MappedFile mappedFile ;

    private volatile long dataQueueCheckpoint ;

    private volatile long indexQueueCheckpoint ;

    public CheckpointService(String checkpointFileName){
        try {
            mappedFile = new DefaultMappedFileImpl(checkpointFileName,16) ;
        } catch (IOException e) {
            throw new RuntimeException("new map file implement instance fail." , e ) ;
        }
    }

    @Override
    public void init() {
        ByteBuffer byteBuffer = mappedFile.getByteBuffer(0);
        dataQueueCheckpoint =  byteBuffer.getLong() ;
        indexQueueCheckpoint = byteBuffer.getLong() ;
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {
        try {
            mappedFile.close();
        } catch (IOException e) {
            log.warn("Close map file fail , error:{}" , e );
        }
    }

    public long getDataQueueCheckpoint() {
        return dataQueueCheckpoint;
    }

    public void setDataQueueCheckpoint(long dataQueueCheckpoint) {
        this.dataQueueCheckpoint = dataQueueCheckpoint;
    }

    public long getIndexQueueCheckpoint() {
        return indexQueueCheckpoint;
    }

    public void setIndexQueueCheckpoint(long indexQueueCheckpoint) {
        this.indexQueueCheckpoint = indexQueueCheckpoint;
    }

    public void flushCheckpoint(){
        ByteBuffer byteBuffer = this.mappedFile.getByteBuffer(0);
        byteBuffer.putLong(dataQueueCheckpoint);
        byteBuffer.putLong(indexQueueCheckpoint);
        try {
            this.mappedFile.flush();
        } catch (IOException e) {
            log.warn("Flush map file fail , error:{}" , e );
        }
    }
}
