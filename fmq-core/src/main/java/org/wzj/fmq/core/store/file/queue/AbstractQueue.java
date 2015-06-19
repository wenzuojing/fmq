package org.wzj.fmq.core.store.file.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.file.DefaultMappedFileImpl;
import org.wzj.fmq.core.store.file.MappedFile;
import org.wzj.fmq.core.store.file.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-18.
 */
public abstract class AbstractQueue implements FileQueue {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    protected MappedFile mappedFile;

    public AbstractQueue(String fileName , int mappedFileSize) {

        try {
            mappedFile = new DefaultMappedFileImpl( fileName , mappedFileSize ) ;
        } catch (IOException e) {
            throw new RuntimeException("new MappedFile instance fail" , e) ;
        }

    }

    @Override
    public long getQueueCreateTimestamp() {
        return mappedFile.getFile().lastModified() ;
    }

    @Override
    public void delete() {
        mappedFile.delete() ;
    }

    @Override
    public void setCommittedPosition(int position) {

    }

    @Override
    public long getFromOffset() {
        return Long.parseLong(mappedFile.getFile().getName());
    }

    @Override
    public void setWrotePosition(int position) {
        mappedFile.setWritePosition(position);
    }


    @Override
    public boolean isFull() {
        return mappedFile.isFull() ;
    }

    @Override
    public long getWritePosition() {
        return 0;
    }

    @Override
    public void commit() {
        try {
            mappedFile.flush();
        } catch (IOException e) {
            log.warn("flush map file fail , file:{}" ,  mappedFile.getFile() );
        }
    }

    @Override
    public boolean isValid() {
        return !mappedFile.isClose();
    }

    @Override
    public boolean isDirty() {
        return mappedFile.isDirty();
    }

    @Override
    public void init() {

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() {
        try {
            mappedFile.close();
        } catch (IOException e) {
            log.warn("close file fail , file :{} , error : {}" , mappedFile.getFile() , e );
        }
    }


    public  SelectMappedBufferResult selectMappedBuffer(int pos, int size){
        ByteBuffer byteBuffer = mappedFile.getByteBuffer(pos);
        byteBuffer.limit(size);
        return new SelectMappedBufferResult(pos , byteBuffer , size ) ;

    }
}
