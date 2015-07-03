package org.wzj.fmq.core.store.file.queue;

import org.wzj.fmq.core.store.StoreMessage;
import org.wzj.fmq.core.store.file.SelectMappedBufferResult;
import org.wzj.fmq.core.store.file.StoreMessagePosition;

import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-18.
 */
public class IndexQueue extends AbstractQueue {

    public static final int INDEX_UNIT_SIZE = 20 ;

    private volatile long  fromTimestamp ;



    @Override
    public void init() {
        fromTimestamp = mappedFile.getByteBuffer(0).getLong(12) ;
        mappedFile.setWritePosition(INDEX_UNIT_SIZE);
    }

    public IndexQueue(String filePath, int mappedFileSize) {
        super(filePath, mappedFileSize);
    }


    public void appendMessageIndex(long dataOffset, int msgSize, long timestamp) {

        if( getFromTimestamp() == 0 ) {
            setFromTimestamp(timestamp);
        }
        int writeOffset = getWritePosition();

        ByteBuffer byteBuffer = mappedFile.getByteBuffer(writeOffset);

        byteBuffer.putLong(dataOffset) ;
        byteBuffer.putInt(msgSize) ;
        byteBuffer.putLong(timestamp) ;

        setWritePosition(writeOffset+INDEX_UNIT_SIZE);


    }

    public void setFromTimestamp(long fromTimestamp) {
        this.fromTimestamp = fromTimestamp;
    }

    public long getFromTimestamp() {
        return fromTimestamp;
    }

    public long getMinSequence(){
        long fromOffset = getFromOffset();
        return fromOffset / mappedFile.getFileSize();
    }

    public long getMaxSequence(){
        long writePosition = getFromOffset() + getWritePosition();
        return writePosition / IndexQueue.INDEX_UNIT_SIZE;
    }


    public long getSequenceByTime(long timestamp) {

        if( timestamp < getFromTimestamp()){
            return -1 ;
        }

        ByteBuffer byteBuffer = mappedFile.getByteBuffer(0);

        for(int i = 0 , len = mappedFile.getFileSize() / INDEX_UNIT_SIZE ; i < len ; i++ ){

            long t  = byteBuffer.getLong(i * INDEX_UNIT_SIZE + 12  ) ;

            if(t >= timestamp ){
                return getMinSequence() + i ;
            }

        }

        return  -1 ;
    }


    public StoreMessagePosition indexFor(long sequence ) {

        if(sequence < getMinSequence() || sequence > getMaxSequence() ){
            return null ;
        }

        int c = (int) (sequence - getMinSequence());
        ByteBuffer byteBuffer = mappedFile.getByteBuffer(INDEX_UNIT_SIZE + INDEX_UNIT_SIZE * c);
        long dataOffset = byteBuffer.getLong();
        int msgSize  = byteBuffer.getInt() ;
        long createTimestamp = byteBuffer.getLong() ;

        return new StoreMessagePosition(null , dataOffset, msgSize,createTimestamp ,sequence  ) ;

    }
}
