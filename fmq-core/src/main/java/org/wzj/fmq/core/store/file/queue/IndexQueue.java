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

    private volatile long  minDataCreateTimestamp ;
    private volatile long  maxDataCreateTimestamp ;

    private ByteBuffer header ;


    @Override
    public void init() {

        header = mappedFile.getByteBuffer(0 , INDEX_UNIT_SIZE ) ;
        minDataCreateTimestamp = header.getLong();
        maxDataCreateTimestamp = header.getLong();

        mappedFile.setWritePosition(INDEX_UNIT_SIZE);
    }

    public IndexQueue(String dirPath, int mappedFileSize) {
        super(dirPath, mappedFileSize);
    }


    @Override
    public long recover() {

        ByteBuffer byteBuffer = mappedFile.getByteBuffer();
        int process = 0 ;
        while (byteBuffer.hasRemaining() ){

            long dataOffset = byteBuffer.getLong();
            int msgSize  = byteBuffer.getInt() ;
            long createTimestamp = byteBuffer.getLong() ;

            if(msgSize>0 && createTimestamp >0  ){
                process += INDEX_UNIT_SIZE ;
            }else{
                break;
            }
        }
        setWrotePosition(INDEX_UNIT_SIZE /*header length */ + process);
        return INDEX_UNIT_SIZE + process ;
    }


    public void appendMessageIndex(long dataOffset, int msgSize, long createTimestamp) {

        if( getMinDataCreateTimestamp() == 0 ) {
            setMinDataCreateTimestamp(createTimestamp);
        }
        setMaxDataCreateTimestamp(createTimestamp);

        ByteBuffer byteBuffer = mappedFile.getByteBuffer();

        byteBuffer.putLong(dataOffset) ;
        byteBuffer.putLong(msgSize ) ;
        byteBuffer.putLong(createTimestamp) ;


    }

    public long getMinDataCreateTimestamp() {
        return minDataCreateTimestamp;
    }

    public void setMinDataCreateTimestamp(long minDataCreateTimestamp) {
        header.putLong(0, minDataCreateTimestamp);
        this.minDataCreateTimestamp = minDataCreateTimestamp;
    }

    public long getMaxDataCreateTimestamp() {

        return maxDataCreateTimestamp;
    }

    public void setMaxDataCreateTimestamp(long maxDataCreateTimestamp) {
        header.putLong(8, maxDataCreateTimestamp ) ;
        this.maxDataCreateTimestamp = maxDataCreateTimestamp;
    }

    public int getMinIndex(){
        long fromOffset = getFromOffset();

        long c = fromOffset / mappedFile.getFileSize();

        long s = fromOffset / IndexQueue.INDEX_UNIT_SIZE;

        return (int) (s  - c);
    }

    public int getMaxIndex(){
        long writePosition = getWritePosition() ;

        long c = writePosition / mappedFile.getFileSize() ;

        long s = writePosition / IndexQueue.INDEX_UNIT_SIZE;

        return (int) (s  - c);
    }

    /**
     * 二分查找
     * @param timestamp
     * @return
     */
    public long getIndexByTime(long timestamp) {

        if( timestamp < getMinDataCreateTimestamp() || timestamp > getMaxDataCreateTimestamp() ){
            return -1 ;
        }

        return findIndex(timestamp, getMinIndex(), getMaxIndex());
    }

    private int  findIndex(long timestamp, int sIndex, int eIndex) {
        if(sIndex == eIndex ){
            return sIndex ;
        }
        int mIndex  = sIndex + eIndex / 2 ;

        StoreMessagePosition storeMessagePosition = indexFor(mIndex) ;

        if( timestamp < storeMessagePosition.getCreateTimestamp() ){
            return findIndex(timestamp, sIndex , mIndex ) ;
        }else if( timestamp < storeMessagePosition.getCreateTimestamp() ){
            return findIndex(timestamp, eIndex , mIndex ) ;
        }else {
            return mIndex ;
        }
    }

    public StoreMessagePosition indexFor(long index ) {

        if(index < getMinIndex() || index > getMinIndex() ){
            return null ;
        }

        int c = (int) (index - getMinIndex());
        ByteBuffer byteBuffer = mappedFile.getByteBuffer(INDEX_UNIT_SIZE + INDEX_UNIT_SIZE * c);
        long dataOffset = byteBuffer.getLong();
        int msgSize  = byteBuffer.getInt() ;
        long createTimestamp = byteBuffer.getLong() ;

        return new StoreMessagePosition(null , dataOffset, msgSize,createTimestamp ) ;

    }
}
