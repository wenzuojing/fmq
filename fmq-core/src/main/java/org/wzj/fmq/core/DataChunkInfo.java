package org.wzj.fmq.core;

import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-12.
 */
public class DataChunkInfo implements Encodable {

    static final int  B_SIZE = 8 + 8 + 4 + 8 + 1 ;

    long offset   ;
    long length   ;
    int checksum  ;
    long id       ;
    byte flag     ;

    public DataChunkInfo(long length, int checksum, byte flag) {
        this.length = length;
        this.checksum = checksum;
        this.flag = flag;
    }

    public DataChunkInfo() {

    }

    @Override
    public byte[] encode() {

        ByteBuffer buffer = ByteBuffer.allocate(B_SIZE);
        buffer.putLong(offset);
        buffer.putLong(length);
        buffer.putInt(checksum);
        buffer.putLong(id);
        buffer.put(flag) ;
        buffer.flip() ;
        return buffer.array();
    }

    @Override
    public void decode(byte[] bytes) {
        if(bytes.length != B_SIZE ){
            throw new IllegalArgumentException("bytes's length must be "+ B_SIZE +", but actually is "+bytes.length ) ;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        offset = buffer.getLong() ;
        length = buffer.getLong() ;
        checksum = buffer.getInt() ;
        id = buffer.getLong() ;
        flag = buffer.get() ;

    }
}
