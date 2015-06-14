package org.wzj.fmq.core;

import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-12.
 */
public class ChunkInfo implements Encodable , Comparable<ChunkInfo> {

    static final byte MAGIC = 1 << 0 | 1 << 1 << 1 ;

    static final int  B_SIZE = 1 + 8 + 4 + 4 + 4 + 1  ;



    long id       ;
    int offset   ;
    int length   ;
    int checksum  ;
    byte flag     ;
    byte magic ;

    public ChunkInfo(int length, int checksum, byte flag) {
        this.length = length;
        this.checksum = checksum;
        this.flag = flag;
    }



    private ChunkInfo(long id) {
        this.id  = id ;
    }

    public ChunkInfo() {

    }

    @Override
    public byte[] encode() {

        ByteBuffer buffer = ByteBuffer.allocate(B_SIZE);
        buffer.put(MAGIC);
        buffer.putLong(id);
        buffer.putInt(offset);
        buffer.putInt(length);
        buffer.putInt(checksum);
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
        magic = buffer.get();
        id = buffer.getLong() ;
        offset = buffer.getInt() ;
        length = buffer.getInt() ;
        checksum = buffer.getInt() ;
        flag = buffer.get() ;

    }

    @Override
    public int compareTo(ChunkInfo o) {
        return (int) (id - o.id);
    }

    public static ChunkInfo toChunkInfo(long id ){
        return new ChunkInfo(id){

            @Override
            public byte[] encode() {
                throw new UnsupportedOperationException() ;
            }

            @Override
            public void decode(byte[] bytes) {
                throw new UnsupportedOperationException() ;
            }

            @Override
            public int compareTo(ChunkInfo o) {
                return super.compareTo(o);
            }


        } ;
    }
}
