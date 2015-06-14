package org.wzj.fmq.core;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by wens on 15-6-12.
 */
public class MemSegment extends Segment implements ChunkRecover {

    public static final int MAX_BYTE_SIZE = 50 * 1024 * 1024 ; // 50

    private String basePath ;

    private MemSegmentLog memSegmentLog ;

    private ByteBuffer byteBuffer ;

    public MemSegment( String basePath ,long sid) {
        super( sid , MAX_BYTE_SIZE );
        byteBuffer = ByteBuffer.allocateDirect( MAX_BYTE_SIZE ) ;
        memSegmentLog = new MemSegmentLog(this.basePath + File.separator + sid + ".log" ) ;
    }


    @Override
    protected void loadChunkInfo(){
        memSegmentLog.recover( this );
    }

    @Override
    protected Message query(ChunkInfo chunkInfo) throws IOException {
        return null;
    }

    @Override
    protected void write_0(byte[] bytes) throws IOException {

    }


    @Override
    public void recover(ChunkInfo chunkInfo, byte[] data) {

    }
}
