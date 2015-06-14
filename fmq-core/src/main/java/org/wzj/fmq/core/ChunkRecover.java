package org.wzj.fmq.core;

/**
 * Created by wens on 15-6-14.
 */
public interface ChunkRecover {

    void recover(ChunkInfo chunkInfo , byte[] data ) ;

}
