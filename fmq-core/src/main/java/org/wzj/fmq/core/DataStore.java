package org.wzj.fmq.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wens on 15-6-11.
 */
public interface DataStore extends Cloneable {


    /**
     * 写入消息
     * @param message
     */
    void write(Message message ) throws IOException;

    /**
     * 检索消息
     * @param startId
     * @param size
     * @return
     */
    List<Message> query(long startId , int size ) ;



}
