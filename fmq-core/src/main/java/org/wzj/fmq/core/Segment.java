package org.wzj.fmq.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.SortedSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by wens on 15-6-12.
 */
public class Segment implements Comparable<Segment> ,Closeable {

    public static final String DATA_FILE_NAME_PREFFIX = "data-" ;
    //public static final String INDEX_FILE_NAME_PREFFIX = "index-" ;

    public static final long MAX_BYTE_SIZE = 1 * 1024 * 1024 * 1024 ; //1G

    private long sid ;

    private String basePath ;

    private long byteSize ;

    private  SegmentIndex index ;

    private ReadWriteLock rwLock = new ReentrantReadWriteLock() ;

    private WriteMappedFile writeMappedFile ;

    private SortedSet<DataChunkInfo>

    public Segment(String basePath , long sid ) {
        this.sid = sid;
        this.basePath = basePath;
    }

    public void loadDataChunkInfo(){

        RandomAccessFile raf  = null ;
        try{
            raf = new RandomAccessFile(basePath + File.separator + DATA_FILE_NAME_PREFFIX +sid , "r") ;

            byte[] buf  = new byte[DataChunkInfo.Header.B_SIZE] ;
            while(true){
                int n = raf.read(buf);

                if(DataChunkInfo.Header.B_SIZE != n ){
                    break ;
                }

                DataChunkInfo.Header header = new DataChunkInfo.Header();
                header.decode(buf);
                DataChunkInfo dataChunk = new DataChunkInfo(header, null);
                raf.skipBytes((int)header.length) ;

            }


        }catch (IOException e ){
            throw new RuntimeException("load chunk info fail." , e ) ;
        }

    }


    public Message query(long id ) {
        return null ;
    }


    @Override
    public int compareTo(Segment o) {
        return (int)( sid - o.sid ) ;
    }

    public void write(SegmentIndex.IndexItem indexItem, Message message) throws IOException {
        rwLock.writeLock().lock();
        try{
            long id  = sid + index.getSize()  + 1 ;
            indexItem.offset = this.byteSize ;
            indexItem.id = id   ;
            write_0(message.getData()) ;
            index.write(indexItem) ;
            message.setId(id);
        }finally {
            rwLock.writeLock().unlock();
        }


    }

    private void write_0(byte[] bytes) throws IOException {
        if( writeMappedFile == null ){
            writeMappedFile = new WriteMappedFile(basePath + File.separator + DATA_FILE_NAME_PREFFIX + sid  , 0 , 50 * 1024 * 1024 , 100  * 1024 ) ;
        }
        writeMappedFile.write(bytes);
    }

    public long getSid() {
        return sid;
    }

    public SegmentIndex getIndex() {
        return index;
    }

    public boolean isFull() {

        rwLock.readLock().lock(); ;
        try{
            if(byteSize > MAX_BYTE_SIZE) {
                return true ;
            }else {
                return false ;
            }
        }finally {
            rwLock.readLock().unlock();
        }



    }

    @Override
    public void close() throws IOException {

    }
}
