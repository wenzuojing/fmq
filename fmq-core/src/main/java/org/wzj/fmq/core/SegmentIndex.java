package org.wzj.fmq.core;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by wens on 15-6-12.
 */
public class SegmentIndex implements Closeable {


    private String baseDir ;

    private String name ;

    private SortedSet<IndexItem> indexItems  ;
    private WriteMappedFile writeMappedFile ;

    private volatile boolean closed = false ;

    private static ReadWriteLock rwLock = new ReentrantReadWriteLock() ;

    public SegmentIndex( String baseDir , String  name){
        this.baseDir = baseDir ;
        this.name = name ;
        this.indexItems = new TreeSet<>() ;
        init();

    }

    private void init() {
        loadIndexs() ;

    }

    private void loadIndexs(){

        //FileInputStream fileInputStream = null ;
        BufferedInputStream bufferedInputStream = null ;
        try{
            bufferedInputStream = new BufferedInputStream(new FileInputStream(baseDir + File.separator + name) , IndexItem.B_SIZE * 10000 ) ;
            byte[] buf  = new byte[IndexItem.B_SIZE] ;
            for(;;){
                int n = bufferedInputStream.read(buf);

                if(n < 0 ){
                    break ;
                }

                IndexItem indexItem = new IndexItem();
                indexItem.decode(buf);
                indexItems.add(indexItem) ;
            }


        }catch (IOException e){

        }finally {

            if(bufferedInputStream != null ){
                try {
                    bufferedInputStream.close();
                } catch (IOException e) {
                    //
                }
            }
        }


    }



    public void write(IndexItem indexItem) throws IOException {

        checkState() ;
        rwLock.writeLock().lock();
        try{

            if( writeMappedFile == null ){
                this.writeMappedFile = new WriteMappedFile(baseDir + File.separator + name , this.indexItems.size() * IndexItem.B_SIZE , 10 *1024*1024 , 100 * 1024 ) ;
            }

            writeMappedFile.write(indexItem.encode());
            indexItems.add(indexItem) ;
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     *
     * @return
     */
    public long getSize() {
        checkState() ;
        rwLock.readLock().lock(); ;
        try{
            return this.indexItems.size();
        }finally {
            rwLock.readLock().unlock();
        }

    }

    private void checkState(){
        if(closed){
            throw new ClosedIndexException() ;
        }
    }

    @Override
    public void close() throws IOException {

        closed = true ;

        this.indexItems.clear();

        if(writeMappedFile != null ){
            writeMappedFile.close();
        }


    }

    static class IndexItem  implements Encodable , Comparable<IndexItem> {

        static final int  B_SIZE = 8 + 8 + 4 + 8 + 1 ;

         long offset   ;
         long length   ;
         int checksum  ;
         long id       ;
         byte flag     ;

        public IndexItem(long length, int checksum, byte flag) {
            this.length = length;
            this.checksum = checksum;
            this.flag = flag;
        }

        public IndexItem() {

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

        @Override
        public int compareTo(IndexItem o) {
            return (int) (id - o.id);
        }
    }



}
