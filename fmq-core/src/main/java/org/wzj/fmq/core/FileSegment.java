package org.wzj.fmq.core;

import java.io.*;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by wens on 15-6-12.
 */
public class FileSegment extends Segment {

    public static final String DATA_FILE_NAME_PREFFIX = "data-" ;

    public static final int MAX_BYTE_SIZE = 1 * 1024 * 1024 * 1024 ; //1G
    private String filePath  ;
    private WriteMappedFile writeMappedFile ;
    private BufferedRandomReadFile readBufferedRandomAccessFile ;

    public FileSegment(String basePath, long sid) {
        super(sid, MAX_BYTE_SIZE);
        this.filePath = basePath + File.separator + DATA_FILE_NAME_PREFFIX +sid ;
        checkFile();
        try {
            readBufferedRandomAccessFile = new BufferedRandomReadFile( this.filePath , 512 * 1024 ) ;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e) ;
        }
    }

    private void checkFile(){
        File file = new File(this.filePath);
        if(!file.exists()){
            try {
                file.createNewFile() ;
            } catch (IOException e) {
                throw new RuntimeException("Create file fail:"+this.filePath , e ) ;
            }
        }
    }


    @Override
    protected void loadChunkInfo() {
        RandomAccessFile raf  = null ;
        try{

            raf = new RandomAccessFile( this.filePath  , "r") ;

            byte[] buf  = new byte[ChunkInfo.B_SIZE] ;
            while(true){
                int n = raf.read(buf);

                if(ChunkInfo.B_SIZE != n || buf[0] != ChunkInfo.MAGIC ){
                    break ;
                }
                ChunkInfo chunkInfo  = new ChunkInfo();
                chunkInfo.decode(buf);
                raf.skipBytes(chunkInfo.length);
                byteSize += chunkInfo.length + ChunkInfo.B_SIZE ;
                cid = chunkInfo.id ;
                this.chunkInfos.add(chunkInfo) ;
            }

        }catch (IOException e ){
            throw new RuntimeException("load chunk info fail." , e ) ;
        }finally {
            if(raf != null ){
                try {
                    raf.close();
                } catch (IOException e) {
                    //
                }
            }
        }
    }

    @Override
    protected Message query(ChunkInfo chunkInfo) throws IOException {
        synchronized (readBufferedRandomAccessFile){
            readBufferedRandomAccessFile.seek(chunkInfo.offset + ChunkInfo.B_SIZE);
            byte[] buf = new byte[chunkInfo.length];
            readBufferedRandomAccessFile.read(buf) ;
            return new Message(chunkInfo.id , buf );
        }
    }

    protected void write_0(byte[] bytes) throws IOException {
        if( writeMappedFile == null ){
            writeMappedFile = new WriteMappedFile(this.filePath , 0 , 50 * 1024 * 1024 , 100  * 1024  ) ;
        }
        writeMappedFile.write(bytes);
    }


    @Override
    public void close()  {
        super.close();

        try {
            readBufferedRandomAccessFile.close();
        } catch (IOException e) {
            //
        }
        try {
            writeMappedFile.close();
        } catch (IOException e) {
            //
        }
    }

    public void completeWrite(){
        try {
            writeMappedFile.close();
            writeMappedFile = null ;
        } catch (IOException e) {
            throw new RuntimeException("close writeMappedFile fail." , e );
        }

    }

}
