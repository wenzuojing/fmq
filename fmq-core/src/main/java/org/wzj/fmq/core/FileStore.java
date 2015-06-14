package org.wzj.fmq.core;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

/**
 * Created by wens on 15-6-12.
 */
public class FileStore implements DataStore  {

    private String baseDir ;

    private TreeSet<Segment> segments ;

    private ReadWriteLock rwLock  = new ReentrantReadWriteLock() ;


    public FileStore(String baseDir){
        this.baseDir = baseDir ;
    }

    @Override
    public void init() {
        loadSegment() ;
    }

    private void loadSegment() {
        File dir = new File(this.baseDir);

        if( !dir.exists() ){
            dir.mkdirs() ;
        }

        if(!dir.isDirectory()){
            throw new IllegalArgumentException("baseDir is not a directory:"+baseDir) ;
        }


        File[] segmentFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(FileSegment.DATA_FILE_NAME_PREFFIX);
            }
        });

        segments = new TreeSet<>() ;

        for(File segmentFile :  segmentFiles ){
            segments.add(buildSegment(segmentFile)) ;
        }

    }

    private FileSegment buildSegment(String  baseDir , long offset  ) {
        return new FileSegment(baseDir , offset  );
    }

    private FileSegment buildSegment(File segmentFile) {
        long sid  = Long.parseLong( segmentFile.getName().split("-")[1] ) ;
        FileSegment segment = new FileSegment(baseDir , sid );
        return segment;
    }



    @Override
    public void write(Message message) throws IOException {

        CRC32 crc32 = new CRC32();
        crc32.update(message.getData());
        ChunkInfo chunkInfo = new ChunkInfo(message.getData().length, (int) crc32.getValue() , (byte) 0);

        rwLock.writeLock().lock();
        try{
            Segment segment = indexCurrentWriteSegment();
            segment.write(message , chunkInfo );
        }finally {
            rwLock.writeLock().unlock();
        }

    }

    private Segment indexCurrentWriteSegment() {
        rwLock.writeLock().lock();
        try{

        }finally {
            rwLock.writeLock().unlock();
        }
        if( segments.size() == 0  ){
            segments.add(buildSegment(baseDir, 0 )) ;
        }
        Segment segment = segments.last() ;
        if(segment.isFull()){
            ((FileSegment)segment).completeWrite();
            segment = buildSegment(baseDir , segment.getCid() + 1 ) ;
            segments.add(segment) ;
        }


        return segment;
    }

    @Override
    public List<Message> query(long startId, int size) throws IOException {

        List<Message> list  = new ArrayList<>() ;

        for(long id  =  startId , end = startId + size ; id < end  ; id ++ ){
            Segment segment = indexSegment(id) ;
            if(segment == null ){
                continue;
            }
            Message message = segment.query(id);
            if(message != null ){
                list.add(message) ;
            }
        }

        return list ;
    }

    private Segment indexSegment(long id) {

        rwLock.readLock().lock();
        try{
            NavigableSet<Segment> segments = this.segments.headSet( Segment.toSegment(id), true);

            if( segments.size() == 0  ){
                return null ;
            }
            return segments.last() ;
        }finally {
            rwLock.readLock().unlock();
        }

    }



    @Override
    public void close()  {

        this.segments.clear();


    }


}
