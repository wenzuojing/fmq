package org.wzj.fmq.core;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

/**
 * Created by wens on 15-6-12.
 */
public class FileDataStore implements DataStore {



    private String baseDir ;

    private List<Segment> segments ;

    private ReadWriteLock rwLock  = new ReentrantReadWriteLock() ;


    public FileDataStore(String baseDir ){
        loadSegment(baseDir) ;
    }

    private void loadSegment(String baseDir) {

        File dir = new File(baseDir);

        if( !dir.exists() ){
            dir.mkdirs() ;
        }

        if(!dir.isDirectory()){
            throw new IllegalArgumentException("baseDir is not a directory:"+baseDir) ;
        }


        File[] segmentFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(DATA_FILE_NAME_PREFFIX);
            }
        });

        segments = new ArrayList<>(segmentFiles.length + 5 ) ;

        for(File segmentFile :  segmentFiles ){
            segments.add(buildSegment(segmentFile)) ;
        }

        Collections.sort(segments);

    }

    private Segment buildSegment(String  baseDir , long offset  ) {
        return new Segment(offset , baseDir , DATA_FILE_NAME_PREFFIX + offset  );
    }

    private Segment buildSegment(File segmentFile) {
        long sid  = Long.parseLong( segmentFile.getName().split("-")[1] ) ;
        Segment segment = new Segment(sid, segmentFile.getParent(), segmentFile.getName());
        return segment;
    }

    private SegmentIndex.IndexItem buidIndexItem(byte[] data ){
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        SegmentIndex.IndexItem indexItem =  new SegmentIndex.IndexItem(crc32.getValue(), data.length , (byte)0 );
        return indexItem;
    }

    @Override
    public void write(Message message) throws IOException {

        SegmentIndex.IndexItem indexItem = buidIndexItem(message.getData());
        rwLock.writeLock().lock(); ;
        try{
            Segment curr = indexCurrentWriteSegment();
            curr.write( indexItem , message ) ;
        }finally {
            rwLock.writeLock().unlock();
        }

    }

    private Segment indexCurrentWriteSegment() {
        if( segments.size() == 0  ){
            segments.add(buildSegment(baseDir, 0 )) ;
        }
        Segment segment = segments.get(segments.size() - 1);
        if(segment.isFull()){
            segment = buildSegment(baseDir , segment.getSid() + segment.getIndex().getSize()) ;
        }

        return segment;
    }

    @Override
    public List<Message> query(long startId, int size) {
        return null;
    }
}
