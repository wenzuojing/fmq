package org.wzj.fmq.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Created by wens on 15-6-11.
 */
public class WriteSegmentStore implements Closeable{

    private static final int MAX_SIZE = 512 * 1024 * 1024 ;

    private WriteMappedFile writeMappedFile ;

    private String segmentFilePath ;
    
    private int segmentIndex ;

    private long writePosition ;

    public WriteSegmentStore( String baseDir , int segmentIndex , int offset ) {
        if(offset < 0 ){
            throw new IllegalArgumentException("offset must be greater then 0.") ;
        }
        this.writePosition = offset ;
        this.segmentIndex = segmentIndex ;
        this.segmentFilePath = baseDir + File.separator + DataStore.DATA_FILE_NAME_PREFIX + segmentIndex ;
        this.writeMappedFile = new WriteMappedFile( this.segmentFilePath , offset , 10 * 1024 * 1024, 512 * 1024  ) ;
    }

    public synchronized void appendWrite(byte[] bytes ) throws IOException {

        if(bytes == null || bytes.length == 0 ){
            return ;
        }

        if(this.writePosition + bytes.length > MAX_SIZE  ){
            throw new SegmentStoreFullException("The size of store file is full : "+ segmentFilePath ) ;
        }

        writeMappedFile.write(bytes);
        this.writePosition += bytes.length ;

    }

    public int getSegmentIndex() {
        return segmentIndex;
    }

    public long getWritePosition() {
        return this.writePosition ;
    }

    @Override
    public void close() throws IOException {
        writeMappedFile.close();
    }


}
