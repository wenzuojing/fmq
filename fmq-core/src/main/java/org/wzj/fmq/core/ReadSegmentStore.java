package org.wzj.fmq.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Created by wens on 15-6-11.
 */
public class ReadSegmentStore implements Closeable {

    private static final int MAX_SIZE = 512 * 1024 * 1024 ;

    private ReadMappedFile readMappedFile ;

    private String segmentFilePath ;

    private int segmentIndex ;

    private long readPosition ;



    public ReadSegmentStore( String baseDir , int segmentIndex , int offset ) {

        if(offset < 0 ){
            throw new IllegalArgumentException("offset must be greater then 0.") ;
        }

        this.readPosition = offset ;
        this.segmentIndex = segmentIndex ;
        this.segmentFilePath = baseDir + File.separator + DataStore.DATA_FILE_NAME_PREFIX + segmentIndex ;
        this.readMappedFile = new ReadMappedFile( this.segmentFilePath , offset , 10 * 1024 * 1024 ) ;
    }

    public synchronized byte[]  read() throws IOException {

        byte[] header = new byte[5];
        this.readMappedFile.read(header);

        int dataLength = ((header[1] << 24) + (header[1] << 16) + (header[1] << 8) + (header[1] << 0)) ;

        byte[] data = new byte[dataLength] ;

        this.readMappedFile.read(data);

        return data ;

    }

    public int getSegmentIndex() {
        return segmentIndex;
    }

    public long getReadPosition() {
        return this.readPosition;
    }

    @Override
    public void close() throws IOException {
        readMappedFile.close();
    }
}
