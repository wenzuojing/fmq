package org.wzj.fmq.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by wens on 15-6-11.
 */
public class DataStore implements Cloneable {

    private static final String FILE_PREFIX  = "data-" ;

    private MetaStore metaStore;

    private String dataDir ;

    private SegmentStore  currentSegmentStore ;


    public DataStore(MetaStore metaStore , String dir ){
        this.metaStore = metaStore ;
        this.dataDir = dir ;
        openCurrentStore() ;

    }

    private void openCurrentStore() {

        Meta meta = metaStore.getMeta();

        currentSegmentStore = new SegmentStore( this.dataDir + File.separator + FILE_PREFIX + meta.getDataStroeSegment() , meta.getDataStroeOffset() ) ;


    }


    static class SegmentStore {
        private WriteMappedFile writeMappedFile ;
        public SegmentStore( String filePath , int offset ) {

            writeMappedFile = new WriteMappedFile( filePath , offset , 10 * 1024 * 1024, 512 * 1024  ) ;

        }
    }
}
