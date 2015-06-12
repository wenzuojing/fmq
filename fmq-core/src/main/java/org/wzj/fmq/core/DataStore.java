package org.wzj.fmq.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wens on 15-6-11.
 */
public class DataStore implements Cloneable {

    public static final String DATA_FILE_NAME_PREFIX  = "data-" ;

    private static byte FLAG_CRC32 = 1 << 0;//  crc32
    private static byte FLAG_COMP = 1 << 1;// compression

    private MetaStore metaStore;

    private String dataDir ;

    private WriteSegmentStore  currentWriteSegmentStore ;

    private ConcurrentHashMap<String,ReadSegmentStore> readMappedFileMap = new ConcurrentHashMap<>() ;


    public DataStore(MetaStore metaStore , String dir  ){
        this.metaStore = metaStore ;
        this.dataDir = dir ;
        openCurrentStore() ;

    }

    public void write(byte[] data) throws IOException {

        int dataLength = data.length ;

        //flag + dataLength + data
        byte[] bytes  = new byte[1 + 4 + dataLength];

        bytes[0]= 0 ;
        bytes[1] = (byte)( (dataLength >> 24) & 0xFF );
        bytes[2] = (byte)( (dataLength >> 16) & 0xFF );
        bytes[3] = (byte)( (dataLength >> 8) & 0xFF );
        bytes[4] = (byte)( (dataLength >> 0) & 0xFF );
        System.arraycopy(data, 0, bytes, 5, dataLength);

        synchronized (this){
            try{
                currentWriteSegmentStore.appendWrite(bytes);
            }catch (SegmentStoreFullException e){
                nextWriteSegmentStore() ;
                currentWriteSegmentStore.appendWrite(bytes);
            }

            metaStore.updateMeta(currentWriteSegmentStore.getSegmentIndex(),(int)currentWriteSegmentStore.getWritePosition() - bytes.length );
        }





    }

    public byte[] read(String group ) throws IOException {

        String g = group.intern();

        synchronized (g){

            ReadSegmentStore readSegmentStore = readMappedFileMap.get(g);

            if(readSegmentStore == null ){
                readMappedFileMap.put(g , null ) ;
            }

            byte[] data = readSegmentStore.read();

            return data ;
        }

    }

    private void openCurrentStore() {
        Meta meta = metaStore.getMeta();
        currentWriteSegmentStore = new WriteSegmentStore( this.dataDir, meta.getDataStroeSegmentIndex() , meta.getDataStroeOffset() ) ;
    }

    private  void nextWriteSegmentStore(){
        WriteSegmentStore writeSegmentStore = new WriteSegmentStore( this.dataDir, currentWriteSegmentStore.getSegmentIndex() + 1  , 0 ) ;
        try {
            currentWriteSegmentStore.close();
        } catch (IOException e) {
            throw new RuntimeException("Close write segment store fail." , e ) ;
        }
        currentWriteSegmentStore = writeSegmentStore ;
    }


}
