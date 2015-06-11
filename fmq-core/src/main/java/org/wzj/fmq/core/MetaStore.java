package org.wzj.fmq.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by wens on 15-6-11.
 */
public class MetaStore implements Closeable {

    private static final String FILE_NAME = ".meta" ;

    private static final int META_SIZE  = 8 ;

    private RandomAccessFile randomAccessFile ;

    private MappedByteBuffer mappedByteBuffer ;


    private Meta meta ;

    private ScheduledExecutorService scheduledExecutorService ;

    public MetaStore(String dir){

        File file  = new File(dir , FILE_NAME ) ;

        if( !file.exists() ){
            try {
                file.createNewFile() ;
            } catch (IOException e) {
                throw new RuntimeException(e) ;
            }
        }

        try{
            randomAccessFile = new RandomAccessFile(file , "rw") ;
            mappedByteBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE , 0 , META_SIZE ) ;
        }catch (IOException e){
            throw new RuntimeException(e) ;
        }

        reloadMeta() ;

        scheduledExecutorService = Executors.newScheduledThreadPool(1, Threads.make("bg-flush-meta-data"));

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();

            }
        }, 1, 1, TimeUnit.SECONDS);

    }

    private synchronized void flush() {
        mappedByteBuffer.force();
    }

    private synchronized void reloadMeta() {
        mappedByteBuffer.flip() ;
        meta = new Meta(mappedByteBuffer.getInt() , mappedByteBuffer.getInt()) ;
    }

    public synchronized void updateMeta(int dataStroeSegment , int dataStroeOffset ){
        mappedByteBuffer.flip() ;
        mappedByteBuffer.putInt(dataStroeSegment) ;
        mappedByteBuffer.putInt(dataStroeOffset) ;

        meta.setDataStroeSegment(dataStroeSegment);
        meta.setDataStroeOffset(dataStroeOffset);
    }

    public Meta getMeta() {
        return new Meta(this.meta.getDataStroeSegment() , this.meta.getDataStroeOffset());
    }

    @Override
    public synchronized void close() throws IOException {
        scheduledExecutorService.shutdown();
        if(mappedByteBuffer != null ){
            flush();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {

                @SuppressWarnings("restriction")
                public Object run() {
                    try {
                        Method e = mappedByteBuffer.getClass().getMethod("cleaner", new Class[0]);
                        if (e != null) {
                            e.setAccessible(true);
                            Object cleaner = e.invoke(mappedByteBuffer, new Object[0]);
                            if (cleaner != null) {
                                Method clearMethod = cleaner.getClass().getMethod("clean", new Class[0]);
                                if (e != null) {
                                    clearMethod.invoke(cleaner, new Object[0]);
                                }
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        }

        if(randomAccessFile != null ){
            randomAccessFile.close();
        }
    }

}
