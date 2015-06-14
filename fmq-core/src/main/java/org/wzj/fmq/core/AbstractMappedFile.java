package org.wzj.fmq.core;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;

/**
 * Created by wens on 15-6-11.
 */
public abstract class AbstractMappedFile implements Closeable {


    protected FileChannel fileChannel;
    protected RandomAccessFile randomAccessFile ;
    protected int bufferSize;

    protected MappedByteBuffer[] byteBuffers;
    protected int bufferIndex = 0;
    protected int freeBufferSize = 0;
    protected long mappedOffset = 0;

    public AbstractMappedFile(String filePath, long mappedOffset, int bufferSize) {

        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be greater than 0.");
        }


        if (filePath == null) {
            throw new IllegalArgumentException("filePath must not be null.");
        }

        this.bufferSize = bufferSize;

        try {
            randomAccessFile = new RandomAccessFile(filePath , "rw") ;
            fileChannel = randomAccessFile.getChannel() ;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        this.mappedOffset = mappedOffset;

    }


    protected void adjustMappedBufferIfNeed(int need) throws IOException {

        if (byteBuffers == null) {
            byteBuffers = new MappedByteBuffer[0];
        }

        while (freeBufferSize < need) {
            byteBuffers = Arrays.copyOf(byteBuffers, byteBuffers.length + 1);
            MappedByteBuffer mmap = mmap(mappedOffset);
            byteBuffers[byteBuffers.length - 1] = mmap;
            freeBufferSize += bufferSize;
            mappedOffset += bufferSize;
        }
    }


    protected void releaseMappedBuffer() {

        for (int i = 0; i < bufferIndex; i++) {
            MappedByteBuffer byteBuffer = byteBuffers[i];
            unmap(byteBuffer);
        }

        MappedByteBuffer[] newByteBuffers = new MappedByteBuffer[byteBuffers.length - bufferIndex];
        System.arraycopy(byteBuffers, bufferIndex, newByteBuffers, 0, newByteBuffers.length);
        byteBuffers = newByteBuffers;
        bufferIndex = 0;


    }

    protected MappedByteBuffer mmap(long position) throws IOException {
        return this.fileChannel.map(FileChannel.MapMode.READ_WRITE, position, bufferSize);
    }

    protected void unmap(final MappedByteBuffer byteBuffer) {

        if (byteBuffer != null) {

            AccessController.doPrivileged(new PrivilegedAction<Object>() {

                @SuppressWarnings("restriction")
                public Object run() {
                    try {
                        Method e = byteBuffer.getClass().getMethod("cleaner", new Class[0]);
                        if (e != null) {
                            e.setAccessible(true);
                            Object cleaner = e.invoke(byteBuffer, new Object[0]);
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
    }

    @Override
    public void close() throws IOException {
        release() ;
        this.fileChannel.close();
        this.randomAccessFile.close();

    }

    protected abstract void release();
}
