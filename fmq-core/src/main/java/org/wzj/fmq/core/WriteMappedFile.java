package org.wzj.fmq.core;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by wens on 15-6-11.
 */
public class WriteMappedFile extends AbstractMappedFile  {

    private long maxFlushDataSize;
    private long waterLevel;

    public WriteMappedFile(String  filePath, long mappedOffset, int bufferSize, long maxFlushDataSize) {
        super(filePath, mappedOffset, bufferSize);
        this.maxFlushDataSize = maxFlushDataSize;
    }

    public synchronized void write(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return;
        }
        adjustMappedBufferIfNeed(bytes.length);
        write_0(bytes);
    }


    protected synchronized void release() {
        for (int i = 0; i <= bufferIndex; i++) {
            MappedByteBuffer byteBuffer = byteBuffers[i];

            if( i == bufferIndex && waterLevel > 0 ){
                flush(byteBuffer);
            }

            unmap(byteBuffer);
        }

        byteBuffers = null;
    }


    private void write_0(byte[] bytes) {

        int offset = 0;
        while (true) {
            MappedByteBuffer byteBuffer = byteBuffers[bufferIndex];
            int bSize = byteBuffer.capacity() - byteBuffer.position();
            bSize = Math.min(bSize, bytes.length - offset);
            byteBuffer.put(bytes, offset, bSize);
            offset += bSize;
            freeBufferSize -= bSize;
            waterLevel += bSize;

            boolean flush = false;

            if (waterLevel >= maxFlushDataSize) {
                flush = true;
                flush(byteBuffer);
            }

            if (offset == bytes.length) {
                break;
            }

            bufferIndex++;
            if (!flush) {
                flush(byteBuffer);
            }

        }

        if (bufferIndex > 0) {
            releaseMappedBuffer();
        }

    }

    private void flush(MappedByteBuffer byteBuffer) {
        waterLevel = 0;
        byteBuffer.force();
    }


}
