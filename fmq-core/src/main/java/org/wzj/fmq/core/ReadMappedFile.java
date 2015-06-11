package org.wzj.fmq.core;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by wens on 15-6-11.
 */
public class ReadMappedFile extends AbstractMappedFile {

    public ReadMappedFile(String  filePath, long mappedOffset, int bufferSize) {
        super(filePath, mappedOffset, bufferSize);
    }

    public synchronized void read(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return;
        }
        adjustMappedBufferIfNeed(bytes.length);
        read_0(bytes);
    }


    @Override
    protected synchronized void release() {
        for (int i = 0; i <= bufferIndex; i++) {
            MappedByteBuffer byteBuffer = byteBuffers[i];
            unmap(byteBuffer);
        }

        byteBuffers = null;
    }


    private void read_0(byte[] bytes) {

        int offset = 0;

        while (true) {
            MappedByteBuffer byteBuffer = byteBuffers[bufferIndex];
            int bSize = byteBuffer.capacity() - byteBuffer.position();
            bSize = Math.min(bSize, bytes.length - offset);
            byteBuffer.get(bytes, offset, bSize);
            offset += bSize;
            freeBufferSize -= bSize;

            if (offset == bytes.length) {
                break;
            }

            bufferIndex++;
        }

        if (bufferIndex > 0) {
            releaseMappedBuffer();
        }
    }


}
