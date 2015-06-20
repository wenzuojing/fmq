/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wzj.fmq.core.store.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.util.Utils;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by wens on 15-6-18.
 */
public class DefaultMappedFileImpl implements MappedFile {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private static final AtomicLong TOTAL_MAPPED_VISUAL_MEMORY = new AtomicLong(0);
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    private static Field CLEANER_FIELD;

    static {
        try {
            ByteBuffer direct = ByteBuffer.allocateDirect(1);
            CLEANER_FIELD = direct.getClass().getDeclaredField("cleaner");
            CLEANER_FIELD.setAccessible(true);
            Cleaner addressField = (Cleaner) CLEANER_FIELD.get(direct);
            addressField.clean();
        } catch (Throwable var12) {
            CLEANER_FIELD = null;
        }
    }

    private final String fileName;
    private final int fileSize;
    private final File file;
    private final MappedByteBuffer mappedByteBuffer;
    private final AtomicInteger writePosition = new AtomicInteger(0);
    private final AtomicInteger flushPosition = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private FileChannel fileChannel;

    public DefaultMappedFileImpl(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        boolean ok = false;
        Utils.createDirIfNotExist(this.file.getParent());
        closed.set(false);
        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            TOTAL_MAPPED_VISUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                closed.set(true);
                this.fileChannel.close();
            }
        }
    }

    @Override
    public ByteBuffer getByteBuffer(int position) {
        ByteBuffer duplicate = mappedByteBuffer.duplicate();
        duplicate.position(position) ;
        return duplicate;
    }

    @Override
    public ByteBuffer getByteBuffer(int position, int size) {
        ByteBuffer duplicate = mappedByteBuffer.duplicate();
        duplicate.position(position) ;
        duplicate.limit(position + size);
        return duplicate;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return mappedByteBuffer.duplicate();
    }

    @Override
    public int getWritePosition() {
        return writePosition.get();
    }

    public void setWritePosition(int position) {
        this.writePosition.set(position);
    }

    @Override
    public int getFlushPosition() {
        return flushPosition.get();
    }

    @Override
    public File getFile() {
        return this.file ;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void flush() {
        int value = this.writePosition.get();
        this.mappedByteBuffer.force();
        this.flushPosition.set(value);
    }

    public boolean isFull() {
        return this.fileSize == this.writePosition.get();
    }

    public void close() throws IOException {
        closed.set(true);
        unmap(mappedByteBuffer);
        TOTAL_MAPPED_VISUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file  OK");
        this.fileChannel.close();
    }

    @Override
    public boolean isClose() {
        return closed.get();
    }

    @Override
    public void delete() {
        try {
            close();
        } catch (IOException e) {
            log.warn("close mapped file fail , file :{}" , fileName );
        }

        file.delete() ;
    }

    @Override
    public boolean isDirty() {
        return this.writePosition.get() - this.flushPosition.get() > 0 ;
    }

    private void unmap(ByteBuffer buffer) {

        if (buffer == null) {
            return;
        }

        if (CLEANER_FIELD != null) {
            try {
                Cleaner cleaner = (Cleaner) CLEANER_FIELD.get(buffer);
                cleaner.clean();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }


    }


}
