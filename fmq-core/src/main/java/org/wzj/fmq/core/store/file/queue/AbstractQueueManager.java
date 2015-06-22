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
package org.wzj.fmq.core.store.file.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.Lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by wens on 15-6-18.
 */
public abstract class AbstractQueueManager<T extends FileQueue> implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    protected final String storePath;
    protected final int mappedFileSize;

    protected final List<T> loadedQueues = new ArrayList<>(10);

    protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    protected long committedWhere = 0;
    protected volatile long storeTimestamp = 0;


    public AbstractQueueManager(final String storePath, int mapedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mapedFileSize;
    }





    public void truncateDirtyFiles(long offset) {
        List<T> willRemoveFileQueues = new ArrayList<T>();

        for (T queue : this.loadedQueues) {
            long fileTailOffset = queue.getFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= queue.getFromOffset()) {
                    queue.setWritePosition((int) (offset % this.mappedFileSize));
                    queue.setCommittedPosition((int) (offset % this.mappedFileSize));
                } else {
                    queue.delete();
                    willRemoveFileQueues.add(queue);
                }
            }
        }
    }


    public T getFirstQueue() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.loadedQueues.isEmpty()) {
                return this.loadedQueues.get(0);
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        return null;
    }


    public T getLastQueue() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.loadedQueues.isEmpty()) {
                return this.loadedQueues.get(this.loadedQueues.size() - 1);
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }


    public long getMinOffset() {
        T firstQueue = getFirstQueue();
        if (firstQueue != null) {
            return firstQueue.getFromOffset();
        }
        return -1;
    }


    public long getMaxOffset() {
        T lastQueue = getLastQueue();
        if (lastQueue != null) {
            return lastQueue.getFromOffset() + lastQueue.getWritePosition();
        }
        return 0;
    }


    public void commit() {

        try {
            this.readWriteLock.readLock().lock();
            for (int i = this.loadedQueues.size() - 1; i >= 0; i--) {
                T queue = this.loadedQueues.get(i);
                if (queue.isDirty()) {
                    queue.commit();
                } else {
                    break;
                }
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }

    }


    public T findQueueByOffset(long offset) {
        try {
            this.readWriteLock.readLock().lock();
            T queue = this.getFirstQueue();

            if (queue != null) {
                int index = (int) ((offset / this.mappedFileSize) - (queue.getFromOffset() / this.mappedFileSize));
                if (index < 0 || index >= this.loadedQueues.size()) {
                    log.warn("out of index , request Offset: " + offset
                            + ", index: " + index + ", mappedFileSize: " + this.mappedFileSize
                            + ", loadedQueues count: " + this.loadedQueues.size());
                }
                return this.loadedQueues.get(index);
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }


    public long getCommittedWhere() {
        return committedWhere;
    }


    public void setCommittedWhere(long committedWhere) {
        this.committedWhere = committedWhere;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public List<T> getLoadedQueues() {
        return Collections.unmodifiableList(this.loadedQueues);
    }


    public int getMappedFileSize() {
        return mappedFileSize;
    }

    @Override
    public void init() {

    }

    @Override
    public void start()  {

    }

    @Override
    public void shutdown() {
        try {
            this.readWriteLock.readLock().lock();
            for (int i = this.loadedQueues.size() - 1; i >= 0; i--) {
                T queue = this.loadedQueues.get(i);
                queue.shutdown();
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }

    }


    public abstract long recoverNormally(long fromOffset);


    public abstract T createNewQueue();


}
