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


import org.wzj.fmq.core.store.file.queue.IndexQueue;
import org.wzj.fmq.core.store.file.queue.IndexQueueManager;

import java.io.File;


/**
 * Created by wens on 15-6-18.
 */
public class MessageStoreConfig {


    private String storePathData = System.getProperty("user.home") + File.separator + "store" + File.separator + "data";
    private String storePathIndex = System.getProperty("user.home") + File.separator + "store" + File.separator + "index";
    private String abortFile = System.getProperty("user.home") + File.separator + "store" + File.separator  + "abort";
    private int dataQueueFileSize = 1024 * 1024 * 1024;
    private int indexQueueFileSize = 300000 * IndexQueue.INDEX_UNIT_SIZE ;
    private int flushInterval = 1000;
    private int cleanResourceInterval = 10000;
    private int putMsgIndexHighWater = 600000;
    private int maxMessageSize = 1024 * 512;

    private String checkpointFile = System.getProperty("user.home") + File.separator + "store/.checkpoint" ;


    public String getStorePathData() {
        return storePathData;
    }

    public void setStorePathData(String storePathData) {
        this.storePathData = storePathData;
    }

    public String getStorePathIndex() {
        return storePathIndex;
    }

    public void setStorePathIndex(String storePathIndex) {
        this.storePathIndex = storePathIndex;
    }

    public String getAbortFile() {
        return abortFile;
    }

    public void setAbortFile(String abortFile) {
        this.abortFile = abortFile;
    }

    public int getDataQueueFileSize() {
        return dataQueueFileSize;
    }

    public void setDataQueueFileSize(int dataQueueFileSize) {
        this.dataQueueFileSize = dataQueueFileSize;
    }

    public int getIndexQueueFileSize() {
        return indexQueueFileSize / IndexQueue.INDEX_UNIT_SIZE * IndexQueue.INDEX_UNIT_SIZE ;
    }

    public void setIndexQueueFileSize(int indexQueueFileSize) {
        this.indexQueueFileSize = indexQueueFileSize;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }

    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }

    public int getPutMsgIndexHighWater() {
        return putMsgIndexHighWater;
    }

    public void setPutMsgIndexHighWater(int putMsgIndexHighWater) {
        this.putMsgIndexHighWater = putMsgIndexHighWater;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public String getCheckpointFile() {
        return this.checkpointFile ;
    }
}
