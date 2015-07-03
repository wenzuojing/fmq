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
import org.wzj.fmq.core.store.*;
import org.wzj.fmq.core.store.file.service.ServiceManager;
import org.wzj.fmq.core.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by wens on 15-6-18.
 */
public class FileMessageStore implements MessageStore {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    // 存储服务是否启动
    private volatile boolean shutdown = true;

    // 权限控制后，打印间隔次数
    private AtomicLong printTimes = new AtomicLong(0);

    private ServiceManager serviceManager;


    public FileMessageStore(final MessageStoreConfig messageStoreConfig) throws IOException {
        this.serviceManager = new ServiceManager(messageStoreConfig);
    }


    @Override
    public void init() {

        try {
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown " + (lastExitOK ? "normally" : "abnormally"));
            this.serviceManager.getMessageStoreService().init();
            this.serviceManager.getIndexService().init();
            this.recover(lastExitOK);
        } catch (Exception e) {
            log.error("init exception", e);
        }

    }


    public void start()  {
        this.serviceManager.start();
        try {
            this.createTempFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.shutdown = false;
    }


    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;
            this.serviceManager.shutdown();

            this.deleteFile(getMessageStoreConfig().getAbortFile());
        }
    }


    public PutMessageResult putMessage(StoreMessage msg) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!this.serviceManager.getRunningFlagsService().isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden "
                        + this.serviceManager.getRunningFlagsService().getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        long beginTime = System.currentTimeMillis();

        PutMessageResult result = this.serviceManager.getMessageStoreService().putMessage(msg);
        long eclipseTime = Utils.elapseTimeMilliseconds(beginTime);
        if (eclipseTime > 1000) {
            log.warn("putMessage not in lock eclipse time(ms) " + eclipseTime);
        }
        return result;
    }


    public GetMessageResult getMessage(final String topic, final long index,
                                       final int maxNum) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.serviceManager.getRunningFlagsService().isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden "
                    + this.serviceManager.getRunningFlagsService().getFlagBits());
            return null;
        }
        return this.serviceManager.getMessageQueryService().getMessage(topic, index, maxNum);
    }


    public long getMaxSequence(String topic) {
        return this.serviceManager.getIndexService().getMaxSequence(topic);
    }


    public long getMinSequence(String topic) {
        return this.serviceManager.getIndexService().getMinSequence(topic);
    }


    public long getSequenceByTime(String topic, long timestamp) {
        return this.serviceManager.getIndexService().getSequenceByTime(topic, timestamp);
    }


    @Override
    public long getMessageStoreTimestamp(String topic, long index) {
        return this.serviceManager.getMessageQueryService().getMessageStoreTimestamp(topic, index);
    }


    @Override
    public long getTotalMessageNum(String topic) {
        return this.serviceManager.getIndexService().getTotalMessageNum(topic);
    }


    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }


    private void createTempFile() throws IOException {
        String fileName = getMessageStoreConfig().getAbortFile();
        File file = new File(fileName);
        Utils.createDirIfNotExist(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }


    private boolean isTempFileExist() {
        String fileName = getMessageStoreConfig().getAbortFile();
        File file = new File(fileName);
        return file.exists();
    }


    private MessageStoreConfig getMessageStoreConfig() {
        return this.serviceManager.getMessageStoreConfig();
    }


    private void recover(final boolean lastExitOK) {

        long dataQueueCheckpoint = this.serviceManager.getCheckpointService().getDataQueueCheckpoint();
        long indexQueueCheckpoint = this.serviceManager.getCheckpointService().getIndexQueueCheckpoint();
        long endOffset = this.serviceManager.getMessageStoreService().recoverNormally(dataQueueCheckpoint);

        if(indexQueueCheckpoint < endOffset ){
            this.serviceManager.getMessageStoreService().redispatchMessage( indexQueueCheckpoint , endOffset ) ;
        }else if( indexQueueCheckpoint > endOffset ){
            //this.serviceManager.getIndexService().deleteIndex() ;
        }



    }

}
