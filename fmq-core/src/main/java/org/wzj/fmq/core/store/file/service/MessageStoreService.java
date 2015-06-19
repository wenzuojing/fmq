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
package org.wzj.fmq.core.store.file.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.common.ServiceThread;
import org.wzj.fmq.core.store.PutMessageResult;
import org.wzj.fmq.core.store.PutMessageStatus;
import org.wzj.fmq.core.store.StoreMessage;
import org.wzj.fmq.core.store.file.*;
import org.wzj.fmq.core.store.file.queue.DataQueue;
import org.wzj.fmq.core.store.file.queue.DataQueueManager;
import org.wzj.fmq.core.util.Utils;


/**
 * CommitLog实现
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class MessageStoreService implements Lifecycle {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);
    private DataQueueManager dataQueueManager;

    private ServiceManager serviceManager;

    /**
     * 构造函数
     */
    public MessageStoreService(ServiceManager serviceManager) {
        this.serviceManager = serviceManager ;
        this.dataQueueManager =
                new DataQueueManager(this.serviceManager.getMessageStoreConfig().getStorePathData(),
                        this.serviceManager.getMessageStoreConfig().getDataQueueFileSize(), this.serviceManager.getMessageStoreConfig().getMaxMessageSize());

    }

    public int deleteExpiredFile(long fileReservedTime, int deletePhysicFilesInterval, int destroyMapedFileIntervalForcibly, boolean cleanAtOnce) {
        return 0;
    }

    public boolean retryDeleteFirstFile(int destroyMapedFileIntervalForcibly) {
        return false;
    }

    @Override
    public void init() {


    }

    @Override
    public void start()  {

    }

    @Override
    public void shutdown() {

    }


    public long getMinOffset() {
        return dataQueueManager.getMinOffset();
    }


    public long getMaxOffset() {
        return this.dataQueueManager.getMaxOffset();
    }


    public void recoverNormally() {
        this.recoverNormally();
    }


    public void recoverAbnormally() {
        this.dataQueueManager.recoverAbnormally();
    }

    public PutMessageResult putMessage(final StoreMessage msg) {
        msg.setCheckSum(Utils.crc32(msg.getBody()));
        AppendMessageResult result = null;
        // 写文件要加锁
        synchronized (this) {
            long beginLockTimestamp = System.currentTimeMillis();
            msg.setCreateTimestamp(beginLockTimestamp);

            DataQueue dataQueue = this.dataQueueManager.getLastQueue();
            if (null == dataQueue) {
                dataQueue = this.dataQueueManager.createNewQueue();
            }
            result = dataQueue.appendMessage(msg );
            switch (result.getStatus()) {
                // 成功追加消息
                case PUT_OK:
                    break;
                // 走到文件末尾
                case END_OF_FILE:
                    // 创建新文件，重新写消息
                    dataQueue = this.dataQueueManager.getLastQueue();
                    if (null == dataQueue) {
                        log.error("create maped file2 error, topic: " + msg.getTopic());
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = dataQueue.appendMessage(msg );
                    break;
                // 消息大小超限
                case MESSAGE_SIZE_EXCEEDED:
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                // 未知错误
                case UNKNOWN_ERROR:
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            StoreMessagePosition storeMessagePosition = new StoreMessagePosition(msg.getTopic(), result.getWroteOffset(), result.getWroteBytes(), result.getCreateTimestamp() );

            this.serviceManager.getDispatchMessageService().putDispatchRequest(storeMessagePosition);

            long eclipseTime = Utils.elapseTimeMilliseconds(beginLockTimestamp);
            if (eclipseTime > 1000) {
                log.warn("putMessage in lock eclipse time(ms) " + eclipseTime);
            }
        }

        // 返回结果
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 向发送方返回结果
        return putMessageResult;
    }


    public long readCreateTimestamp(long offset, int size) {
        SelectMappedBufferResult result = this.getMessage(offset, size);
        if (null != result) {
            return result.getByteBuffer().getLong(StoreMessage.POSITION_CREATE_TIMESTAMP);
        }
        return -1;
    }


    /**
     * 读取消息
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mapedFileSize = this.serviceManager.getMessageStoreConfig().getDataQueueFileSize();
        DataQueue dataQueue = this.dataQueueManager.findQueueByOffset(offset);
        if (dataQueue != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMappedBufferResult result = dataQueue.selectMappedBuffer(pos, size);
            return result;
        }

        return null;
    }



}
