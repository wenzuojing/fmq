package org.wzj.fmq.core.store.file.service;

import org.wzj.fmq.core.store.GetMessageResult;
import org.wzj.fmq.core.store.GetMessageStatus;
import org.wzj.fmq.core.store.Lifecycle;
import org.wzj.fmq.core.store.file.SelectMappedBufferResult;
import org.wzj.fmq.core.store.file.StoreMessagePosition;
import org.wzj.fmq.core.store.file.queue.IndexQueueManager;

/**
 * Created by wens on 15-6-18.
 */
public class MessageQueryService implements Lifecycle {

    private ServiceManager serviceManager;

    public MessageQueryService(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    public long getMessageStoreTimestamp(String topic, long index) {
        IndexQueueManager indexIndexQueueManager = this.serviceManager.getIndexService().findIndexIndexQueueManager(topic);
        if (indexIndexQueueManager != null) {
            StoreMessagePosition result = indexIndexQueueManager.indexStoreMessagePosition(index);
            if (result != null) {
                long createTimestamp = this.serviceManager.getMessageStoreService().readCreateTimestamp(result.getDataQueueOffset(), result.getMsgSize());
                return createTimestamp;
            }
        }

        return -1;
    }

    public GetMessageResult getMessage(final String topic, final long index,
                                       final int maxNum) {


        long minIndex = 0;
        long maxIndex = 0;

        GetMessageResult getResult = new GetMessageResult();
        getResult.setStatus(GetMessageStatus.NO_MESSAGE);

        IndexQueueManager indexIndexQueueManager = this.serviceManager.getIndexService().findIndexIndexQueueManager(topic);
        if (indexIndexQueueManager != null) {
            minIndex = indexIndexQueueManager.getMinIndex();
            maxIndex = indexIndexQueueManager.getMaxIndex();

            if (maxIndex == 0) {
                getResult.setStatus(GetMessageStatus.NO_MESSAGE);
                getResult.setNextIndex(0);
            } else if (index < minIndex) {
                getResult.setStatus(GetMessageStatus.INDEX_TOO_SMALL);
                getResult.setNextIndex(minIndex);
            } else if (index > maxIndex) {
                getResult.setStatus(GetMessageStatus.INDEX_TOO_BIG);
                getResult.setNextIndex(maxIndex);
            } else {
                lookupMessages(index, maxNum, getResult, indexIndexQueueManager);
            }
        } else {
            getResult.setStatus(GetMessageStatus.NO_MESSAGE);
            getResult.setNextIndex(0);
        }


        getResult.setMaxIndex(maxIndex);
        getResult.setMinIndex(minIndex);
        return getResult;
    }

    private void lookupMessages(long index, int maxNum, GetMessageResult getResult, IndexQueueManager indexIndexQueueManager) {
        if (indexIndexQueueManager != null) {
            getResult.setStatus(GetMessageStatus.NOT_FOUND);
            for (int i = 0; i < maxNum; i++) {
                StoreMessagePosition storeMessagePosition = indexIndexQueueManager.indexStoreMessagePosition(index++);
                SelectMappedBufferResult selectResult =
                        this.serviceManager.getMessageStoreService().getMessage(storeMessagePosition.getDataQueueOffset(), storeMessagePosition.getMsgSize());
                if (selectResult != null) {
                    getResult.addMessage(selectResult);
                    getResult.setStatus(GetMessageStatus.FOUND);
                } else {
                    break;
                }
            }
            getResult.setNextIndex(index);
        } else {
            getResult.setStatus(GetMessageStatus.NOT_FOUND);
            getResult.setNextIndex(index++);
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }
}
