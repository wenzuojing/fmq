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

    public GetMessageResult getMessage(final String topic, final long sequence,
                                       final int maxNum) {


        long minSequence = 0;
        long maxSequence = 0;

        GetMessageResult getResult = new GetMessageResult();
        getResult.setStatus(GetMessageStatus.NO_MESSAGE);

        IndexQueueManager indexIndexQueueManager = this.serviceManager.getIndexService().findIndexIndexQueueManager(topic);
        if (indexIndexQueueManager != null) {
            minSequence = indexIndexQueueManager.getMinSequence();
            maxSequence = indexIndexQueueManager.getMaxSequence();

            if (maxSequence == 0) {
                getResult.setStatus(GetMessageStatus.NO_MESSAGE);
                getResult.setNextSequence(0);
            } else if (sequence < minSequence) {
                getResult.setStatus(GetMessageStatus.INDEX_TOO_SMALL);
                getResult.setNextSequence(minSequence);
            } else if (sequence > maxSequence) {
                getResult.setStatus(GetMessageStatus.INDEX_TOO_BIG);
                getResult.setNextSequence(maxSequence);
            } else {
                lookupMessages(sequence, maxNum, getResult, indexIndexQueueManager);
            }
        } else {
            getResult.setStatus(GetMessageStatus.NO_MESSAGE);
            getResult.setNextSequence(0);
        }


        getResult.setMaxSequence(maxSequence);
        getResult.setMinSequence(minSequence);
        return getResult;
    }

    private void lookupMessages(long sequence, int maxNum, GetMessageResult getResult, IndexQueueManager indexIndexQueueManager) {
        if (indexIndexQueueManager != null) {
            getResult.setStatus(GetMessageStatus.NOT_FOUND);
            for (int i = 0; i < maxNum; i++) {
                StoreMessagePosition storeMessagePosition = indexIndexQueueManager.indexStoreMessagePosition(sequence++);
                SelectMappedBufferResult selectResult =
                        this.serviceManager.getMessageStoreService().getMessage(storeMessagePosition.getDataQueueOffset(), storeMessagePosition.getMsgSize());
                if (selectResult != null) {
                    getResult.addMessage(selectResult);
                    getResult.setStatus(GetMessageStatus.FOUND);
                } else {
                    break;
                }
            }
            getResult.setNextSequence(sequence);
        } else {
            getResult.setStatus(GetMessageStatus.NOT_FOUND);
            getResult.setNextSequence(sequence++);
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
