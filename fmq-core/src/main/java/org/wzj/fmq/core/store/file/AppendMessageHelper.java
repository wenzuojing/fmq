package org.wzj.fmq.core.store.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.StoreMessage;

import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-18.
 */
public class AppendMessageHelper {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    // 文件末尾空洞最小定长
    private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
    // 存储消息ID
    private final ByteBuffer msgIdMemory;
    // 存储消息内容
    private final ByteBuffer msgStoreItemMemory;
    // 消息的最大长度
    private final int maxMessageSize;


    public AppendMessageHelper( int maxMessageSize) {
        this.msgIdMemory = ByteBuffer.allocate(MessageId.MSG_ID_LENGTH);
        this.msgStoreItemMemory = ByteBuffer.allocate(maxMessageSize + END_FILE_MIN_BLANK_LENGTH);
        this.maxMessageSize = maxMessageSize;
    }


    public ByteBuffer getMsgStoreItemMemory() {
        return msgStoreItemMemory;
    }


    public AppendMessageResult doAppend(long queueFromOffset , ByteBuffer byteBuffer, int maxBlank, final StoreMessage msg) {

        long wroteOffset = queueFromOffset + byteBuffer.position();
        String msgId = MessageId.createMessageId(this.msgIdMemory, wroteOffset);


        final int totalByteSize = msg.calNeedByteSize();

        if (totalByteSize > this.maxMessageSize) {
            log.warn("message size exceeded, msg total size: " + totalByteSize + ", msg body size: "
                    + msg.getBodyByteSize() + ", maxMessageSize: " + this.maxMessageSize);
            return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
        }

        if ((totalByteSize + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
            this.resetMsgStoreItemMemory(maxBlank);
            this.msgStoreItemMemory.putInt(maxBlank);
            this.msgStoreItemMemory.putInt(StoreMessage.BLANK_MAGIC);

            byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
            return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId,
                    msg.getCreateTimestamp());
        }

        this.resetMsgStoreItemMemory(totalByteSize);

        msg.encode(this.msgStoreItemMemory);

        byteBuffer.put(this.msgStoreItemMemory.array(), 0, totalByteSize);

        AppendMessageResult result =
                new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalByteSize, msgId,
                        msg.getCreateTimestamp());

        return result;
    }


    private void resetMsgStoreItemMemory(final int length) {
        this.msgStoreItemMemory.flip();
        this.msgStoreItemMemory.limit(length);
    }
}
