package org.wzj.fmq.core.store.file.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wzj.fmq.core.common.Constant;
import org.wzj.fmq.core.store.StoreMessage;
import org.wzj.fmq.core.store.file.AppendMessageHelper;
import org.wzj.fmq.core.store.file.AppendMessageResult;
import org.wzj.fmq.core.store.file.SelectMappedBufferResult;
import org.wzj.fmq.core.util.Utils;

import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-18.
 */
public class DataQueue extends AbstractQueue {

    private static final Logger log = LoggerFactory.getLogger(Constant.STORE_LOG_NAME);

    private AppendMessageHelper appendMessageHelper ;

    public DataQueue(String filePath, int mappedFileSize , int maxMessageSize ) {
        super(filePath, mappedFileSize);
        appendMessageHelper = new AppendMessageHelper(maxMessageSize) ;
    }


    @Override
    public long recover() {
        long processOffset = getFromOffset();

        ByteBuffer byteBuffer = mappedFile.getByteBuffer();

        while (true) {
            byteBuffer.mark();
            int magic = byteBuffer.getInt(4);
            byteBuffer.reset();
            if (magic == StoreMessage.MESSAGE_MAGIC) {
                StoreMessage storeMessage = new StoreMessage();
                storeMessage.decode(byteBuffer);
                if (!checkMessage(storeMessage, true)) {
                    log.info("Found illegal message , " + storeMessage);
                    break;
                }
                processOffset += storeMessage.getStoreSize();
            } else if (magic == StoreMessage.BLANK_MAGIC) {
                log.info("recover physics file end, " + mappedFile.getFile().getName());
                break;

            } else {
                log.info("recover physics file end, " + mappedFile.getFile().getName());
                break;
            }
        }
        return processOffset;

    }

    private boolean checkMessage(StoreMessage storeMessage, boolean checkCRC) {

        if (storeMessage.getCreateTimestamp() <= 0) {
            log.warn("illegal create timestamp " + storeMessage.getCreateTimestamp());
            return false;
        }

        if (storeMessage.getDataOffset() < 0) {
            log.warn("illegal data offset " + storeMessage.getDataOffset());
            return false;
        }


        if (checkCRC) {
            int crc32 = Utils.crc32(storeMessage.getBody());
            if (storeMessage.getCheckSum() != crc32) {
                log.warn("CRC check failed " + crc32 + " " + storeMessage.getCheckSum());
                return false;
            }
        }

        return true;
    }

    public AppendMessageResult appendMessage(StoreMessage msg ) {
        ByteBuffer byteBuffer = mappedFile.getByteBuffer();
        int writeOffset = getWritePosition() ;
        byteBuffer.position(writeOffset) ;
        AppendMessageResult appendMessageResult = appendMessageHelper.doAppend(getFromOffset(), byteBuffer, byteBuffer.remaining(), msg);

        setWritePosition(writeOffset + appendMessageResult.getWroteBytes());

        return appendMessageResult;
    }
}
