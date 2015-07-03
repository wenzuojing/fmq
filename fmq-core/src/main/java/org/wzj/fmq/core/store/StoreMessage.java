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
package org.wzj.fmq.core.store;

import org.wzj.fmq.core.store.file.Encodable;

import java.io.Serializable;
import java.nio.ByteBuffer;


/**
 * Created by wens on 15-6-18.
 */
public class StoreMessage implements Serializable, Encodable {

    public final static int MESSAGE_MAGIC = 0x11223344;
    public final static int BLANK_MAGIC = 0x22334455;

    public static final int POSITION_SIZE = 0;
    public static final int POSITION_MAGIC = 4;
    public static final int POSITION_CRC32 = 8;
    public static final int POSITION_FLAG = 12;
    public static final int POSITION_CREATE_TIMESTAMP = 16 ;
    public static final int POSITION_DATA_OFFSET = 24 ;
    public static final int position_sequence = 32 ;

    private String topic;
    private byte[] body;
    private int storeSize;
    private long dataOffset;
    private long sequence ;
    private int flag;
    private long createTimestamp;
    private int checkSum;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public int getCheckSum() {
        return checkSum;
    }

    public void setCheckSum(int checkSum) {
        this.checkSum = checkSum;
    }

    public long getDataOffset() {
        return dataOffset;
    }

    public void setDataOffset(long dataOffset) {
        this.dataOffset = dataOffset;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    @Override
    public void decode(ByteBuffer buffer) {

        storeSize = buffer.getInt();
        buffer.getInt();
        checkSum = buffer.getInt();
        flag = buffer.getInt();
        createTimestamp = buffer.getLong();
        dataOffset = buffer.getLong();
        sequence = buffer.getLong();
        int bodyByteSize = buffer.getInt();

        if (bodyByteSize != 0) {
            body = new byte[bodyByteSize] ;
            buffer.get(body);
        }

        byte topicByteSize = buffer.get();
        if (topicByteSize != 0) {
            byte[] buf = new byte[topicByteSize];
            buffer.get(buf);
            topic = new String(buf).intern();
        }



    }

    @Override
    public void encode(ByteBuffer buffer) {

        buffer.putInt(storeSize);
        buffer.putInt(MESSAGE_MAGIC);
        buffer.putInt(checkSum);
        buffer.putInt(flag);
        buffer.putLong(createTimestamp);
        buffer.putLong(dataOffset);
        buffer.putLong(sequence);
        int bodyByteSize = getBodyByteSize();
        buffer.putInt(bodyByteSize);

        if (bodyByteSize != 0) {
            buffer.put(body);
        }

        int topicByteSize = getTopicByteSize();
        buffer.put((byte) topicByteSize);

        if (topicByteSize != 0) {
            buffer.put(topic.getBytes());
        }

    }

    public int getTopicByteSize() {
        return topic == null ? 0 : topic.getBytes().length;
    }

    public int getBodyByteSize() {
        return body == null ? 0 : body.length;
    }

    //todo
    public int calNeedByteSize() {
        return 4 /* storeSize*/
                + 4 /*magic*/
                + 4 /* checkSum*/
                + 4 //*flag*/
                + 8 /*createTimestamp*/
                + 8 /*dataOffset*/
                + 4 + getBodyByteSize() /*body*/
                + 1 + getTopicByteSize() /*topic*/;
    }
}
