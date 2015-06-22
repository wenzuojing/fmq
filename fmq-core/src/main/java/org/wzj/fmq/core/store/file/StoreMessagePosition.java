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

/**
 * Created by wens on 15-6-18.
 */
public class StoreMessagePosition {

    private final String topic;
    private final long dataQueueOffset;
    private final int msgSize;
    private final long createTimestamp;

    public StoreMessagePosition(int msgSize) {
        this(null, 0, msgSize, 0 );
    }

    public StoreMessagePosition(String topic, long dataQueueOffset, int msgSize, long createTimestamp ) {
        this.topic = topic;
        this.dataQueueOffset = dataQueueOffset;
        this.msgSize = msgSize;
        this.createTimestamp = createTimestamp;
    }

    public String getTopic() {
        return topic;
    }

    public long getDataQueueOffset() {
        return dataQueueOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    @Override
    public String toString() {
        return "StoreMessagePosition{" +
                "topic='" + topic + '\'' +
                ", dataQueueOffset=" + dataQueueOffset +
                ", msgSize=" + msgSize +
                ", createTimestamp=" + createTimestamp +
                '}';
    }
}
