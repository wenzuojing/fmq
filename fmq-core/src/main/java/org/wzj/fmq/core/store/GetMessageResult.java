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

import org.wzj.fmq.core.store.file.SelectMappedBufferResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by wens on 15-6-18.
 */
public class GetMessageResult {
    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>(100);
    private GetMessageStatus status;
    private long nextIndex;
    private long minIndex;
    private long maxIndex;


    public GetMessageResult() {
    }

    public GetMessageStatus getStatus() {
        return status;
    }


    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }


    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMinIndex() {
        return minIndex;
    }

    public void setMinIndex(long minIndex) {
        this.minIndex = minIndex;
    }

    public long getMaxIndex() {
        return maxIndex;
    }

    public void setMaxIndex(long maxIndex) {
        this.maxIndex = maxIndex;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
    }

    public int getMessageCount() {
        return this.messageBufferList.size();
    }

    @Override
    public String toString() {
        return "GetMessageResult{" +
                "messageBufferList=" + messageBufferList +
                ", status=" + status +
                ", nextIndex=" + nextIndex +
                ", minIndex=" + minIndex +
                ", maxIndex=" + maxIndex +
                '}';
    }
}
