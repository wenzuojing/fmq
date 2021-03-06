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

import java.nio.ByteBuffer;


/**
 * Created by wens on 15-6-18.
 */
public class SelectMappedBufferResult {
    // 从队列中哪个绝对Offset开始
    private final long startOffset;
    // position从0开始
    private final ByteBuffer byteBuffer;
    // 有效数据大小
    private int size;


    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size ) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
    }


    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }


    public int getSize() {
        return size;
    }


    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }


    public long getStartOffset() {
        return startOffset;
    }
}
