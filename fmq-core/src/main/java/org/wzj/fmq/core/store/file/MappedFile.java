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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Created by wens on 15-6-18.
 */
public interface MappedFile {

    ByteBuffer getByteBuffer(int position);

    ByteBuffer getByteBuffer(int position , int size );

    ByteBuffer getByteBuffer();

    int getWritePosition();

    void setWritePosition(int position);

    int getFlushPosition();

    File getFile();

    int getFileSize();

    void flush() throws IOException;


    boolean isFull();

    void close() throws IOException;

    boolean isClose();


    void delete();

    boolean isDirty();
}
