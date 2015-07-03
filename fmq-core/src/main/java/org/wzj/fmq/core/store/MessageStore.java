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


/**
 * Created by wens on 15-6-18.
 */
public interface MessageStore extends Lifecycle {

    PutMessageResult putMessage(StoreMessage msg);

    GetMessageResult getMessage(String topic, long index, final int maxNum);

    long getMaxSequence(String topic);

    long getMinSequence(String topic);

    long getSequenceByTime(String topic, long timestamp);

    long getMessageStoreTimestamp(String topic, long index);

    long getTotalMessageNum(String topic);

}
