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
public enum AppendMessageStatus {
    // 成功追加消息
    PUT_OK,
    // 走到文件末尾
    END_OF_FILE,
    // 消息大小超限
    MESSAGE_SIZE_EXCEEDED,
    // 未知错误
    UNKNOWN_ERROR,
}
