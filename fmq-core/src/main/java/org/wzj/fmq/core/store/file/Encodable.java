package org.wzj.fmq.core.store.file;

import java.nio.ByteBuffer;

/**
 * Created by wens on 15-6-16.
 */
public interface Encodable {

    void decode(ByteBuffer buffer);

    void encode(ByteBuffer buffer);

}
