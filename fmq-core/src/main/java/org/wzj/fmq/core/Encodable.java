package org.wzj.fmq.core;

/**
 * Created by wens on 15-6-12.
 */
public interface Encodable {

    byte[] encode() ;

    void decode(byte[] bytes ) ;

}
