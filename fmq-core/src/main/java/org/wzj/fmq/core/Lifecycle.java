package org.wzj.fmq.core;

import java.io.Closeable;

/**
 * Created by wens on 15-6-14.
 */
public interface Lifecycle  {

    void init() ;

    void close() ;
}
