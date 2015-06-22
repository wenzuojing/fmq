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
package org.wzj.fmq.core.common;

import org.wzj.fmq.core.store.Lifecycle;


//
public abstract class ServiceThread implements Lifecycle ,Runnable {

    private String serviceName ;

    private Thread thread ;

    private volatile boolean stop = false ;


    public ServiceThread(String serviceName ){
        this.serviceName = serviceName ;
    }

    public abstract void executeTask() ;


    @Override
    public void run() {
        while(!stop){
            executeTask();
        }
    }

    @Override
    public void init() {
        thread = new Thread(this , this.serviceName ) ;
    }

    @Override
    public void start() {
        thread.start();
    }

    @Override
    public void shutdown() {
        stop = true ;
    }
}
