/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.client.consumer.listener;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * Consumer Orderly consumption context
 *
 * @author shijia.wxr
 */
public class ConsumeOrderlyContext {
    private final MessageQueue messageQueue;
    private boolean autoCommit = true;
    private long suspendCurrentQueueTimeMillis = -1;


    public ConsumeOrderlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }


    public boolean isAutoCommit() {
        return autoCommit;
    }


    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }


    public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }
}
