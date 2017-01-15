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
package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;


/**
 * @author shijia.wxr
 */
public class PullResultExt extends PullResult {
    private final long suggestWhichBrokerId;
    private byte[] messageBinary;


    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
                         List<MessageExt> msgFoundList, final long suggestWhichBrokerId, final byte[] messageBinary) {
        super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
        this.suggestWhichBrokerId = suggestWhichBrokerId;
        this.messageBinary = messageBinary;
    }


    public byte[] getMessageBinary() {
        return messageBinary;
    }


    public void setMessageBinary(byte[] messageBinary) {
        this.messageBinary = messageBinary;
    }


    public long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }
}
