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
package com.alibaba.rocketmq.client.producer.selector;

import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.Random;


/**
 * @author shijia.wxr
 */
public class SelectMessageQueueByRandoom implements MessageQueueSelector {
    private Random random = new Random(System.currentTimeMillis());


    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
        }

        value = value % mqs.size();
        return mqs.get(value);
    }
}
