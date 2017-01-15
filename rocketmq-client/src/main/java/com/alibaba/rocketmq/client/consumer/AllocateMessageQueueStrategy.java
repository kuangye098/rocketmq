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
package com.alibaba.rocketmq.client.consumer;

import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.List;


/**
 * Strategy Algorithm for message allocating between consumers
 *
 * @author shijia.wxr
 * @author vongosling
 */
public interface AllocateMessageQueueStrategy {

    /**
     * Allocating by consumer id
     *
     * @param consumerGroup
     *         current consumer group
     * @param currentCID
     *         current consumer id
     * @param mqAll
     *         message queue set in current topic
     * @param cidAll
     *         consumer set in current consumer group
     *
     * @return
     */
     List<MessageQueue> allocate(//
                                       final String consumerGroup,//
                                       final String currentCID,//
                                       final List<MessageQueue> mqAll,//
                                       final List<String> cidAll//
    );


    /**
     * Algorithm name
     *
     * @return
     */
     String getName();
}
