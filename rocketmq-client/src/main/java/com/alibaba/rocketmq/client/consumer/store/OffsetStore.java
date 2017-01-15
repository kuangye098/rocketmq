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
package com.alibaba.rocketmq.client.consumer.store;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.Map;
import java.util.Set;


/**
 * Offset store interface
 *
 * @author shijia.wxr
 */
public interface OffsetStore {
    /**
     * Load
     *
     * @throws MQClientException
     */
    void load() throws MQClientException;


    /**
     * Update the offset,store it in memory
     *
     * @param mq
     * @param offset
     * @param increaseOnly
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * Get offset from local storage
     *
     * @param mq
     * @param type
     *
     * @return
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * Persist all offsets,may be in local storage or remote name server
     *
     * @param mqs
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * Persist the offset,may be in local storage or remote name server
     *
     * @param mq
     */
    void persist(final MessageQueue mq);

    /**
     * Remove offset
     *
     * @param mq
     */
    void removeOffset(MessageQueue mq);

    /**
     * @param topic
     *
     * @return
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);
}
