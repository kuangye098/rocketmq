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

import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;


/**
 * Push consumer
 *
 * @author shijia.wxr
 */
public interface MQPushConsumer extends MQConsumer {
    /**
     * Start the consumer
     *
     * @throws MQClientException
     */
    void start() throws MQClientException;


    /**
     * Shutdown the consumer
     */
    void shutdown();


    /**
     * Register the message listener
     *
     * @param messageListener
     */
    @Deprecated
    void registerMessageListener(MessageListener messageListener);


    void registerMessageListener(final MessageListenerConcurrently messageListener);


    void registerMessageListener(final MessageListenerOrderly messageListener);


    /**
     * Subscribe some topic
     *
     * @param topic
     * @param subExpression
     *         subscription expression.it only support or operation such as
     *         "tag1 || tag2 || tag3" <br>
     *         if null or * expression,meaning subscribe all
     *
     * @throws MQClientException
     */
    void subscribe(final String topic, final String subExpression) throws MQClientException;


    /**
     * Subscribe some topic
     *
     * @param topic
     * @param fullClassName
     *         full class name，must extend
     *         com.alibaba.rocketmq.common.filter. MessageFilter
     * @param filterClassSource
     *         class source code，used UTF-8 file encoding,must be responsible
     *         for your code safety
     *
     * @throws MQClientException
     */
    void subscribe(final String topic, final String fullClassName, final String filterClassSource) throws MQClientException;


    /**
     * Unsubscribe consumption some topic
     *
     * @param topic
     *         message topic
     */
    void unsubscribe(final String topic);


    /**
     * Update the consumer thread pool size Dynamically
     *
     * @param corePoolSize
     */
    void updateCorePoolSize(int corePoolSize);


    /**
     * Suspend the consumption
     */
    void suspend();


    /**
     * Resume the consumption
     */
    void resume();
}
