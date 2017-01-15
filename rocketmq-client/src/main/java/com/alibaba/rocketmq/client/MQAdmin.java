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
package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * Base interface for MQ management
 *
 * @author shijia.wxr
 */
public interface MQAdmin {
    /**
     * Creates an topic
     *
     * @param key
     *         accesskey
     * @param newTopic
     *         topic name
     * @param queueNum
     *         topic's queue number
     *
     * @throws MQClientException
     */
    void createTopic(final String key, final String newTopic, final int queueNum)
            throws MQClientException;


    /**
     * Creates an topic
     *
     * @param key
     *         accesskey
     * @param newTopic
     *         topic name
     * @param queueNum
     *         topic's queue number
     * @param topicSysFlag
     *         topic system flag
     *
     * @throws MQClientException
     */
    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
            throws MQClientException;


    /**
     * Gets the message queue offset according to some time in milliseconds<br>
     * be cautious to call because of more IO overhead
     *
     * @param mq
     *         Instance of MessageQueue
     * @param timestamp
     *         from when in milliseconds.
     *
     * @return offset
     *
     * @throws MQClientException
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;


    /**
     * Gets the max offset
     *
     * @param mq
     *         Instance of MessageQueue
     *
     * @return the max offset
     *
     * @throws MQClientException
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;


    /**
     * Gets the minimum offset
     *
     * @param mq
     *         Instance of MessageQueue
     *
     * @return the minimum offset
     *
     * @throws MQClientException
     */
    long minOffset(final MessageQueue mq) throws MQClientException;


    /**
     * Gets the earliest stored message time
     *
     * @param mq
     *         Instance of MessageQueue
     *
     * @return the time in microseconds
     *
     * @throws MQClientException
     */
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;


    /**
     * Query message according tto message id
     *
     * @param offsetMsgId
     *         message id
     *
     * @return message
     *
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    MessageExt viewMessage(final String offsetMsgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * Query messages
     *
     * @param topic
     *         message topic
     * @param key
     *         message key index word
     * @param maxNum
     *         max message number
     * @param begin
     *         from when
     * @param end
     *         to when
     *
     * @return Instance of QueryResult
     *
     * @throws MQClientException
     * @throws InterruptedException
     */
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
                             final long end) throws MQClientException, InterruptedException;
    
    /**

     * @param topic
     * @param msgId
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;        

    
}