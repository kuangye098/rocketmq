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

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.util.Set;


/**
 * Message queue consumer interface
 *
 * @author shijia.wxr
 */
public interface MQConsumer extends MQAdmin {
    /**
     * If consuming failure,message will be send back to the brokers,and delay consuming some time
     *
     * @param msg
     * @param delayLevel
     *
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    @Deprecated
    void sendMessageBack(final MessageExt msg, final int delayLevel) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    /**
     * If consuming failure,message will be send back to the broker,and delay consuming some time
     *
     * @param msg
     * @param delayLevel
     * @param brokerName
     *
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException;


    /**
     * Fetch message queues from consumer cache according to the topic
     *
     * @param topic
     *         message topic
     *
     * @return queue set
     *
     * @throws MQClientException
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
