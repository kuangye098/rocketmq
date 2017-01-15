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
package com.alibaba.rocketmq.client.producer;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.*;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.util.List;


/**
 * @author shijia.wxr
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;
    private String producerGroup;
    /**
     * Just for testing or demo program
     */
    private String createTopicKey = MixAll.DEFAULT_TOPIC;
    private volatile int defaultTopicQueueNums = 4;
    private int sendMsgTimeout = 3000;
    private int compressMsgBodyOverHowmuch = 1024 * 4;
    private int retryTimesWhenSendFailed = 2;
    private int retryTimesWhenSendAsyncFailed = 2;

    private boolean retryAnotherBrokerWhenNotStoreOK = false;
    private int maxMessageSize = 1024 * 1024 * 4; // 4M
    public DefaultMQProducer() {
        this(MixAll.DEFAULT_PRODUCER_GROUP, null);
    }


    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }


    public DefaultMQProducer(final String producerGroup) {
        this(producerGroup, null);
    }


    public DefaultMQProducer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }


    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.start();
    }

    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
    }


    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(topic);
    }


    @Override
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg);
    }


    @Override
    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, timeout);
    }


    @Override
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }


    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }


    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg);
    }


    @Override
    public SendResult send(Message msg, MessageQueue mq)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq);
    }


    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq, timeout);
    }


    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback);
    }


    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback, timeout);
    }


    @Override
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, mq);
    }


    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }


    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }


    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }


    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }


    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
            throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }


    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, final Object arg)
            throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(mq, timestamp);
    }


    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(mq);
    }


    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(mq);
    }


    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(topic, key, maxNum, begin, end);
    }


    @Override
    public MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageId oldMsgId = MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(topic, msgId);
    }

    public String getProducerGroup() {
        return producerGroup;
    }


    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }


    public String getCreateTopicKey() {
        return createTopicKey;
    }


    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }


    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }


    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }


    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }


    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }


    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }


    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }


    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }


    public int getMaxMessageSize() {
        return maxMessageSize;
    }


    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }


    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }


    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }


    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }


    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }


    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }


    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }


    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }
}
