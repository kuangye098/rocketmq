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

/**
 *
 */
package com.alibaba.rocketmq.broker.plugin;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.store.*;

import java.util.HashMap;
import java.util.Set;

public abstract class AbstractPluginMessageStore implements MessageStore {
    protected MessageStore next = null;
    protected MessageStorePluginContext context;

    public AbstractPluginMessageStore(MessageStorePluginContext context, MessageStore next) {
        this.next = next;
        this.context = context;
    }

    @Override
    public long getEarliestMessageTime() {
        return next.getEarliestMessageTime();
    }

    @Override
    public long lockTimeMills() {
        return next.lockTimeMills();
    }

    @Override
    public boolean isOSPageCacheBusy() {
        return next.isOSPageCacheBusy();
    }

    @Override
    public boolean load() {
        return next.load();
    }

    @Override
    public void start() throws Exception {
        next.start();
    }

    @Override
    public void shutdown() {
        next.shutdown();
    }

    @Override
    public void destroy() {
        next.destroy();
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return next.putMessage(msg);
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset,
                                       int maxMsgNums, SubscriptionData subscriptionData) {
        return next.getMessage(group, topic, queueId, offset, maxMsgNums, subscriptionData);
    }

    @Override
    public long getMaxOffsetInQuque(String topic, int queueId) {
        return next.getMaxOffsetInQuque(topic, queueId);
    }

    @Override
    public long getMinOffsetInQuque(String topic, int queueId) {
        return next.getMinOffsetInQuque(topic, queueId);
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long cqOffset) {
        return next.getCommitLogOffsetInQueue(topic, queueId, cqOffset);
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return next.getOffsetInQueueByTime(topic, queueId, timestamp);
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        return next.lookMessageByOffset(commitLogOffset);
    }

    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        return next.selectOneMessageByOffset(commitLogOffset);
    }

    @Override
    public SelectMapedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return next.selectOneMessageByOffset(commitLogOffset, msgSize);
    }

    @Override
    public String getRunningDataInfo() {
        return next.getRunningDataInfo();
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        return next.getRuntimeInfo();
    }

    @Override
    public long getMaxPhyOffset() {
        return next.getMaxPhyOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return next.getMinPhyOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        return next.getEarliestMessageTime(topic, queueId);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long offset) {
        return next.getMessageStoreTimeStamp(topic, queueId, offset);
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        return next.getMessageTotalInQueue(topic, queueId);
    }

    @Override
    public SelectMapedBufferResult getCommitLogData(long offset) {
        return next.getCommitLogData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        return next.appendToCommitLog(startOffset, data);
    }

    @Override
    public void excuteDeleteFilesManualy() {
        next.excuteDeleteFilesManualy();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin,
                                           long end) {
        return next.queryMessage(topic, key, maxNum, begin, end);
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        next.updateHaMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return next.slaveFallBehindMuch();
    }

    @Override
    public long now() {
        return next.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        return next.cleanUnusedTopic(topics);
    }

    @Override
    public void cleanExpiredConsumerQueue() {
        next.cleanExpiredConsumerQueue();
    }

    @Override
    public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {
        return next.checkInDiskByConsumeOffset(topic, queueId, consumeOffset);
    }

    @Override
    public long dispatchBehindBytes() {
        return next.dispatchBehindBytes();
    }

    @Override
    public long flush() {
        return next.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return next.resetWriteOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return next.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        next.setConfirmOffset(phyOffset);
    }

}
