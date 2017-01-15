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
package com.alibaba.rocketmq.store;

/**
 * @author shijia.wxr
 */
public class DispatchRequest {
    private final String topic;
    private final int queueId;
    private final long commitLogOffset;
    private final int msgSize;
    private final long tagsCode;
    private final long storeTimestamp;
    private final long consumeQueueOffset;
    private final String keys;
    private final boolean success;
    private final String uniqKey;

    private final int sysFlag;
    private final long tranStateTableOffset;
    private final long preparedTransactionOffset;
    private final String producerGroup;


    public DispatchRequest(//
                           final String topic,// 1
                           final int queueId,// 2
                           final long commitLogOffset,// 3
                           final int msgSize,// 4
                           final long tagsCode,// 5
                           final long storeTimestamp,// 6
                           final long consumeQueueOffset,// 7
                           final String keys,// 8
                           final String uniqKey,// 9
                           final int sysFlag,// 10
                           final long tranStateTableOffset,// 11
                           final long preparedTransactionOffset,// 12
                           final String producerGroup// 13
    ) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;
        this.uniqKey = uniqKey;

        this.sysFlag = sysFlag;
        this.tranStateTableOffset = tranStateTableOffset;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.producerGroup = producerGroup;
        this.success = true;
    }

    public DispatchRequest(int size) {
        // 1
        this.topic = "";
        // 2
        this.queueId = 0;
        // 3
        this.commitLogOffset = 0;
        // 4
        this.msgSize = size;
        // 5
        this.tagsCode = 0;
        // 6
        this.storeTimestamp = 0;
        // 7
        this.consumeQueueOffset = 0;
        // 8
        this.keys = "";
        // 9
        this.uniqKey = null;
        // 10
        this.sysFlag = 0;
        // 11
        this.tranStateTableOffset = 0;
        // 12
        this.preparedTransactionOffset = 0;
        // 13
        this.producerGroup = "";
        // 14
        this.success = false;
    }

    public DispatchRequest(int size, boolean success) {
        // 1
        this.topic = "";
        // 2
        this.queueId = 0;
        // 3
        this.commitLogOffset = 0;
        // 4
        this.msgSize = size;
        // 5
        this.tagsCode = 0;
        // 6
        this.storeTimestamp = 0;
        // 7
        this.consumeQueueOffset = 0;
        // 8
        this.keys = "";
        // 9
        this.uniqKey = null;
        // 10
        this.sysFlag = 0;
        // 11
        this.tranStateTableOffset = 0;
        // 12
        this.preparedTransactionOffset = 0;
        // 13
        this.producerGroup = "";
        // 14
        this.success = success;
    }


    public String getTopic() {
        return topic;
    }


    public int getQueueId() {
        return queueId;
    }


    public long getCommitLogOffset() {
        return commitLogOffset;
    }


    public int getMsgSize() {
        return msgSize;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }


    public String getKeys() {
        return keys;
    }


    public long getTagsCode() {
        return tagsCode;
    }


    public int getSysFlag() {
        return sysFlag;
    }


    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }


    public long getTranStateTableOffset() {
        return tranStateTableOffset;
    }


    public boolean isSuccess() {
        return success;
    }


    public String getUniqKey() {
        return uniqKey;
    }


    public String getProducerGroup() {
        return producerGroup;
    }
}
