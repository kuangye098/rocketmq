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
package com.alibaba.rocketmq.broker.transaction;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import com.alibaba.rocketmq.store.transaction.TransactionCheckExecuter;


/**
 * @author shijia.wxr
 */
public class DefaultTransactionCheckExecuter implements TransactionCheckExecuter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerController brokerController;


    public DefaultTransactionCheckExecuter(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public void gotoCheck(int producerGroupHashCode, long tranStateTableOffset, long commitLogOffset,
            int msgSize) {
        final ClientChannelInfo clientChannelInfo =
                this.brokerController.getProducerManager().pickProducerChannelRandomly(producerGroupHashCode);
        if (null == clientChannelInfo) {
            log.warn("check a producer transaction state, but not find any channel of this group[{}]",
                producerGroupHashCode);
            return;
        }

         SelectMapedBufferResult selectMapedBufferResult =
                this.brokerController.getMessageStore().selectOneMessageByOffset(commitLogOffset, msgSize);
        if (null == selectMapedBufferResult) {
            log.warn(
                "check a producer transaction state, but not find message by commitLogOffset: {}, msgSize: ",
                commitLogOffset, msgSize);
            return;
        }

        final CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
        requestHeader.setCommitLogOffset(commitLogOffset);
        requestHeader.setTranStateTableOffset(tranStateTableOffset);
        this.brokerController.getBroker2Client().checkProducerTransactionState(
            clientChannelInfo.getChannel(), requestHeader, selectMapedBufferResult);
    }
}
