/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.hook;

import java.util.List;
import java.util.Map;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;


public class ConsumeMessageContext {
    private String consumerGroup;
    private List<MessageExt> msgList;
    private MessageQueue mq;
    private boolean success;
    private String status;
    private Object mqTraceContext;
    private Map<String, String> props;


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public List<MessageExt> getMsgList() {
        return msgList;
    }


    public void setMsgList(List<MessageExt> msgList) {
        this.msgList = msgList;
    }


    public MessageQueue getMq() {
        return mq;
    }


    public void setMq(MessageQueue mq) {
        this.mq = mq;
    }


    public boolean isSuccess() {
        return success;
    }


    public void setSuccess(boolean success) {
        this.success = success;
    }


    public Object getMqTraceContext() {
        return mqTraceContext;
    }


    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }


    public Map<String, String> getProps() {
        return props;
    }


    public void setProps(Map<String, String> props) {
        this.props = props;
    }


    public String getStatus() {
        return status;
    }


    public void setStatus(String status) {
        this.status = status;
    }
}
