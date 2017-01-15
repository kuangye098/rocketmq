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
package com.alibaba.rocketmq.client.hook;

import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author manhong.yqd
 */
public class CheckForbiddenContext {
    private String nameSrvAddr;
    private String group;
    private Message message;
    private MessageQueue mq;
    private String brokerAddr;
    private CommunicationMode communicationMode;
    private SendResult sendResult;
    private Exception exception;
    private Object arg;
    private boolean unitMode = false;


    public String getGroup() {
        return group;
    }


    public void setGroup(String group) {
        this.group = group;
    }


    public Message getMessage() {
        return message;
    }


    public void setMessage(Message message) {
        this.message = message;
    }


    public MessageQueue getMq() {
        return mq;
    }


    public void setMq(MessageQueue mq) {
        this.mq = mq;
    }


    public String getBrokerAddr() {
        return brokerAddr;
    }


    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }


    public CommunicationMode getCommunicationMode() {
        return communicationMode;
    }


    public void setCommunicationMode(CommunicationMode communicationMode) {
        this.communicationMode = communicationMode;
    }


    public SendResult getSendResult() {
        return sendResult;
    }


    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }


    public Exception getException() {
        return exception;
    }


    public void setException(Exception exception) {
        this.exception = exception;
    }


    public Object getArg() {
        return arg;
    }


    public void setArg(Object arg) {
        this.arg = arg;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }


    public String getNameSrvAddr() {
        return nameSrvAddr;
    }


    public void setNameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
    }


    @Override
    public String toString() {
        return "SendMessageContext [nameSrvAddr=" + nameSrvAddr + ", group=" + group + ", message=" + message
                + ", mq=" + mq + ", brokerAddr=" + brokerAddr + ", communicationMode=" + communicationMode
                + ", sendResult=" + sendResult + ", exception=" + exception + ", unitMode=" + unitMode
                + ", arg=" + arg + "]";
    }
}
