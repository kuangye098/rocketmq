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
package com.alibaba.rocketmq.tools.command.broker;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.UnsupportedEncodingException;


/**
 * @author lansheng.zj
 */
public class SendMsgStatusCommand implements SubCommand {

    @Override
    public String commandName() {
        return "sendMsgStatus";
    }


    @Override
    public String commandDesc() {
        return "send msg to broker.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerName", true, "Broker Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "messageSize", true, "Message Size, Default: 128");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "count", true, "send message count, Default: 50");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        final DefaultMQProducer producer = new DefaultMQProducer("PID_SMSC",rpcHook);
        producer.setInstanceName("PID_SMSC_" + System.currentTimeMillis());

        try {
            producer.start();
            String brokerName = commandLine.getOptionValue('b').trim();
            int messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 128;
            int count = commandLine.hasOption('c') ? Integer.parseInt(commandLine.getOptionValue('c')) : 50;

            producer.send(buildMessage(brokerName, 16));

            for (int i = 0; i < count; i++) {
                long begin = System.currentTimeMillis();
                SendResult result = producer.send(buildMessage(brokerName, messageSize));
                System.out.println("rt:" + (System.currentTimeMillis() - begin) + "ms, SendResult=" + result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutdown();
        }
    }


    private static Message buildMessage(final String topic, final int messageSize) throws UnsupportedEncodingException {
        Message msg = new Message();
        msg.setTopic(topic);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageSize; i += 11) {
            sb.append("hello jodie");
        }
        msg.setBody(sb.toString().getBytes(MixAll.DEFAULT_CHARSET));
        return msg;
    }
}
