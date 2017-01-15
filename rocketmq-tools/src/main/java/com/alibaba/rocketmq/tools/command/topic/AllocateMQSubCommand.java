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
package com.alibaba.rocketmq.tools.command.topic;

import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class AllocateMQSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "allocateMQ";
    }


    @Override
    public String commandDesc() {
        return "Allocate MQ";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "ipList", true, "ipList");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            adminExt.start();

            String topic = commandLine.getOptionValue('t').trim();
            String ips = commandLine.getOptionValue('i').trim();
            final String[] split = ips.split(",");
            final List<String> ipList = new LinkedList<String>();
            for (String ip : split) {
                ipList.add(ip);
            }

            final TopicRouteData topicRouteData = adminExt.examineTopicRouteInfo(topic);
            final Set<MessageQueue> mqs = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);

            final AllocateMessageQueueAveragely averagely = new AllocateMessageQueueAveragely();


            RebalanceResult rr = new RebalanceResult();

            for (String i : ipList) {
                final List<MessageQueue> mqResult = averagely.allocate("aa", i, new ArrayList<MessageQueue>(mqs), ipList);
                rr.getResult().put(i, mqResult);
            }

            final String json = RemotingSerializable.toJson(rr, false);
            System.out.println(json);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            adminExt.shutdown();
        }
    }
}
