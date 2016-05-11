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
package com.alibaba.rocketmq.tools.command.broker;

import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 更新Broker配置文件
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-10-20
 */
public class UpdateBrokerConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateBrokerConfig";
    }


    @Override
    public String commandDesc() {
        return "Update broker's config";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "update which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "update which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("k", "key", true, "config key");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("v", "value", true, "config value");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String key = commandLine.getOptionValue('k').trim();
            String value = commandLine.getOptionValue('v').trim();
            Properties properties = new Properties();
            properties.put(key, value);

            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();

                defaultMQAdminExt.updateBrokerConfig(brokerAddr, properties);
                System.out.printf("update broker config success, %s\n", brokerAddr);
                return;

            }
            else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();

                Set<String> masterSet =
                        CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String brokerAddr : masterSet) {
                    defaultMQAdminExt.updateBrokerConfig(brokerAddr, properties);
                    System.out.printf("update broker config success, %s\n", brokerAddr);
                }
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
