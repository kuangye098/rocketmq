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
package com.alibaba.rocketmq.tools.command.namesrv;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


/**
 *
 * @author manhong.yqd
 *
 */
public class DeleteKvConfigCommand implements SubCommand {
    @Override
    public String commandName() {
        return "deleteKvConfig";
    }


    @Override
    public String commandDesc() {
        return "Delete KV config.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("s", "namespace", true, "set the namespace");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "key", true, "set the key name");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // namespace
            String namespace = commandLine.getOptionValue('s').trim();
            // key name
            String key = commandLine.getOptionValue('k').trim();

            defaultMQAdminExt.start();
            defaultMQAdminExt.deleteKvConfig(namespace, key);
            System.out.printf("delete kv config from namespace success.%n");
            return;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
