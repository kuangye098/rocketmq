package com.alibaba.rocketmq.service;

import java.util.Collection;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.config.ConfigureInitializer;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


public abstract class AbstractService {

    @Autowired
    ConfigureInitializer configureInitializer;


    protected DefaultMQAdminExt getDefaultMQAdminExt() {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        return defaultMQAdminExt;
    }


    protected void shutdownDefaultMQAdminExt(DefaultMQAdminExt defaultMQAdminExt) {
        defaultMQAdminExt.shutdown();
    }


    protected Collection<Option> getOptions(SubCommand subCommand) {
        Options options = new Options();
        subCommand.buildCommandlineOptions(options);
        @SuppressWarnings("unchecked")
        Collection<Option> col = options.getOptions();
        return col;
    }


    protected int translatePerm(String perm) {
        if (perm.toLowerCase().equals("r")) {
            return PermName.PERM_READ;
        }
        else if (perm.toLowerCase().equals("w")) {
            return PermName.PERM_WRITE;
        }
        else {
            return PermName.PERM_READ | PermName.PERM_WRITE;
        }
    }

}
