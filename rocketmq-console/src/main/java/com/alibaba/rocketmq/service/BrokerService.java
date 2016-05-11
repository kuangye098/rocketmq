package com.alibaba.rocketmq.service;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.broker.BrokerStatusSubCommand;
import com.alibaba.rocketmq.tools.command.broker.UpdateBrokerConfigSubCommand;
import com.alibaba.rocketmq.validate.CmdTrace;
import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-18
 */
@Service
public class BrokerService extends AbstractService {

    static final Logger logger = LoggerFactory.getLogger(BrokerService.class);

    static final BrokerStatusSubCommand brokerStatsSubCommand = new BrokerStatusSubCommand();


    public Collection<Option> getOptionsForBrokerStats() {
        return getOptions(brokerStatsSubCommand);
    }


    @CmdTrace(cmdClazz = BrokerStatusSubCommand.class)
    public Table brokerStats(String brokerAddr) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(brokerAddr);
            TreeMap<String, String> tmp = new TreeMap<String, String>();
            tmp.putAll(kvTable.getTable());
            return Table.Map2VTable(tmp);
        }
        catch (Throwable e) {
            logger.error(e.getMessage(), e);
            t = e;
        }
        finally {
            shutdownDefaultMQAdminExt(defaultMQAdminExt);
        }
        throw t;
    }

    static final UpdateBrokerConfigSubCommand updateBrokerConfigSubCommand =
            new UpdateBrokerConfigSubCommand();


    public Collection<Option> getOptionsForUpdateBrokerConfig() {
        return getOptions(updateBrokerConfigSubCommand);
    }


    @CmdTrace(cmdClazz = UpdateBrokerConfigSubCommand.class)
    public boolean updateBrokerConfig(String brokerAddr, String clusterName, String key, String value)
            throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            Properties properties = new Properties();
            properties.put(key, value);
            if (StringUtils.isNotBlank(brokerAddr)) {
                defaultMQAdminExt.start();
                defaultMQAdminExt.updateBrokerConfig(brokerAddr, properties);
                return true;
            }
            else if (StringUtils.isNotBlank(clusterName)) {
                defaultMQAdminExt.start();
                Set<String> masterSet =
                        CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String tempBrokerAddr : masterSet) {
                    defaultMQAdminExt.updateBrokerConfig(tempBrokerAddr, properties);
                }
                return true;
            }
            else {
                throw new IllegalStateException("brokerAddr or clusterName can not be all blank");
            }
        }
        catch (Throwable e) {
            logger.error(e.getMessage(), e);
            t = e;
        }
        finally {
            shutdownDefaultMQAdminExt(defaultMQAdminExt);
        }
        throw t;
    }
}
