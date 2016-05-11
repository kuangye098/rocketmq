package com.alibaba.rocketmq.service;

import java.util.Collection;

import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.ProducerConnection;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import com.alibaba.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import com.alibaba.rocketmq.validate.CmdTrace;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-16
 */
@Service
public class ConnectionService extends AbstractService {
    static final Logger logger = LoggerFactory.getLogger(ConnectionService.class);

    static final ConsumerConnectionSubCommand consumerConnectionSubCommand =
            new ConsumerConnectionSubCommand();


    public Collection<Option> getOptionsForGetConsumerConnection() {
        return getOptions(consumerConnectionSubCommand);
    }


    @CmdTrace(cmdClazz = ConsumerConnectionSubCommand.class)
    public ConsumerConnection getConsumerConnection(String consumerGroup) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
            return cc;
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

    static final ProducerConnectionSubCommand producerConnectionSubCommand =
            new ProducerConnectionSubCommand();


    public Collection<Option> getOptionsForGetProducerConnection() {
        return getOptions(producerConnectionSubCommand);
    }


    @CmdTrace(cmdClazz = ProducerConnectionSubCommand.class)
    public ProducerConnection getProducerConnection(String group, String topicName) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            ProducerConnection pc = defaultMQAdminExt.examineProducerConnectionInfo(group, topicName);
            return pc;
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
