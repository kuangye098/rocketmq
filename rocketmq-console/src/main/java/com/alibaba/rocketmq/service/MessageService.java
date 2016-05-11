package com.alibaba.rocketmq.service;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByIdSubCommand;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByKeySubCommand;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByOffsetSubCommand;
import com.alibaba.rocketmq.validate.CmdTrace;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-17
 */
@Service
public class MessageService extends AbstractService {

    static final Logger logger = LoggerFactory.getLogger(MessageService.class);

    static final QueryMsgByIdSubCommand queryMsgByIdSubCommand = new QueryMsgByIdSubCommand();


    public Collection<Option> getOptionsForQueryMsgById() {
        return getOptions(queryMsgByIdSubCommand);
    }


    @CmdTrace(cmdClazz = QueryMsgByIdSubCommand.class)
    public Table queryMsgById(String msgId) throws Throwable {
        Throwable t = null;
        Map<String, String> map = new LinkedHashMap<String, String>();
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            MessageExt msg = defaultMQAdminExt.viewMessage(msgId);
            String bodyTmpFilePath = createBodyFile(msg);
            // System.out.printf("%-20s %s\n",//
            // "Topic:",//
            // msg.getTopic()//
            // );
            map.put("Topic", msg.getTopic());

            // System.out.printf("%-20s %s\n",//
            // "Tags:",//
            // "[" + msg.getTags() + "]"//
            // );
            map.put("Tags", "[" + msg.getTags() + "]");

            // System.out.printf("%-20s %s\n",//
            // "Keys:",//
            // "[" + msg.getKeys() + "]"//
            // );
            map.put("Keys", "[" + msg.getKeys() + "]");

            // System.out.printf("%-20s %d\n",//
            // "Queue ID:",//
            // msg.getQueueId()//
            // );
            map.put("Queue ID", String.valueOf(msg.getQueueId()));

            // System.out.printf("%-20s %d\n",//
            // "Queue Offset:",//
            // msg.getQueueOffset()//
            // );
            map.put("Queue Offset:", String.valueOf(msg.getQueueOffset()));

            // System.out.printf("%-20s %d\n",//
            // "CommitLog Offset:",//
            // msg.getCommitLogOffset()//
            // );
            map.put("CommitLog Offset:", String.valueOf(msg.getCommitLogOffset()));

            // System.out.printf("%-20s %s\n",//
            // "Born Timestamp:",//
            // UtilAll.timeMillisToHumanString2(msg.getBornTimestamp())//
            // );
            map.put("Born Timestamp:", UtilAll.timeMillisToHumanString2(msg.getBornTimestamp()));

            // System.out.printf("%-20s %s\n",//
            // "Store Timestamp:",//
            // UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp())//
            // );
            map.put("Store Timestamp:", UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp()));

            // System.out.printf("%-20s %s\n",//
            // "Born Host:",//
            // RemotingHelper.parseSocketAddressAddr(msg.getBornHost())//
            // );
            map.put("Born Host:", RemotingHelper.parseSocketAddressAddr(msg.getBornHost()));

            // System.out.printf("%-20s %s\n",//
            // "Store Host:",//
            // RemotingHelper.parseSocketAddressAddr(msg.getStoreHost())//
            // );
            map.put("Store Host:", RemotingHelper.parseSocketAddressAddr(msg.getStoreHost()));

            // System.out.printf("%-20s %d\n",//
            // "System Flag:",//
            // msg.getSysFlag()//
            // );
            map.put("System Flag:", String.valueOf(msg.getSysFlag()));

            // System.out.printf("%-20s %s\n",//
            // "Properties:",//
            // msg.getProperties() != null ? msg.getProperties().toString() :
            // ""//
            // );
            map.put("Properties:", msg.getProperties() != null ? msg.getProperties().toString() : "");

            // System.out.printf("%-20s %s\n",//
            // "Message Body Path:",//
            // bodyTmpFilePath//
            // );
            map.put("Message Body Path:", bodyTmpFilePath);
            return Table.Map2VTable(map);
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


    private String createBodyFile(MessageExt msg) throws IOException {
        DataOutputStream dos = null;

        try {
            String bodyTmpFilePath = "/tmp/rocketmq/msgbodys";
            File file = new File(bodyTmpFilePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            bodyTmpFilePath = bodyTmpFilePath + "/" + msg.getMsgId();
            dos = new DataOutputStream(new FileOutputStream(bodyTmpFilePath));
            dos.write(msg.getBody());
            return bodyTmpFilePath;
        }
        finally {
            if (dos != null)
                dos.close();
        }
    }

    static final QueryMsgByKeySubCommand queryMsgByKeySubCommand = new QueryMsgByKeySubCommand();


    public Collection<Option> getOptionsForQueryMsgByKey() {
        return getOptions(queryMsgByKeySubCommand);
    }


    @CmdTrace(cmdClazz = QueryMsgByKeySubCommand.class)
    public Table queryMsgByKey(String topicName, String msgKey, String fallbackHours) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            long h = 0;
            if (StringUtils.isNotBlank(fallbackHours)) {
                h = Long.parseLong(fallbackHours);
            }
            long end = System.currentTimeMillis() - (h * 60 * 60 * 1000);
            long begin = end - (6 * 60 * 60 * 1000);
            QueryResult queryResult = defaultMQAdminExt.queryMessage(topicName, msgKey, 32, begin, end);

            // System.out.printf("%-50s %-4s  %s\n",//
            // "#Message ID",//
            // "#QID",//
            // "#Offset");
            String[] thead = new String[] { "#Message ID", "#QID", "#Offset" };
            int row = queryResult.getMessageList().size();
            Table table = new Table(thead, row);

            for (MessageExt msg : queryResult.getMessageList()) {
                String[] data =
                        new String[] { msg.getMsgId(), String.valueOf(msg.getQueueId()),
                                      String.valueOf(msg.getQueueOffset()) };
                table.insertTR(data);
                // System.out.printf("%-50s %-4d %d\n", msg.getMsgId(),
                // msg.getQueueId(), msg.getQueueOffset());
            }
            return table;
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

    static final QueryMsgByOffsetSubCommand queryMsgByOffsetSubCommand = new QueryMsgByOffsetSubCommand();


    public Collection<Option> getOptionsForQueryMsgByOffset() {
        return getOptions(queryMsgByOffsetSubCommand);
    }


    @CmdTrace(cmdClazz = QueryMsgByOffsetSubCommand.class)
    public Table queryMsgByOffset(String topicName, String brokerName, String queueId, String offset)
            throws Throwable {
        Throwable t = null;
        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP);

        defaultMQPullConsumer.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topicName);
            mq.setBrokerName(brokerName);
            mq.setQueueId(Integer.parseInt(queueId));

            defaultMQPullConsumer.start();

            PullResult pullResult = defaultMQPullConsumer.pull(mq, "*", Long.parseLong(offset), 1);
            if (pullResult != null) {
                switch (pullResult.getPullStatus()) {
                case FOUND:
                    Table table = queryMsgById(pullResult.getMsgFoundList().get(0).getMsgId());
                    return table;
                case NO_MATCHED_MSG:
                case NO_NEW_MSG:
                case OFFSET_ILLEGAL:
                default:
                    break;
                }
            }
            else {
                throw new IllegalStateException("pullResult is null");
            }
        }
        catch (Throwable e) {
            logger.error(e.getMessage(), e);
            t = e;
        }
        finally {
            defaultMQPullConsumer.shutdown();
        }
        throw t;
    }
}
