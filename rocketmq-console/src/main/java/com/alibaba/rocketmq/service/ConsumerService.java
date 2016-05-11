package com.alibaba.rocketmq.service;

import static com.alibaba.rocketmq.common.Tool.str;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.OffsetWrapper;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.consumer.ConsumerProgressSubCommand;
import com.alibaba.rocketmq.tools.command.consumer.DeleteSubscriptionGroupCommand;
import com.alibaba.rocketmq.tools.command.consumer.UpdateSubGroupSubCommand;
import com.alibaba.rocketmq.validate.CmdTrace;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-18
 */
@Service
public class ConsumerService extends AbstractService {

    static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    static final ConsumerProgressSubCommand consumerProgressSubCommand = new ConsumerProgressSubCommand();


    public Collection<Option> getOptionsForConsumerProgress() {
        return getOptions(consumerProgressSubCommand);
    }


    @CmdTrace(cmdClazz = ConsumerProgressSubCommand.class)
    public Table consumerProgress(String consumerGroup) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            if (isNotBlank(consumerGroup)) {
                ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);

                List<MessageQueue> mqList = new LinkedList<MessageQueue>();
                mqList.addAll(consumeStats.getOffsetTable().keySet());
                Collections.sort(mqList);
                // System.out.printf("%-32s  %-32s  %-4s  %-20s  %-20s  %s\n",//
                // "#Topic",//
                // "#Broker Name",//
                // "#QID",//
                // "#Broker Offset",//
                // "#Consumer Offset",//
                // "#Diff" //
                // );
                String[] thead =
                        new String[] { "#Topic", "#Broker Name", "#QID", "#Broker Offset",
                                      "#Consumer Offset", "#Diff" };
                long diffTotal = 0L;
                Table table = new Table(thead, mqList.size());
                for (MessageQueue mq : mqList) {
                    OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);

                    long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
                    diffTotal += diff;

                    // System.out.printf("%-32s  %-32s  %-4d  %-20d  %-20d  %d\n",//
                    // UtilAll.frontStringAtLeast(mq.getTopic(), 32),//
                    // UtilAll.frontStringAtLeast(mq.getBrokerName(), 32),//
                    // mq.getQueueId(),//
                    // offsetWrapper.getBrokerOffset(),//
                    // offsetWrapper.getConsumerOffset(),//
                    // diff //
                    // );
                    Object[] tr = table.createTR();
                    tr[0] = UtilAll.frontStringAtLeast(mq.getTopic(), 32);
                    tr[1] = UtilAll.frontStringAtLeast(mq.getBrokerName(), 32);
                    tr[2] = str(mq.getQueueId());
                    tr[3] = str(offsetWrapper.getBrokerOffset());
                    tr[4] = str(offsetWrapper.getConsumerOffset());
                    tr[5] = str(diff);

                    table.insertTR(tr);
                }

                // System.out.println("");
                // System.out.printf("Consume TPS: %d\n",
                // consumeStats.getConsumeTps());
                // System.out.printf("Diff Total: %d\n", diffTotal);

                table.addExtData("Consume TPS:", str(consumeStats.getConsumeTps()));
                table.addExtData("Diff Total:", str(diffTotal));

                return table;
            }
            else {
                // System.out.printf("%-32s  %-6s  %-24s %-5s  %-14s  %-7s  %s\n",//
                // "#Group",//
                // "#Count",//
                // "#Version",//
                // "#Type",//
                // "#Model",//
                // "#TPS",//
                // "#Diff Total"//
                // );

                String[] thead =
                        new String[] { "#Group", "#Count", "#Version", "#Type", "#Model", "#TPS",
                                      "#Diff Total" };

                List<GroupConsumeInfo> groupConsumeInfoList = new LinkedList<GroupConsumeInfo>();
                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        String tconsumerGroup = topic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());

                        try {
                            ConsumeStats consumeStats = null;
                            try {
                                consumeStats = defaultMQAdminExt.examineConsumeStats(tconsumerGroup);
                            }
                            catch (Exception e) {
                                logger.warn("examineConsumeStats exception, " + tconsumerGroup, e);
                            }

                            ConsumerConnection cc = null;
                            try {
                                cc = defaultMQAdminExt.examineConsumerConnectionInfo(tconsumerGroup);
                            }
                            catch (Exception e) {
                                logger.warn("examineConsumerConnectionInfo exception, " + tconsumerGroup, e);
                            }

                            GroupConsumeInfo groupConsumeInfo = new GroupConsumeInfo();
                            groupConsumeInfo.setGroup(tconsumerGroup);

                            if (consumeStats != null) {
                                groupConsumeInfo.setConsumeTps((int) consumeStats.getConsumeTps());
                                groupConsumeInfo.setDiffTotal(consumeStats.computeTotalDiff());
                            }

                            if (cc != null) {
                                groupConsumeInfo.setCount(cc.getConnectionSet().size());
                                groupConsumeInfo.setMessageModel(cc.getMessageModel());
                                groupConsumeInfo.setConsumeType(cc.getConsumeType());
                                groupConsumeInfo.setVersion(cc.computeMinVersion());
                            }

                            groupConsumeInfoList.add(groupConsumeInfo);
                        }
                        catch (Exception e) {
                            logger.warn("examineConsumeStats or examineConsumerConnectionInfo exception, "
                                    + tconsumerGroup, e);
                        }
                        Collections.sort(groupConsumeInfoList);

                        Table table = new Table(thead, groupConsumeInfoList.size());
                        for (GroupConsumeInfo info : groupConsumeInfoList) {
                            // System.out.printf("%-32s  %-6d  %-24s %-5s  %-14s  %-7d  %d\n",//
                            // UtilAll.frontStringAtLeast(info.getGroup(),
                            // 32),//
                            // info.getCount(),//
                            // info.versionDesc(),//
                            // info.consumeTypeDesc(),//
                            // info.messageModelDesc(),//
                            // info.getConsumeTps(),//
                            // info.getDiffTotal()//
                            // );
                            Object[] tr = table.createTR();
                            tr[0] = UtilAll.frontStringAtLeast(info.getGroup(), 32);
                            tr[1] = str(info.getCount());
                            tr[2] = info.versionDesc();
                            tr[3] = info.consumeTypeDesc();
                            tr[4] = info.messageModelDesc();
                            tr[5] = str(info.getConsumeTps());
                            tr[6] = str(info.getDiffTotal());
                            table.insertTR(tr);
                        }
                        return table;
                    }
                }
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

    static final DeleteSubscriptionGroupCommand deleteSubscriptionGroupCommand =
            new DeleteSubscriptionGroupCommand();


    public Collection<Option> getOptionsForDeleteSubGroup() {
        return getOptions(deleteSubscriptionGroupCommand);
    }


    @CmdTrace(cmdClazz = DeleteSubscriptionGroupCommand.class)
    public boolean deleteSubGroup(String groupName, String brokerAddr, String clusterName) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt adminExt = getDefaultMQAdminExt();
        try {
            if (isNotBlank(brokerAddr)) {
                adminExt.start();

                adminExt.deleteSubscriptionGroup(brokerAddr, groupName);
                // System.out.printf("delete subscription group [%s] from broker [%s] success.\n",
                // groupName,addr);

                return true;
            }
            else if (isNotBlank(clusterName)) {
                adminExt.start();

                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
                for (String master : masterSet) {
                    adminExt.deleteSubscriptionGroup(master, groupName);
                    // System.out.printf(
                    // "delete subscription group [%s] from broker [%s] in cluster [%s] success.\n",
                    // groupName, master, clusterName);
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
            shutdownDefaultMQAdminExt(adminExt);
        }
        throw t;
    }

    static final UpdateSubGroupSubCommand updateSubGroupSubCommand = new UpdateSubGroupSubCommand();


    public Collection<Option> getOptionsForUpdateSubGroup() {
        return getOptions(updateSubGroupSubCommand);
    }


    @CmdTrace(cmdClazz = UpdateSubGroupSubCommand.class)
    public boolean updateSubGroup(String brokerAddr, String clusterName, String groupName,
            String consumeEnable, String consumeFromMinEnable, String consumeBroadcastEnable,
            String retryQueueNums, String retryMaxTimes, String brokerId, String whichBrokerWhenConsumeSlowly)
            throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();

        try {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setConsumeBroadcastEnable(false);
            subscriptionGroupConfig.setConsumeFromMinEnable(false);

            // groupName
            subscriptionGroupConfig.setGroupName(groupName);

            // consumeEnable
            if (isNotBlank(consumeEnable)) {
                subscriptionGroupConfig.setConsumeEnable(Boolean.parseBoolean(consumeEnable.trim()));
            }

            // consumeFromMinEnable
            if (isNotBlank(consumeFromMinEnable)) {
                subscriptionGroupConfig.setConsumeFromMinEnable(Boolean.parseBoolean(consumeFromMinEnable
                    .trim()));
            }

            // consumeBroadcastEnable
            if (isNotBlank(consumeBroadcastEnable)) {
                subscriptionGroupConfig.setConsumeBroadcastEnable(Boolean.parseBoolean(consumeBroadcastEnable
                    .trim()));
            }

            // retryQueueNums
            if (isNotBlank(retryQueueNums)) {
                subscriptionGroupConfig.setRetryQueueNums(Integer.parseInt(retryQueueNums.trim()));
            }

            // retryMaxTimes
            if (isNotBlank(retryMaxTimes)) {
                subscriptionGroupConfig.setRetryMaxTimes(Integer.parseInt(retryMaxTimes.trim()));
            }

            // brokerId
            if (isNotBlank(brokerId)) {
                subscriptionGroupConfig.setBrokerId(Long.parseLong(brokerId.trim()));
            }

            // whichBrokerWhenConsumeSlowly
            if (isNotBlank(whichBrokerWhenConsumeSlowly)) {
                subscriptionGroupConfig.setWhichBrokerWhenConsumeSlowly(Long
                    .parseLong(whichBrokerWhenConsumeSlowly.trim()));
            }

            if (isNotBlank(brokerAddr)) {

                defaultMQAdminExt.start();

                defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(brokerAddr, subscriptionGroupConfig);
                // System.out.printf("create subscription group to %s success.\n",
                // addr);
                // System.out.println(subscriptionGroupConfig);
                return true;

            }
            else if (isNotBlank(clusterName)) {

                defaultMQAdminExt.start();

                Set<String> masterSet =
                        CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                    // System.out.printf("create subscription group to %s success.\n",
                    // addr);
                }
                // System.out.println(subscriptionGroupConfig);
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


class GroupConsumeInfo implements Comparable<GroupConsumeInfo> {
    private String group;
    private int version;
    private int count;
    private ConsumeType consumeType;
    private MessageModel messageModel;
    private int consumeTps;
    private long diffTotal;


    public String getGroup() {
        return group;
    }


    public String consumeTypeDesc() {
        if (this.count != 0) {
            return this.getConsumeType() == ConsumeType.CONSUME_ACTIVELY ? "PULL" : "PUSH";
        }
        return "";
    }


    public String messageModelDesc() {
        if (this.count != 0 && this.getConsumeType() == ConsumeType.CONSUME_PASSIVELY) {
            return this.getMessageModel().toString();
        }
        return "";
    }


    public String versionDesc() {
        if (this.count != 0) {
            return MQVersion.getVersionDesc(this.version);
        }
        return "";
    }


    public void setGroup(String group) {
        this.group = group;
    }


    public int getCount() {
        return count;
    }


    public void setCount(int count) {
        this.count = count;
    }


    public ConsumeType getConsumeType() {
        return consumeType;
    }


    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }


    public MessageModel getMessageModel() {
        return messageModel;
    }


    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }


    public long getDiffTotal() {
        return diffTotal;
    }


    public void setDiffTotal(long diffTotal) {
        this.diffTotal = diffTotal;
    }


    @Override
    public int compareTo(GroupConsumeInfo o) {
        if (this.count != o.count) {
            return o.count - this.count;
        }

        return (int) (o.diffTotal - diffTotal);
    }


    public int getConsumeTps() {
        return consumeTps;
    }


    public void setConsumeTps(int consumeTps) {
        this.consumeTps = consumeTps;
    }


    public int getVersion() {
        return version;
    }


    public void setVersion(int version) {
        this.version = version;
    }
}
