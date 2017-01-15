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
package com.alibaba.rocketmq.broker.processor;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.broker.client.ConsumerGroupInfo;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.OffsetWrapper;
import com.alibaba.rocketmq.common.admin.TopicOffset;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageId;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.*;
import com.alibaba.rocketmq.common.protocol.header.*;
import com.alibaba.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerResponseHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.stats.StatsItem;
import com.alibaba.rocketmq.common.stats.StatsSnapshot;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.LanguageCode;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author shijia.wxr
 * @author manhong.yqd
 */
public class AdminBrokerProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerController brokerController;


    public AdminBrokerProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return this.updateAndCreateTopic(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_BROKER:
                return this.deleteTopic(ctx, request);

            case RequestCode.GET_ALL_TOPIC_CONFIG:
                return this.getAllTopicConfig(ctx, request);


            case RequestCode.UPDATE_BROKER_CONFIG:
                return this.updateBrokerConfig(ctx, request);

            case RequestCode.GET_BROKER_CONFIG:
                return this.getBrokerConfig(ctx, request);

            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
                return this.searchOffsetByTimestamp(ctx, request);
            case RequestCode.GET_MAX_OFFSET:
                return this.getMaxOffset(ctx, request);
            case RequestCode.GET_MIN_OFFSET:
                return this.getMinOffset(ctx, request);
            case RequestCode.GET_EARLIEST_MSG_STORETIME:
                return this.getEarliestMsgStoretime(ctx, request);


            case RequestCode.GET_BROKER_RUNTIME_INFO:
                return this.getBrokerRuntimeInfo(ctx, request);


            case RequestCode.LOCK_BATCH_MQ:
                return this.lockBatchMQ(ctx, request);
            case RequestCode.UNLOCK_BATCH_MQ:
                return this.unlockBatchMQ(ctx, request);


            case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
                return this.updateAndCreateSubscriptionGroup(ctx, request);
            case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
                return this.getAllSubscriptionGroup(ctx, request);
            case RequestCode.DELETE_SUBSCRIPTIONGROUP:
                return this.deleteSubscriptionGroup(ctx, request);


            case RequestCode.GET_TOPIC_STATS_INFO:
                return this.getTopicStatsInfo(ctx, request);


            case RequestCode.GET_CONSUMER_CONNECTION_LIST:
                return this.getConsumerConnectionList(ctx, request);

            case RequestCode.GET_PRODUCER_CONNECTION_LIST:
                return this.getProducerConnectionList(ctx, request);

            case RequestCode.GET_CONSUME_STATS:
                return this.getConsumeStats(ctx, request);
            case RequestCode.GET_ALL_CONSUMER_OFFSET:
                return this.getAllConsumerOffset(ctx, request);


            case RequestCode.GET_ALL_DELAY_OFFSET:
                return this.getAllDelayOffset(ctx, request);

            case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
                return this.resetOffset(ctx, request);


            case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
                return this.getConsumerStatus(ctx, request);


            case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
                return this.queryTopicConsumeByWho(ctx, request);

            case RequestCode.REGISTER_FILTER_SERVER:
                return this.registerFilterServer(ctx, request);

            case RequestCode.QUERY_CONSUME_TIME_SPAN:
                return this.queryConsumeTimeSpan(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
                return this.getSystemTopicListFromBroker(ctx, request);


            case RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE:
                return this.cleanExpiredConsumeQueue();
            case RequestCode.CLEAN_UNUSED_TOPIC:
                return this.cleanUnusedTopic();

            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);

            case RequestCode.QUERY_CORRECTION_OFFSET:
                return this.queryCorrectionOffset(ctx, request);

            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            case RequestCode.CLONE_GROUP_OFFSET:
                return this.cloneGroupOffset(ctx, request);


            case RequestCode.VIEW_BROKER_STATS_DATA:
                return ViewBrokerStatsData(ctx, request);

            case RequestCode.GET_BROKER_CONSUME_STATS:
                return fetchAllConsumeStatsInBroker(ctx, request);
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final CreateTopicRequestHeader requestHeader =
                (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);
        log.info("updateAndCreateTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));


        if (requestHeader.getTopic().equals(this.brokerController.getBrokerConfig().getBrokerClusterName())) {
            String errorMsg = "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorMsg);
            return response;
        }

        try {
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            response.markResponseType();
            response.setRemark(null);
            ctx.writeAndFlush(response);
        } catch (Exception e) {
        }

        TopicConfig topicConfig = new TopicConfig(requestHeader.getTopic());
        topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
        topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
        topicConfig.setTopicFilterType(requestHeader.getTopicFilterTypeEnum());
        topicConfig.setPerm(requestHeader.getPerm());
        topicConfig.setTopicSysFlag(requestHeader.getTopicSysFlag() == null ? 0 : requestHeader.getTopicSysFlag());

        this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);
        this.brokerController.registerBrokerAll(false, true);
        return null;
    }

    private RemotingCommand deleteTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DeleteTopicRequestHeader requestHeader =
                (DeleteTopicRequestHeader) request.decodeCommandCustomHeader(DeleteTopicRequestHeader.class);

        log.info("deleteTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getTopicConfigManager().deleteTopicConfig(requestHeader.getTopic());
        this.brokerController.getMessageStore()
                .cleanUnusedTopic(this.brokerController.getTopicConfigManager().getTopicConfigTable().keySet());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllTopicConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetAllTopicConfigResponseHeader.class);
        // final GetAllTopicConfigResponseHeader responseHeader =
        // (GetAllTopicConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerController.getTopicConfigManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No topic in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No topic in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand updateBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        log.info("updateBrokerConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                Properties properties = MixAll.string2Properties(bodyStr);
                if (properties != null) {
                    log.info("updateBrokerConfig, new config: " + properties + " client: " + ctx.channel().remoteAddress());
                    this.brokerController.updateAllConfig(properties);
                    if (properties.containsKey("brokerPermission")) {
                        this.brokerController.registerBrokerAll(false, false);
                        this.brokerController.getTopicConfigManager().getDataVersion().nextVersion();
                    }
                } else {
                    log.error("string2Properties error");
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }
            } catch (UnsupportedEncodingException e) {
                log.error("", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {

        final RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader = (GetBrokerConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerController.encodeAllConfig();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        responseHeader.setVersion(this.brokerController.getConfigDataVersion());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand searchOffsetByTimestamp(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
        final SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();
        final SearchOffsetRequestHeader requestHeader =
                (SearchOffsetRequestHeader) request.decodeCommandCustomHeader(SearchOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getOffsetInQueueByTime(requestHeader.getTopic(), requestHeader.getQueueId(),
                requestHeader.getTimestamp());

        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getMaxOffset(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
        final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
        final GetMaxOffsetRequestHeader requestHeader =
                (GetMaxOffsetRequestHeader) request.decodeCommandCustomHeader(GetMaxOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getMaxOffsetInQuque(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getMinOffset(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetMinOffsetResponseHeader.class);
        final GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.readCustomHeader();
        final GetMinOffsetRequestHeader requestHeader =
                (GetMinOffsetRequestHeader) request.decodeCommandCustomHeader(GetMinOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getMinOffsetInQuque(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setOffset(offset);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getEarliestMsgStoretime(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetEarliestMsgStoretimeResponseHeader.class);
        final GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.readCustomHeader();
        final GetEarliestMsgStoretimeRequestHeader requestHeader =
                (GetEarliestMsgStoretimeRequestHeader) request.decodeCommandCustomHeader(GetEarliestMsgStoretimeRequestHeader.class);

        long timestamp =
                this.brokerController.getMessageStore().getEarliestMessageTime(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setTimestamp(timestamp);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getBrokerRuntimeInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        HashMap<String, String> runtimeInfo = this.prepareRuntimeInfo();
        KVTable kvTable = new KVTable();
        kvTable.setTable(runtimeInfo);

        byte[] body = kvTable.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand lockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LockBatchRequestBody requestBody = LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);

        Set<MessageQueue> lockOKMQSet = this.brokerController.getRebalanceLockManager().tryLockBatch(//
                requestBody.getConsumerGroup(), //
                requestBody.getMqSet(), //
                requestBody.getClientId());

        LockBatchResponseBody responseBody = new LockBatchResponseBody();
        responseBody.setLockOKMQSet(lockOKMQSet);

        response.setBody(responseBody.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        UnlockBatchRequestBody requestBody = UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);

        this.brokerController.getRebalanceLockManager().unlockBatch(//
                requestBody.getConsumerGroup(), //
                requestBody.getMqSet(), //
                requestBody.getClientId());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand updateAndCreateSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        log.info("updateAndCreateSubscriptionGroup called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        SubscriptionGroupConfig config = RemotingSerializable.decode(request.getBody(), SubscriptionGroupConfig.class);
        if (config != null) {
            this.brokerController.getSubscriptionGroupManager().updateSubscriptionGroupConfig(config);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        String content = this.brokerController.getSubscriptionGroupManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No subscription group in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No subscription group in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand deleteSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DeleteSubscriptionGroupRequestHeader requestHeader =
                (DeleteSubscriptionGroupRequestHeader) request.decodeCommandCustomHeader(DeleteSubscriptionGroupRequestHeader.class);

        log.info("deleteSubscriptionGroup called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getSubscriptionGroupManager().deleteSubscriptionGroupConfig(requestHeader.getGroupName());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getTopicStatsInfo(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetTopicStatsInfoRequestHeader requestHeader =
                (GetTopicStatsInfoRequestHeader) request.decodeCommandCustomHeader(GetTopicStatsInfoRequestHeader.class);

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        TopicStatsTable topicStatsTable = new TopicStatsTable();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);

            TopicOffset topicOffset = new TopicOffset();
            long min = this.brokerController.getMessageStore().getMinOffsetInQuque(topic, i);
            if (min < 0)
                min = 0;

            long max = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
            if (max < 0)
                max = 0;

            long timestamp = 0;
            if (max > 0) {
                timestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, (max - 1));
            }

            topicOffset.setMinOffset(min);
            topicOffset.setMaxOffset(max);
            topicOffset.setLastUpdateTimestamp(timestamp);

            topicStatsTable.getOffsetTable().put(mq, topicOffset);
        }

        byte[] body = topicStatsTable.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConsumerConnectionList(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerConnectionListRequestHeader requestHeader =
                (GetConsumerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetConsumerConnectionListRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            ConsumerConnection bodydata = new ConsumerConnection();
            bodydata.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
            bodydata.setConsumeType(consumerGroupInfo.getConsumeType());
            bodydata.setMessageModel(consumerGroupInfo.getMessageModel());
            bodydata.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());

            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = consumerGroupInfo.getChannelInfoTable().entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            return response;
        }

        response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
        response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] not online");
        return response;
    }

    private RemotingCommand getProducerConnectionList(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetProducerConnectionListRequestHeader requestHeader =
                (GetProducerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetProducerConnectionListRequestHeader.class);

        ProducerConnection bodydata = new ProducerConnection();
        HashMap<Channel, ClientChannelInfo> channelInfoHashMap =
                this.brokerController.getProducerManager().getGroupChannelTable().get(requestHeader.getProducerGroup());
        if (channelInfoHashMap != null) {
            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = channelInfoHashMap.entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("the producer group[" + requestHeader.getProducerGroup() + "] not exist");
        return response;
    }

    private RemotingCommand getConsumeStats(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumeStatsRequestHeader requestHeader =
                (GetConsumeStatsRequestHeader) request.decodeCommandCustomHeader(GetConsumeStatsRequestHeader.class);

        ConsumeStats consumeStats = new ConsumeStats();

        Set<String> topics = new HashSet<String>();
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getConsumerGroup());
        } else {
            topics.add(requestHeader.getTopic());
        }

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("consumeStats, topic config not exist, {}", topic);
                continue;
            }

            /**

             */
            {
                SubscriptionData findSubscriptionData =
                        this.brokerController.getConsumerManager().findSubscriptionData(requestHeader.getConsumerGroup(), topic);

                if (null == findSubscriptionData //
                        && this.brokerController.getConsumerManager().findSubscriptionDataCount(requestHeader.getConsumerGroup()) > 0) {
                    log.warn("consumeStats, the consumer group[{}], topic[{}] not exist", requestHeader.getConsumerGroup(), topic);
                    continue;
                }
            }

            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);

                OffsetWrapper offsetWrapper = new OffsetWrapper();

                long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
                if (brokerOffset < 0)
                    brokerOffset = 0;

                long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(//
                        requestHeader.getConsumerGroup(), //
                        topic, //
                        i);
                if (consumerOffset < 0)
                    consumerOffset = 0;

                offsetWrapper.setBrokerOffset(brokerOffset);
                offsetWrapper.setConsumerOffset(consumerOffset);


                long timeOffset = consumerOffset - 1;
                if (timeOffset >= 0) {
                    long lastTimestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, timeOffset);
                    if (lastTimestamp > 0) {
                        offsetWrapper.setLastTimestamp(lastTimestamp);
                    }
                }

                consumeStats.getOffsetTable().put(mq, offsetWrapper);
            }

            double consumeTps = this.brokerController.getBrokerStatsManager().tpsGroupGetNums(requestHeader.getConsumerGroup(), topic);

            consumeTps += consumeStats.getConsumeTps();
            consumeStats.setConsumeTps(consumeTps);
        }

        byte[] body = consumeStats.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = this.brokerController.getConsumerOffsetManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("get all consumer offset from master error.", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No consumer offset in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No consumer offset in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand getAllDelayOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = ((DefaultMessageStore) this.brokerController.getMessageStore()).getScheduleMessageService().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("get all delay offset from master error.", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No delay offset in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No delay offset in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    public RemotingCommand resetOffset(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
                (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        log.info("[reset-offset] reset offset started by {}. topic={}, group={}, timestamp={}, isForce={}",
                new Object[]{RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(),
                        requestHeader.getTimestamp(), requestHeader.isForce()});
        boolean isC = false;
        LanguageCode language = request.getLanguage();
        switch (language) {
            case CPP:
                isC = true;
                break;
        }
        return this.brokerController.getBroker2Client().resetOffset(requestHeader.getTopic(), requestHeader.getGroup(),
                requestHeader.getTimestamp(), requestHeader.isForce(), isC);
    }

    public RemotingCommand getConsumerStatus(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final GetConsumerStatusRequestHeader requestHeader =
                (GetConsumerStatusRequestHeader) request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        log.info("[get-consumer-status] get consumer status by {}. topic={}, group={}",
                new Object[]{RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup()});

        return this.brokerController.getBroker2Client().getConsumeStatus(requestHeader.getTopic(), requestHeader.getGroup(),
                requestHeader.getClientAddr());
    }

    private RemotingCommand queryTopicConsumeByWho(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryTopicConsumeByWhoRequestHeader requestHeader =
                (QueryTopicConsumeByWhoRequestHeader) request.decodeCommandCustomHeader(QueryTopicConsumeByWhoRequestHeader.class);


        HashSet<String> groups = this.brokerController.getConsumerManager().queryTopicConsumeByWho(requestHeader.getTopic());

        Set<String> groupInOffset = this.brokerController.getConsumerOffsetManager().whichGroupByTopic(requestHeader.getTopic());
        if (groupInOffset != null && !groupInOffset.isEmpty()) {
            groups.addAll(groupInOffset);
        }

        GroupList groupList = new GroupList();
        groupList.setGroupList(groups);
        byte[] body = groupList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand registerFilterServer(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterFilterServerResponseHeader.class);
        final RegisterFilterServerResponseHeader responseHeader = (RegisterFilterServerResponseHeader) response.readCustomHeader();
        final RegisterFilterServerRequestHeader requestHeader =
                (RegisterFilterServerRequestHeader) request.decodeCommandCustomHeader(RegisterFilterServerRequestHeader.class);

        this.brokerController.getFilterServerManager().registerFilterServer(ctx.channel(), requestHeader.getFilterServerAddr());

        responseHeader.setBrokerId(this.brokerController.getBrokerConfig().getBrokerId());
        responseHeader.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand queryConsumeTimeSpan(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryConsumeTimeSpanRequestHeader requestHeader =
                (QueryConsumeTimeSpanRequestHeader) request.decodeCommandCustomHeader(QueryConsumeTimeSpanRequestHeader.class);

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        List<QueueTimeSpan> timeSpanSet = new ArrayList<QueueTimeSpan>();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            QueueTimeSpan timeSpan = new QueueTimeSpan();
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);
            timeSpan.setMessageQueue(mq);

            long minTime = this.brokerController.getMessageStore().getEarliestMessageTime(topic, i);
            timeSpan.setMinTimeStamp(minTime);

            long max = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
            long maxTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, (max - 1));
            timeSpan.setMaxTimeStamp(maxTime);

            long consumeTime;
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(//
                    requestHeader.getGroup(), topic, i);
            if (consumerOffset > 0) {
                consumeTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, consumerOffset - 1);
            } else {
                consumeTime = minTime;
            }
            timeSpan.setConsumeTimeStamp(consumeTime);

            long maxBrokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(requestHeader.getTopic(), i);
            if (consumerOffset < maxBrokerOffset) {
                long nextTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, consumerOffset);
                timeSpan.setDelayTime(System.currentTimeMillis() - nextTime);
            }
            timeSpanSet.add(timeSpan);
        }

        QueryConsumeTimeSpanBody queryConsumeTimeSpanBody = new QueryConsumeTimeSpanBody();
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(timeSpanSet);
        response.setBody(queryConsumeTimeSpanBody.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getSystemTopicListFromBroker(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        Set<String> topics = this.brokerController.getTopicConfigManager().getSystemTopic();
        TopicList topicList = new TopicList();
        topicList.setTopicList(topics);
        response.setBody(topicList.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand cleanExpiredConsumeQueue() {
        log.warn("invoke cleanExpiredConsumeQueue start.");
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        brokerController.getMessageStore().cleanExpiredConsumerQueue();
        log.warn("invoke cleanExpiredConsumeQueue end.");
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand cleanUnusedTopic() {
        log.warn("invoke cleanUnusedTopic start.");
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        brokerController.getMessageStore().cleanUnusedTopic(brokerController.getTopicConfigManager().getTopicConfigTable().keySet());
        log.warn("invoke cleanUnusedTopic end.");
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**

     */
    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final GetConsumerRunningInfoRequestHeader requestHeader =
                (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        return this.callConsumer(RequestCode.GET_CONSUMER_RUNNING_INFO, request, requestHeader.getConsumerGroup(),
                requestHeader.getClientId());
    }

    private RemotingCommand queryCorrectionOffset(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryCorrectionOffsetHeader requestHeader =
                (QueryCorrectionOffsetHeader) request.decodeCommandCustomHeader(QueryCorrectionOffsetHeader.class);

        Map<Integer, Long> correctionOffset = this.brokerController.getConsumerOffsetManager()
                .queryMinOffsetInAllGroup(requestHeader.getTopic(), requestHeader.getFilterGroups());

        Map<Integer, Long> compareOffset =
                this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getTopic(), requestHeader.getCompareGroup());

        if (compareOffset != null && !compareOffset.isEmpty()) {
            for(Map.Entry<Integer, Long> entry: compareOffset.entrySet()){
                Integer queueId = entry.getKey();
                correctionOffset.put(queueId,
                        correctionOffset.get(queueId) > entry.getValue() ? Long.MAX_VALUE : correctionOffset.get(queueId));
            }
        }

        QueryCorrectionOffsetBody body = new QueryCorrectionOffsetBody();
        body.setCorrectionOffsets(correctionOffset);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final ConsumeMessageDirectlyResultRequestHeader requestHeader = (ConsumeMessageDirectlyResultRequestHeader) request
                .decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        request.getExtFields().put("brokerName", this.brokerController.getBrokerConfig().getBrokerName());
        SelectMapedBufferResult selectMapedBufferResult = null;
        try {
            MessageId messageId = MessageDecoder.decodeMessageId(requestHeader.getMsgId());
            selectMapedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(messageId.getOffset());

            byte[] body = new byte[selectMapedBufferResult.getSize()];
            selectMapedBufferResult.getByteBuffer().get(body);
            request.setBody(body);
        } catch (UnknownHostException e) {
        } finally {
            if (selectMapedBufferResult != null) {
                selectMapedBufferResult.release();
            }
        }

        return this.callConsumer(RequestCode.CONSUME_MESSAGE_DIRECTLY, request, requestHeader.getConsumerGroup(),
                requestHeader.getClientId());
    }

    private RemotingCommand cloneGroupOffset(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        CloneGroupOffsetRequestHeader requestHeader =
                (CloneGroupOffsetRequestHeader) request.decodeCommandCustomHeader(CloneGroupOffsetRequestHeader.class);

        Set<String> topics;
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getSrcGroup());
        } else {
            topics = new HashSet<String>();
            topics.add(requestHeader.getTopic());
        }

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("[cloneGroupOffset], topic config not exist, {}", topic);
                continue;
            }

            /**

             */
            if (!requestHeader.isOffline()) {

                SubscriptionData findSubscriptionData =
                        this.brokerController.getConsumerManager().findSubscriptionData(requestHeader.getSrcGroup(), topic);
                if (this.brokerController.getConsumerManager().findSubscriptionDataCount(requestHeader.getSrcGroup()) > 0
                        && findSubscriptionData == null) {
                    log.warn("[cloneGroupOffset], the consumer group[{}], topic[{}] not exist", requestHeader.getSrcGroup(), topic);
                    continue;
                }
            }

            this.brokerController.getConsumerOffsetManager().cloneOffset(requestHeader.getSrcGroup(), requestHeader.getDestGroup(),
                    requestHeader.getTopic());
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand ViewBrokerStatsData(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final ViewBrokerStatsDataRequestHeader requestHeader =
                (ViewBrokerStatsDataRequestHeader) request.decodeCommandCustomHeader(ViewBrokerStatsDataRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DefaultMessageStore messageStore = (DefaultMessageStore) this.brokerController.getMessageStore();

        StatsItem statsItem = messageStore.getBrokerStatsManager().getStatsItem(requestHeader.getStatsName(), requestHeader.getStatsKey());
        if (null == statsItem) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The stats <%s> <%s> not exist", requestHeader.getStatsName(), requestHeader.getStatsKey()));
            return response;
        }

        BrokerStatsData brokerStatsData = new BrokerStatsData();

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInMinute();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsMinute(it);
        }


        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInHour();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsHour(it);
        }


        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInDay();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsDay(it);
        }

        response.setBody(brokerStatsData.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand fetchAllConsumeStatsInBroker(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        GetConsumeStatsInBrokerHeader requestHeader =
                (GetConsumeStatsInBrokerHeader) request.decodeCommandCustomHeader(GetConsumeStatsInBrokerHeader.class);
        boolean isOrder = requestHeader.isOrder();
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroups =
                brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable();

        List<Map<String/* subscriptionGroupName */, List<ConsumeStats>>> brokerConsumeStatsList =
                new ArrayList<Map<String, List<ConsumeStats>>>();

        long totalDiff = 0L;

        for (String group : subscriptionGroups.keySet()) {
            Map<String, List<ConsumeStats>> subscripTopicConsumeMap = new HashMap<String, List<ConsumeStats>>();
            Set<String> topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(group);
            List<ConsumeStats> consumeStatsList = new ArrayList<ConsumeStats>();
            for (String topic : topics) {
                ConsumeStats consumeStats = new ConsumeStats();
                TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
                if (null == topicConfig) {
                    log.warn("consumeStats, topic config not exist, {}", topic);
                    continue;
                }

                if (isOrder && !topicConfig.isOrder()) {
                    continue;
                }
                /**

                 */
                {
                    SubscriptionData findSubscriptionData = this.brokerController.getConsumerManager().findSubscriptionData(group, topic);

                    if (null == findSubscriptionData //
                            && this.brokerController.getConsumerManager().findSubscriptionDataCount(group) > 0) {
                        log.warn("consumeStats, the consumer group[{}], topic[{}] not exist", group, topic);
                        continue;
                    }
                }

                for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(topic);
                    mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                    mq.setQueueId(i);
                    OffsetWrapper offsetWrapper = new OffsetWrapper();
                    long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
                    if (brokerOffset < 0)
                        brokerOffset = 0;
                    long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(//
                            group, //
                            topic, //
                            i);
                    if (consumerOffset < 0)
                        consumerOffset = 0;

                    offsetWrapper.setBrokerOffset(brokerOffset);
                    offsetWrapper.setConsumerOffset(consumerOffset);


                    long timeOffset = consumerOffset - 1;
                    if (timeOffset >= 0) {
                        long lastTimestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, timeOffset);
                        if (lastTimestamp > 0) {
                            offsetWrapper.setLastTimestamp(lastTimestamp);
                        }
                    }
                    consumeStats.getOffsetTable().put(mq, offsetWrapper);
                }
                double consumeTps = this.brokerController.getBrokerStatsManager().tpsGroupGetNums(group, topic);
                consumeTps += consumeStats.getConsumeTps();
                consumeStats.setConsumeTps(consumeTps);
                totalDiff += consumeStats.computeTotalDiff();
                consumeStatsList.add(consumeStats);
            }
            subscripTopicConsumeMap.put(group, consumeStatsList);
            brokerConsumeStatsList.add(subscripTopicConsumeMap);
        }
        ConsumeStatsList consumeStats = new ConsumeStatsList();
        consumeStats.setBrokerAddr(brokerController.getBrokerAddr());
        consumeStats.setConsumeStatsList(brokerConsumeStatsList);
        consumeStats.setTotalDiff(totalDiff);
        response.setBody(consumeStats.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private HashMap<String, String> prepareRuntimeInfo() {
        HashMap<String, String> runtimeInfo = this.brokerController.getMessageStore().getRuntimeInfo();
        runtimeInfo.put("brokerVersionDesc", MQVersion.getVersionDesc(MQVersion.CurrentVersion));
        runtimeInfo.put("brokerVersion", String.valueOf(MQVersion.CurrentVersion));

        runtimeInfo.put("msgPutTotalYesterdayMorning",
                String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalYesterdayMorning()));
        runtimeInfo.put("msgPutTotalTodayMorning", String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayMorning()));
        runtimeInfo.put("msgPutTotalTodayNow", String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayNow()));

        runtimeInfo.put("msgGetTotalYesterdayMorning",
                String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalYesterdayMorning()));
        runtimeInfo.put("msgGetTotalTodayMorning", String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayMorning()));
        runtimeInfo.put("msgGetTotalTodayNow", String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayNow()));

        runtimeInfo.put("sendThreadPoolQueueSize", String.valueOf(this.brokerController.getSendThreadPoolQueue().size()));

        runtimeInfo.put("sendThreadPoolQueueCapacity",
                String.valueOf(this.brokerController.getBrokerConfig().getSendThreadPoolQueueCapacity()));

        runtimeInfo.put("pullThreadPoolQueueSize", String.valueOf(this.brokerController.getPullThreadPoolQueue().size()));
        runtimeInfo.put("pullThreadPoolQueueCapacity",
                String.valueOf(this.brokerController.getBrokerConfig().getPullThreadPoolQueueCapacity()));

        runtimeInfo.put("dispatchBehindBytes", String.valueOf(this.brokerController.getMessageStore().dispatchBehindBytes()));
        runtimeInfo.put("pageCacheLockTimeMills", String.valueOf(this.brokerController.getMessageStore().lockTimeMills()));

        runtimeInfo.put("sendThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4SendThreadPoolQueue()));
        runtimeInfo.put("pullThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4PullThreadPoolQueue()));
        runtimeInfo.put("earliestMessageTimeStamp", String.valueOf(this.brokerController.getMessageStore().getEarliestMessageTime()));
        runtimeInfo.put("startAcceptSendRequestTimeStamp", String.valueOf(this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp()));


        return runtimeInfo;
    }

    private RemotingCommand callConsumer(//
                                         final int requestCode, //
                                         final RemotingCommand request, //
                                         final String consumerGroup, //
                                         final String clientId) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ClientChannelInfo clientChannelInfo = this.brokerController.getConsumerManager().findChannel(consumerGroup, clientId);

        if (null == clientChannelInfo) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> <%s> not online", consumerGroup, clientId));
            return response;
        }

        if (clientChannelInfo.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> Version <%s> too low to finish, please upgrade it to V3_1_8_SNAPSHOT", //
                    clientId, //
                    MQVersion.getVersionDesc(clientChannelInfo.getVersion())));
            return response;
        }

        try {
            RemotingCommand newRequest = RemotingCommand.createRequestCommand(requestCode, null);
            newRequest.setExtFields(request.getExtFields());
            newRequest.setBody(request.getBody());

            RemotingCommand consumerResponse =
                    this.brokerController.getBroker2Client().callClient(clientChannelInfo.getChannel(), newRequest);
            return consumerResponse;
        } catch (RemotingTimeoutException e) {
            response.setCode(ResponseCode.CONSUME_MSG_TIMEOUT);
            response
                    .setRemark(String.format("consumer <%s> <%s> Timeout: %s", consumerGroup, clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        } catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(
                    String.format("invoke consumer <%s> <%s> Exception: %s", consumerGroup, clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        }
    }

}
