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
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;


/**
 * 管理Producer组及各个Producer连接
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class ProducerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final long LockTimeoutMillis = 3000;
    private static final long ChannelExpiredTimeout = 1000 * 120;
    private final Lock groupChannelLock = new ReentrantLock();
    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable =
            new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
    private final Lock hashcodeChannelLock = new ReentrantLock();
    private final HashMap<Integer /* group hash code */, List<ClientChannelInfo>> hashcodeChannelTable =
            new HashMap<Integer, List<ClientChannelInfo>>();
    private final Random random = new Random(System.currentTimeMillis());



    public ProducerManager() {
    }


    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable =
                new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
        try {
            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)){
                try {
                    newGroupChannelTable.putAll(groupChannelTable);
                } finally {
                    groupChannelLock.unlock();
                }
            }
        } catch (InterruptedException e) {
           log.error("",e);
        }
        return newGroupChannelTable;
    }


    public void scanNotActiveChannel() {
        try {
            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                        .entrySet()) {
                        final String group = entry.getKey();
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            final ClientChannelInfo info = item.getValue();

                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            if (diff > ChannelExpiredTimeout) {
                                it.remove();
                                log.warn(
                                    "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                                    RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                }
                finally {
                    this.groupChannelLock.unlock();
                }
            }
            else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }
    }


    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
                if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                    try {
                        for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                            final String group = entry.getKey();
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                                    entry.getValue();
                            final ClientChannelInfo clientChannelInfo =
                                    clientChannelInfoTable.remove(channel);
                            if (clientChannelInfo != null) {
                                log.info(
                                        "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                        clientChannelInfo.toString(), remoteAddr, group);
                            }

                        }
                    }
                    finally {
                        this.groupChannelLock.unlock();
                    }
                }
                else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            }
            catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }


    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            ClientChannelInfo clientChannelInfoFound = null;

            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null == channelTable) {
                        channelTable = new HashMap<Channel, ClientChannelInfo>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
                    if (null == clientChannelInfoFound) {
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        log.info("new producer connected, group: {} channel: {}", group,
                            clientChannelInfo.toString());
                    }
                }
                finally {
                    this.groupChannelLock.unlock();
                }

                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            }
            else {
                log.warn("ProducerManager registerProducer groupChannelLock timeout");
            }
            
            boolean bClientChannelInfoFound = false;
            
            if (this.hashcodeChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                	List<ClientChannelInfo> channelList = this.hashcodeChannelTable.get(group);
                    if (null == channelList) {
                    	channelList = new ArrayList<ClientChannelInfo>();
                        this.hashcodeChannelTable.put(group.hashCode(), channelList);
                    }

                    bClientChannelInfoFound = channelList.contains(clientChannelInfo);
                    if (!bClientChannelInfoFound) {
                    	channelList.add(clientChannelInfo);
                        log.info("new producer connected, group: {} group hashcode: {} channel: {}", group,
                        		group.hashCode(),    clientChannelInfo.toString());
                    }
                }
                finally {
                    this.hashcodeChannelLock.unlock();
                }

                if (bClientChannelInfoFound) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            }
            else {
                log.warn("ProducerManager registerProducer hashcodeChannelLock timeout");
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }
    }


    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            if (this.groupChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null != channelTable && !channelTable.isEmpty()) {
                        ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                                clientChannelInfo.toString());
                        }

                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                }
                finally {
                    this.groupChannelLock.unlock();
                }
            }
            else {
                log.warn("ProducerManager unregisterProducer groupChannelLock timeout");
            }
            
            
            if (this.hashcodeChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                	List<ClientChannelInfo> channelList = this.hashcodeChannelTable.get(group);
                    if (null != channelList && !channelList.isEmpty()) {
                        boolean bRemove = channelList.remove(clientChannelInfo.getChannel());
                        if (bRemove) {
                            log.info("unregister a producer[{}] from hashcodeChannelTable {}", group,
                                clientChannelInfo.toString());
                        }

                        if (channelList.isEmpty()) {
                            this.hashcodeChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from hashcodeChannelTable", group);
                        }
                    }
                }
                finally {
                    this.hashcodeChannelLock.unlock();
                }
            }
            else {
                log.warn("ProducerManager unregisterProducer hashcodeChannelLock timeout");
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }
    }


	public ClientChannelInfo pickProducerChannelRandomly(final int producerGroupHashCode) {
        try {
            if (this.hashcodeChannelLock.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    List<ClientChannelInfo> channelInfoList =
                            this.hashcodeChannelTable.get(producerGroupHashCode);
                    if (channelInfoList != null && !channelInfoList.isEmpty()) {
                        int index = this.generateRandmonNum() % channelInfoList.size();
                        ClientChannelInfo info = channelInfoList.get(index);
                        return info;
                    }
                }
                finally {
                    this.hashcodeChannelLock.unlock();
                }
            }
            else {
                log.warn("ProducerManager pickProducerChannelRandomly lock timeout");
            }
        }
        catch (InterruptedException e) {
            log.error("", e);
        }

        return null;
    }
	
	private int generateRandmonNum() {
        int value = this.random.nextInt();

        if (value < 0) {
            value = Math.abs(value);
        }

        return value;
    }
}
