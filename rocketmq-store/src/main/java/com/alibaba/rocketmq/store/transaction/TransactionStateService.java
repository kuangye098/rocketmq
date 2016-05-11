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
package com.alibaba.rocketmq.store.transaction;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.ConsumeQueue;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MapedFile;
import com.alibaba.rocketmq.store.MapedFileQueue;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import com.alibaba.rocketmq.store.config.BrokerRole;


/**
 * 事务服务，存储每条事务的状态（Prepared，Commited，Rollbacked）<br>
 * 名词解释：<br>
 * clOffset - Commit Log Offset<br>
 * tsOffset - Transaction State Table Offset
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class TransactionStateService {
    // 存储单元大小
    public static final int TSStoreUnitSize = 24;
    // 用来恢复事务状态表的redolog
    public static final String TRANSACTION_REDOLOG_TOPIC = "TRANSACTION_REDOLOG_TOPIC_XXXX";
    public static final int TRANSACTION_REDOLOG_TOPIC_QUEUEID = 0;
    public final static long PreparedMessageTagsCode = -1;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    // 更改事务状态，具体更改位置
    private final static int TS_STATE_POS = 20;
    private static final Logger tranlog = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);
    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;
    // 重复利用内存Buffer
    private final ByteBuffer byteBufferAppend = ByteBuffer.allocate(TSStoreUnitSize);
    // 事务状态的Redolog，当进程意外宕掉，可通过redolog恢复所有事务的状态
    // Redolog的实现利用了消费队列，主要为了恢复方便
    private final ConsumeQueue tranRedoLog;
    // State Table Offset，重启时，必须纠正
    private final AtomicLong tranStateTableOffset = new AtomicLong(0);
    // 定时回查线程
    private final Timer timer = new Timer("CheckTransactionMessageTimer", true);
    // 存储事务状态的表格
    private MapedFileQueue tranStateTable;


    public TransactionStateService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.tranStateTable =
                new MapedFileQueue(defaultMessageStore.getMessageStoreConfig().getTranStateTableStorePath(),
                    defaultMessageStore.getMessageStoreConfig().getTranStateTableMapedFileSize(), null);

        this.tranRedoLog = new ConsumeQueue(//
            TRANSACTION_REDOLOG_TOPIC,//
            TRANSACTION_REDOLOG_TOPIC_QUEUEID,//
            defaultMessageStore.getMessageStoreConfig().getTranRedoLogStorePath(),//
            defaultMessageStore.getMessageStoreConfig().getTranRedoLogMapedFileSize(),//
            defaultMessageStore);
    }


    public boolean load() {
        boolean result = this.tranRedoLog.load();
        result = result && this.tranStateTable.load();

        return result;
    }


    public void start() {
        this.initTimerTask();
    }


    private void initTimerTask() {
        final List<MapedFile> mapedFiles = this.tranStateTable.getMapedFiles();
        for (MapedFile mf : mapedFiles) {
            this.addTimerTask(mf);
        }
    }


    private void addTimerTask(final MapedFile mf) {
        this.timer.scheduleAtFixedRate(new TimerTask() {
            private final MapedFile mapedFile = mf;
            private final TransactionCheckExecuter transactionCheckExecuter =
                    TransactionStateService.this.defaultMessageStore.getTransactionCheckExecuter();
            private final long checkTransactionMessageAtleastInterval =
                    TransactionStateService.this.defaultMessageStore.getMessageStoreConfig()
                        .getCheckTransactionMessageAtleastInterval();
            private final boolean slave = TransactionStateService.this.defaultMessageStore
                .getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;


            @Override
            public void run() {
                // Slave不需要回查事务状态
                if (slave)
                    return;

                // Check功能是否开启
                if (!TransactionStateService.this.defaultMessageStore.getMessageStoreConfig()
                    .isCheckTransactionMessageEnable()) {
                    return;
                }

                try {
                	/**
                	 * chen.si:这里会延迟执行，消息会先写进去，导致file的write position
                	 * 
                	 * 这里做个同步是不是好点，加载完消息，再触发这里
                	 */
                    SelectMapedBufferResult selectMapedBufferResult = mapedFile.selectMapedBuffer(0);
                    if (selectMapedBufferResult != null) {
                        long preparedMessageCountInThisMapedFile = 0;
                        int i = 0;
                        try {

                            for (; i < selectMapedBufferResult.getSize(); i += TSStoreUnitSize) {
                                selectMapedBufferResult.getByteBuffer().position(i);

                                // Commit Log Offset
                                long clOffset = selectMapedBufferResult.getByteBuffer().getLong();
                                // Message Size
                                int msgSize = selectMapedBufferResult.getByteBuffer().getInt();
                                // Timestamp
                                int timestamp = selectMapedBufferResult.getByteBuffer().getInt();
                                // Producer Group Hashcode
                                int groupHashCode = selectMapedBufferResult.getByteBuffer().getInt();
                                // Transaction State
                                int tranType = selectMapedBufferResult.getByteBuffer().getInt();

                                /**
                                 * chen.si:这里只需要处理超时的prepared消息，用于回查事务状态
                                 */
                                // 已经提交或者回滚的消息跳过
                                if (tranType != MessageSysFlag.TransactionPreparedType) {
                                    continue;
                                }

                                // 遇到时间不符合，终止
                                long timestampLong = timestamp * 1000;
                                long diff = System.currentTimeMillis() - timestampLong;
                                if (diff < checkTransactionMessageAtleastInterval) {
                                    break;
                                }

                                preparedMessageCountInThisMapedFile++;

                                try {
                                    this.transactionCheckExecuter.gotoCheck(//
                                        groupHashCode,//
                                        getTranStateOffset(i),//
                                        clOffset,//
                                        msgSize);
                                }
                                catch (Exception e) {
                                    tranlog.warn("gotoCheck Exception", e);
                                }
                            }

                            // 无Prepared消息，且遍历完，则终止定时任务
                            if (0 == preparedMessageCountInThisMapedFile //
                                    && i == mapedFile.getFileSize()) {
                                tranlog
                                    .info(
                                        "remove the transaction timer task, because no prepared message in this mapedfile[{}]",
                                        mapedFile.getFileName());
                                this.cancel();
                            }
                        }
                        finally {
                            selectMapedBufferResult.release();
                        }

                        tranlog
                            .info(
                                "the transaction timer task execute over in this period, {} Prepared Message: {} Check Progress: {}/{}",
                                mapedFile.getFileName(),//
                                preparedMessageCountInThisMapedFile,//
                                i / TSStoreUnitSize,//
                                mapedFile.getFileSize() / TSStoreUnitSize//
                            );
                    }
                    else if (mapedFile.isFull()) {
                        tranlog.info("the mapedfile[{}] maybe deleted, cancel check transaction timer task",
                            mapedFile.getFileName());
                        this.cancel();
                        return;
                    }
                }
                catch (Exception e) {
                    log.error("check transaction timer task Exception", e);
                }
            }


            private long getTranStateOffset(final long currentIndex) {
                long offset =
                        (this.mapedFile.getFileFromOffset() + currentIndex)
                                / TransactionStateService.TSStoreUnitSize;
                return offset;
            }
        }, 1000 * 60, this.defaultMessageStore.getMessageStoreConfig()
            .getCheckTransactionMessageTimerInterval());
    }


    public void shutdown() {
        this.timer.cancel();
    }


    public int deleteExpiredStateFile(long offset) {
        int cnt = this.tranStateTable.deleteExpiredFileByOffset(offset, TSStoreUnitSize);
        return cnt;
    }


    public void recoverStateTable(final boolean lastExitOK) {
        if (lastExitOK) {
            this.recoverStateTableNormal();
        }
        else {
            // 第一步，删除State Table
        	/**
        	 * chen.si：这里删除了所有的tran table
        	 */
            this.tranStateTable.destroy();
            // 第二步，通过RedoLog全量恢复StateTable
            /**
             * chen.si：重新生成tran table
             */
            this.recreateStateTable();
        }
    }


    private void recreateStateTable() {
        this.tranStateTable =
                new MapedFileQueue(defaultMessageStore.getMessageStoreConfig().getTranStateTableStorePath(),
                    defaultMessageStore.getMessageStoreConfig().getTranStateTableMapedFileSize(), null);

        final TreeSet<Long> preparedItemSet = new TreeSet<Long>();

        // 第一步，重头扫描RedoLog
        final long minOffset = this.tranRedoLog.getMinOffsetInQuque();
        long processOffset = minOffset;
        while (true) {
            SelectMapedBufferResult bufferConsumeQueue = this.tranRedoLog.getIndexBuffer(processOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long i = 0;
                    for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQStoreUnitSize) {
                        long offsetMsg = bufferConsumeQueue.getByteBuffer().getLong();
                        int sizeMsg = bufferConsumeQueue.getByteBuffer().getInt();
                        long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();
                        /**
                         * chen.si:redo 消息示例：
                         *  
                            prepared:
                            1 commitLogOffset:3780
							2 messageSize:195
							3 tagsCode:-1
							
							prepared:
							1 commitLogOffset:3975
							2 messageSize:195
							3 tagsCode:-1
							
							commit/rollback:
							1 commitLogOffset:4170
							2 messageSize:195
							3 tagsCode:3975
                         */

                        // Prepared
                        /**
                         * chen.si：根据tag code来区分redo消息类型
                         */
                        if (TransactionStateService.PreparedMessageTagsCode == tagsCode) {
                            preparedItemSet.add(offsetMsg);
                        }
                        // Commit/Rollback
                        else {
                        	/**
                        	 * chen.si: commit和rollback的消息，不需要继续处理，同时 要将对应的prepared消息移除掉
                        	 */
                            preparedItemSet.remove(tagsCode);
                        }
                    }

                    processOffset += i;
                }
                finally {
                    // 必须释放资源
                    bufferConsumeQueue.release();
                }
            }
            else {
                break;
            }
        }

        log.info("scan transaction redolog over, End offset: {},  Prepared Transaction Count: {}",
            processOffset, preparedItemSet.size());
        // 第二步，重建StateTable
        Iterator<Long> it = preparedItemSet.iterator();
        while (it.hasNext()) {
            Long offset = it.next();
            /**
             * chen.si：根据redo消息，在commit log中找到对应的prepared消息
             */
            MessageExt msgExt = this.defaultMessageStore.lookMessageByOffset(offset);
            if (msgExt != null) {
            	/**
            	 * chen.si:重建tran stat消息
            	 */
                this.appendPreparedTransaction(msgExt.getCommitLogOffset(), msgExt.getStoreSize(),
                    (int) (msgExt.getStoreTimestamp() / 1000),
                    msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP).hashCode());
                this.tranStateTableOffset.incrementAndGet();
            }
        }
    }


    /**
     * 单线程调用
     */
    public boolean appendPreparedTransaction(//
            final long clOffset,//
            final int size,//
            final int timestamp,//
            final int groupHashCode//
    ) {
        MapedFile mapedFile = this.tranStateTable.getLastMapedFile();
        if (null == mapedFile) {
            log.error("appendPreparedTransaction: create mapedfile error.");
            return false;
        }

        /**
         * chen.si:用来处理prepared tran消息，主要进行回查事务状态
         */
        // 首次创建，加入定时任务中
        if (0 == mapedFile.getWrotePostion()) {
            this.addTimerTask(mapedFile);
        }

        /**
         * chen.si：将redo中的prepared消息，重新写入tran stat文件
         */
        this.byteBufferAppend.position(0);
        this.byteBufferAppend.limit(TSStoreUnitSize);

        // Commit Log Offset
        this.byteBufferAppend.putLong(clOffset);
        // Message Size
        this.byteBufferAppend.putInt(size);
        // Timestamp
        this.byteBufferAppend.putInt(timestamp);
        // Producer Group Hashcode
        this.byteBufferAppend.putInt(groupHashCode);
        // Transaction State
        this.byteBufferAppend.putInt(MessageSysFlag.TransactionPreparedType);

        return mapedFile.appendMessage(this.byteBufferAppend.array());
    }


    private void recoverStateTableNormal() {
    	/**
    	 * chen.si：这个方法的主要任务如下：
    	 * 
    	 * 1. 设置 tran log queue中的最后一个消息位置 ？ 这个版本为什么不设置？
    	 * 
    	 * 2. 设置最后一个消息所在 log文件的commit 和 write position
    	 * 
    	 * 3. 设置tran stat的事务消息的下一个序号
    	 * 
    	 * 3. 删除多余的文件
    	 */
        final List<MapedFile> mapedFiles = this.tranStateTable.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
        	/**
        	 * chen.si:老路子了，依旧从最后的3个文件中找出 最后一个在用文件
        	 */
            // 从倒数第三个文件开始恢复
            int index = mapedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mapedFileSizeLogics = this.tranStateTable.getMapedFileSize();
            MapedFile mapedFile = mapedFiles.get(index);
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            /*
             * chen.si：queue offset(global offset)
             */
            long processOffset = mapedFile.getFileFromOffset();
            /*
             * chen.si：file offset(local offset)
             */
            long mapedFileOffset = 0;
            while (true) {
            	/**
            	 * chen.si：每个事务消息24个字节
            	 */
                for (int i = 0; i < mapedFileSizeLogics; i += TSStoreUnitSize) {

                    final long clOffset_read = byteBuffer.getLong();
                    final int size_read = byteBuffer.getInt();
                    final int timestamp_read = byteBuffer.getInt();
                    final int groupHashCode_read = byteBuffer.getInt();
                    /**
                     * chen.si：事务状态：prepared/commit/rollback
                     */
                    final int state_read = byteBuffer.getInt();
                    
                    /**
                     * chen.si: 事务消息的示例
                     * 
                     *  其中<>内的内容为时间转换后的
                     * 
                     * prepared：
                        1 commitLogOffset:3780
						2 messageSize:195
						3 timestamp:1404354477 <1970-01-17 14:05:54>
						4 groupHCode:1119945399
						5 tranType:4
					   
					   commit：
						1 commitLogOffset:3975
						2 messageSize:195
						3 timestamp:1404355385 <1970-01-17 14:05:55>
						4 groupHCode:1119945399
						5 tranType:8
                     */

                    boolean stateOK = false;
                    switch (state_read) {
                    case MessageSysFlag.TransactionPreparedType:
                    case MessageSysFlag.TransactionCommitType:
                    case MessageSysFlag.TransactionRollbackType:
                        stateOK = true;
                        break;
                    default:
                        break;
                    }

                    // 说明当前存储单元有效
                    // TODO 这样判断有效是否合理？
                    if (clOffset_read >= 0 && size_read > 0 && stateOK) {
                    	/**
                    	 * chen.si：消息合法，增加 file offset
                    	 */
                        mapedFileOffset = i + TSStoreUnitSize;
                    }
                    else {
                        log.info("recover current transaction state table file over,  "
                                + mapedFile.getFileName() + " " + clOffset_read + " " + size_read + " "
                                + timestamp_read);
                        break;
                    }
                }

                // 走到文件末尾，切换至下一个文件
                if (mapedFileOffset == mapedFileSizeLogics) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        // 当前条件分支不可能发生
                        log.info("recover last transaction state table file over, last maped file "
                                + mapedFile.getFileName());
                        break;
                    }
                    else {
                    	/**
                    	 * chen.si：继续解析下一个文件
                    	 */
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        /**
                         * chen.si：queue offset直接更新为 file的名字对应的offset
                         */
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next transaction state table file, " + mapedFile.getFileName());
                    }
                }
                else {
                    log.info("recover current transaction state table queue over " + mapedFile.getFileName()
                            + " " + (processOffset + mapedFileOffset));
                    break;
                }
            }

            processOffset += mapedFileOffset;
            /**
             * chen.si：
             * 1. 设置 代写文件 的commit 和 write position
             * 2. 删除多余的文件
             */
            this.tranStateTable.truncateDirtyFiles(processOffset);
            /**
             * chen.si:这个很重要，基于事务消息个数，来设置 事务的ID
             */
            this.tranStateTableOffset.set(this.tranStateTable.getMaxOffset() / TSStoreUnitSize);
            log.info("recover normal over, transaction state table max offset: {}",
                this.tranStateTableOffset.get());
        }
    }


    /**
     * 单线程调用
     */
    public boolean updateTransactionState(//
            final long tsOffset,//
            final long clOffset,//
            final int groupHashCode,//
            final int state//
    ) {
        SelectMapedBufferResult selectMapedBufferResult = this.findTransactionBuffer(tsOffset);
        if (selectMapedBufferResult != null) {
            try {
                final long clOffset_read = selectMapedBufferResult.getByteBuffer().getLong();
                final int size_read = selectMapedBufferResult.getByteBuffer().getInt();
                final int timestamp_read = selectMapedBufferResult.getByteBuffer().getInt();
                final int groupHashCode_read = selectMapedBufferResult.getByteBuffer().getInt();
                final int state_read = selectMapedBufferResult.getByteBuffer().getInt();

                // 校验数据正确性
                if (clOffset != clOffset_read) {
                    log.error("updateTransactionState error clOffset: {} clOffset_read: {}", clOffset,
                        clOffset_read);
                    return false;
                }

                // 校验数据正确性
                if (groupHashCode != groupHashCode_read) {
                    log.error("updateTransactionState error groupHashCode: {} groupHashCode_read: {}",
                        groupHashCode, groupHashCode_read);
                    return false;
                }

                // 判断是否已经更新过
                if (MessageSysFlag.TransactionPreparedType != state_read) {
                    log.warn("updateTransactionState error, the transaction is updated before.");
                    return true;
                }

                // 更新事务状态
                selectMapedBufferResult.getByteBuffer().putInt(TS_STATE_POS, state);
            }
            catch (Exception e) {
                log.error("updateTransactionState exception", e);
            }
            finally {
                selectMapedBufferResult.release();
            }
        }

        return false;
    }


    private SelectMapedBufferResult findTransactionBuffer(final long tsOffset) {
        final int mapedFileSize =
                this.defaultMessageStore.getMessageStoreConfig().getTranStateTableMapedFileSize();
        final long offset = tsOffset * TSStoreUnitSize;
        MapedFile mapedFile = this.tranStateTable.findMapedFileByOffset(offset);
        if (mapedFile != null) {
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer((int) (offset % mapedFileSize));
            return result;
        }

        return null;
    }


    public AtomicLong getTranStateTableOffset() {
        return tranStateTableOffset;
    }


    public ConsumeQueue getTranRedoLog() {
        return tranRedoLog;
    }
}
