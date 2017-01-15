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
 * clOffset - Commit Log Offset<br>
 * tsOffset - Transaction State Table Offset
 */
public class TransactionStateService {
    public static final int TSStoreUnitSize = 24;

    public static final String TRANSACTION_REDOLOG_TOPIC = "TRANSACTION_REDOLOG_TOPIC_XXXX";
    public static final int TRANSACTION_REDOLOG_TOPIC_QUEUEID = 0;
    public final static long PreparedMessageTagsCode = -1;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private final static int TS_STATE_POS = 20;
    private static final Logger tranlog = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);

    private final DefaultMessageStore defaultMessageStore;

    private final ByteBuffer byteBufferAppend = ByteBuffer.allocate(TSStoreUnitSize);

    private final ConsumeQueue tranRedoLog;

    private final AtomicLong tranStateTableOffset = new AtomicLong(0);

    private final Timer timer = new Timer("CheckTransactionMessageTimer", true);

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
                if (slave)
                    return;

                if (!TransactionStateService.this.defaultMessageStore.getMessageStoreConfig()
                    .isCheckTransactionMessageEnable()) {
                    return;
                }

                try {

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

                                if (tranType != MessageSysFlag.TransactionPreparedType) {
                                    continue;
                                }

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
            this.tranStateTable.destroy();

            this.recreateStateTable();
        }
    }


    private void recreateStateTable() {
        this.tranStateTable =
                new MapedFileQueue(defaultMessageStore.getMessageStoreConfig().getTranStateTableStorePath(),
                    defaultMessageStore.getMessageStoreConfig().getTranStateTableMapedFileSize(), null);

        final TreeSet<Long> preparedItemSet = new TreeSet<Long>();

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

                        // Prepared
                        if (TransactionStateService.PreparedMessageTagsCode == tagsCode) {
                            preparedItemSet.add(offsetMsg);
                        }
                        // Commit/Rollback
                        else {
                            preparedItemSet.remove(tagsCode);
                        }
                    }

                    processOffset += i;
                }
                finally {
                    bufferConsumeQueue.release();
                }
            }
            else {
                break;
            }
        }

        log.info("scan transaction redolog over, End offset: {},  Prepared Transaction Count: {}",
            processOffset, preparedItemSet.size());

        Iterator<Long> it = preparedItemSet.iterator();
        while (it.hasNext()) {
            Long offset = it.next();

            MessageExt msgExt = this.defaultMessageStore.lookMessageByOffset(offset);
            if (msgExt != null) {

                this.appendPreparedTransaction(msgExt.getCommitLogOffset(), msgExt.getStoreSize(),
                    (int) (msgExt.getStoreTimestamp() / 1000),
                    msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP).hashCode());
                this.tranStateTableOffset.incrementAndGet();
            }
        }
    }

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

        if (0 == mapedFile.getWrotePostion()) {
            this.addTimerTask(mapedFile);
        }

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

        final List<MapedFile> mapedFiles = this.tranStateTable.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
            int index = mapedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mapedFileSizeLogics = this.tranStateTable.getMapedFileSize();
            MapedFile mapedFile = mapedFiles.get(index);
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            /*
             * queue offset(global offset)
             */
            long processOffset = mapedFile.getFileFromOffset();
            /*
             * file offset(local offset)
             */
            long mapedFileOffset = 0;
            while (true) {
                for (int i = 0; i < mapedFileSizeLogics; i += TSStoreUnitSize) {

                    final long clOffset_read = byteBuffer.getLong();
                    final int size_read = byteBuffer.getInt();
                    final int timestamp_read = byteBuffer.getInt();
                    final int groupHashCode_read = byteBuffer.getInt();
                    /**
                     * prepared/commit/rollback
                     */
                    final int state_read = byteBuffer.getInt();

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

                    if (clOffset_read >= 0 && size_read > 0 && stateOK) {
                        mapedFileOffset = i + TSStoreUnitSize;
                    }
                    else {
                        log.info("recover current transaction state table file over,  "
                                + mapedFile.getFileName() + " " + clOffset_read + " " + size_read + " "
                                + timestamp_read);
                        break;
                    }
                }

                if (mapedFileOffset == mapedFileSizeLogics) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        log.info("recover last transaction state table file over, last maped file "
                                + mapedFile.getFileName());
                        break;
                    }
                    else {
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();

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

            this.tranStateTable.truncateDirtyFiles(processOffset);

            this.tranStateTableOffset.set(this.tranStateTable.getMaxOffset() / TSStoreUnitSize);
            log.info("recover normal over, transaction state table max offset: {}",
                this.tranStateTableOffset.get());
        }
    }

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

                if (clOffset != clOffset_read) {
                    log.error("updateTransactionState error clOffset: {} clOffset_read: {}", clOffset,
                        clOffset_read);
                    return false;
                }

                if (groupHashCode != groupHashCode_read) {
                    log.error("updateTransactionState error groupHashCode: {} groupHashCode_read: {}",
                        groupHashCode, groupHashCode_read);
                    return false;
                }

                if (MessageSysFlag.TransactionPreparedType != state_read) {
                    log.warn("updateTransactionState error, the transaction is updated before.");
                    return true;
                }

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
