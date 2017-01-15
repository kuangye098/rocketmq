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
package com.alibaba.rocketmq.store.config;

import com.alibaba.rocketmq.common.annotation.ImportantField;
import com.alibaba.rocketmq.store.ConsumeQueue;

import java.io.File;


/**
 * @author shijia.wxr
 */
public class MessageStoreConfig {
    //The root directory in which the log data is kept
    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

    //The directory in which the commitlog is kept
    @ImportantField
    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "commitlog";

    // CommitLog file size,default is 1G
    private int mapedFileSizeCommitLog = 1024 * 1024 * 1024;
    // ConsumeQueue file size, default is 30W
    private int mapedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQStoreUnitSize;
    // CommitLog flush interval
    @ImportantField
    private int flushIntervalCommitLog = 1000;
    // Whether schedule flush,default is real-time
    @ImportantField
    private boolean flushCommitLogTimed = false;
    // ConsumeQueue flush interval
    private int flushIntervalConsumeQueue = 1000;
    // Resource reclaim interval
    private int cleanResourceInterval = 10000;
    // CommitLog removal interval
    private int deleteCommitLogFilesInterval = 100;
    // ConsumeQueue removal interval
    private int deleteConsumeQueueFilesInterval = 100;
    private int destroyMapedFileIntervalForcibly = 1000 * 120;
    private int redeleteHangedFileInterval = 1000 * 120;
    // When to delete,default is at 4 am
    @ImportantField
    private String deleteWhen = "04";
    private int diskMaxUsedSpaceRatio = 75;
    // The number of hours to keep a log file before deleting it (in hours)
    @ImportantField
    private int fileReservedTime = 72;
    // Flow control for ConsumeQueue
    private int putMsgIndexHightWater = 600000;
    // The maximum size of a single log file,default is 512K
    private int maxMessageSize = 1024 * 1024 * 4;
    // Whether check the CRC32 of the records consumed.
    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
    // This check adds some overhead, so it may be disabled in cases seeking extreme performance.
    private boolean checkCRCOnRecover = true;
    // How many pages are to be flushed when flush CommitLog
    private int flushCommitLogLeastPages = 4;
    // Flush page size when the disk in warming state
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;
    // How many pages are to be flushed when flush ConsumeQueue
    private int flushConsumeQueueLeastPages = 2;
    private int flushCommitLogThoroughInterval = 1000 * 10;
    private int flushConsumeQueueThoroughInterval = 1000 * 60;
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;
    @ImportantField
    private boolean messageIndexEnable = true;
    private int maxHashSlotNum = 5000000;
    private int maxIndexNum = 5000000 * 4;
    private int maxMsgsNumBatch = 64;
    @ImportantField
    private boolean messageIndexSafe = false;
    private int haListenPort = 10912;
    private int haSendHeartbeatInterval = 1000 * 5;
    private int haHousekeepingInterval = 1000 * 20;
    private int haTransferBatchSize = 1024 * 32;
    @ImportantField
    private String haMasterAddress = null;
    private int haSlaveFallbehindMax = 1024 * 1024 * 256;
    @ImportantField
    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
    private int syncFlushTimeout = 1000 * 5;
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    private long flushDelayOffsetInterval = 1000 * 10;
    private String tranStateTableStorePath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "transaction" + File.separator + "statetable";
    private int tranStateTableMapedFileSize = 2000000 * 1;// TransactionStateService.TSStoreUnitSize;
    private String tranRedoLogStorePath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "transaction" + File.separator + "redolog";
    private int tranRedoLogMapedFileSize = 2000000 * ConsumeQueue.CQStoreUnitSize;
    private long checkTransactionMessageAtleastInterval = 1000 * 60;
    private long checkTransactionMessageTimerInterval = 1000 * 60;
    private boolean checkTransactionMessageEnable = true;
    @ImportantField
    private boolean cleanFileForciblyEnable = true;
    private boolean warmMapedFileEnable = false;
    private boolean offsetCheckInSlave = false;
    private boolean debugLockEnable = false;
    private boolean duplicationEnable = false;
    private boolean diskFallRecorded = true;
    private long osPageCacheBusyTimeOutMills = 1000;
    private int defaultQueryMaxNum = 32;
    
    public boolean isDebugLockEnable() {
        return debugLockEnable;
    }

    public void setDebugLockEnable(final boolean debugLockEnable) {
        this.debugLockEnable = debugLockEnable;
    }

    public boolean isDuplicationEnable() {
        return duplicationEnable;
    }

    public void setDuplicationEnable(final boolean duplicationEnable) {
        this.duplicationEnable = duplicationEnable;
    }

    public long getOsPageCacheBusyTimeOutMills() {
        return osPageCacheBusyTimeOutMills;
    }

    public void setOsPageCacheBusyTimeOutMills(final long osPageCacheBusyTimeOutMills) {
        this.osPageCacheBusyTimeOutMills = osPageCacheBusyTimeOutMills;
    }

    public boolean isDiskFallRecorded() {
        return diskFallRecorded;
    }

    public void setDiskFallRecorded(final boolean diskFallRecorded) {
        this.diskFallRecorded = diskFallRecorded;
    }

    public boolean isWarmMapedFileEnable() {
        return warmMapedFileEnable;
    }


    public void setWarmMapedFileEnable(boolean warmMapedFileEnable) {
        this.warmMapedFileEnable = warmMapedFileEnable;
    }


    public int getMapedFileSizeCommitLog() {
        return mapedFileSizeCommitLog;
    }


    public void setMapedFileSizeCommitLog(int mapedFileSizeCommitLog) {
        this.mapedFileSizeCommitLog = mapedFileSizeCommitLog;
    }


    public int getMapedFileSizeConsumeQueue() {

        int factor = (int) Math.ceil(this.mapedFileSizeConsumeQueue / (ConsumeQueue.CQStoreUnitSize * 1.0));
        return (int) (factor * ConsumeQueue.CQStoreUnitSize);
    }


    public void setMapedFileSizeConsumeQueue(int mapedFileSizeConsumeQueue) {
        this.mapedFileSizeConsumeQueue = mapedFileSizeConsumeQueue;
    }


    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }


    public void setFlushIntervalCommitLog(int flushIntervalCommitLog) {
        this.flushIntervalCommitLog = flushIntervalCommitLog;
    }


    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }


    public void setFlushIntervalConsumeQueue(int flushIntervalConsumeQueue) {
        this.flushIntervalConsumeQueue = flushIntervalConsumeQueue;
    }


    public int getPutMsgIndexHightWater() {
        return putMsgIndexHightWater;
    }


    public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
        this.putMsgIndexHightWater = putMsgIndexHightWater;
    }


    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }


    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }


    public int getMaxMessageSize() {
        return maxMessageSize;
    }


    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }


    public boolean isCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }


    public boolean getCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }


    public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
        this.checkCRCOnRecover = checkCRCOnRecover;
    }


    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }


    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }


    public String getDeleteWhen() {
        return deleteWhen;
    }


    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }


    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10)
            return 10;

        if (this.diskMaxUsedSpaceRatio > 95)
            return 95;

        return diskMaxUsedSpaceRatio;
    }


    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }


    public int getDeleteCommitLogFilesInterval() {
        return deleteCommitLogFilesInterval;
    }


    public void setDeleteCommitLogFilesInterval(int deleteCommitLogFilesInterval) {
        this.deleteCommitLogFilesInterval = deleteCommitLogFilesInterval;
    }


    public int getDeleteConsumeQueueFilesInterval() {
        return deleteConsumeQueueFilesInterval;
    }


    public void setDeleteConsumeQueueFilesInterval(int deleteConsumeQueueFilesInterval) {
        this.deleteConsumeQueueFilesInterval = deleteConsumeQueueFilesInterval;
    }


    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }


    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }


    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }


    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }


    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }


    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }


    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }


    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }


    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }


    public void setFlushCommitLogLeastPages(int flushCommitLogLeastPages) {
        this.flushCommitLogLeastPages = flushCommitLogLeastPages;
    }


    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }


    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }


    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }


    public void setFlushCommitLogThoroughInterval(int flushCommitLogThoroughInterval) {
        this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
    }


    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }


    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }


    public int getDestroyMapedFileIntervalForcibly() {
        return destroyMapedFileIntervalForcibly;
    }


    public void setDestroyMapedFileIntervalForcibly(int destroyMapedFileIntervalForcibly) {
        this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
    }


    public int getFileReservedTime() {
        return fileReservedTime;
    }


    public void setFileReservedTime(int fileReservedTime) {
        this.fileReservedTime = fileReservedTime;
    }


    public int getRedeleteHangedFileInterval() {
        return redeleteHangedFileInterval;
    }


    public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
        this.redeleteHangedFileInterval = redeleteHangedFileInterval;
    }


    public int getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }


    public void setAccessMessageInMemoryMaxRatio(int accessMessageInMemoryMaxRatio) {
        this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
    }


    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }


    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }


    public int getMaxHashSlotNum() {
        return maxHashSlotNum;
    }


    public void setMaxHashSlotNum(int maxHashSlotNum) {
        this.maxHashSlotNum = maxHashSlotNum;
    }


    public int getMaxIndexNum() {
        return maxIndexNum;
    }


    public void setMaxIndexNum(int maxIndexNum) {
        this.maxIndexNum = maxIndexNum;
    }


    public int getMaxMsgsNumBatch() {
        return maxMsgsNumBatch;
    }


    public void setMaxMsgsNumBatch(int maxMsgsNumBatch) {
        this.maxMsgsNumBatch = maxMsgsNumBatch;
    }


    public int getHaListenPort() {
        return haListenPort;
    }


    public void setHaListenPort(int haListenPort) {
        this.haListenPort = haListenPort;
    }


    public int getHaSendHeartbeatInterval() {
        return haSendHeartbeatInterval;
    }


    public void setHaSendHeartbeatInterval(int haSendHeartbeatInterval) {
        this.haSendHeartbeatInterval = haSendHeartbeatInterval;
    }


    public int getHaHousekeepingInterval() {
        return haHousekeepingInterval;
    }


    public void setHaHousekeepingInterval(int haHousekeepingInterval) {
        this.haHousekeepingInterval = haHousekeepingInterval;
    }


    public BrokerRole getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = BrokerRole.valueOf(brokerRole);
    }

    public int getHaTransferBatchSize() {
        return haTransferBatchSize;
    }


    public void setHaTransferBatchSize(int haTransferBatchSize) {
        this.haTransferBatchSize = haTransferBatchSize;
    }


    public int getHaSlaveFallbehindMax() {
        return haSlaveFallbehindMax;
    }


    public void setHaSlaveFallbehindMax(int haSlaveFallbehindMax) {
        this.haSlaveFallbehindMax = haSlaveFallbehindMax;
    }


    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    public void setFlushDiskType(String type) {
        this.flushDiskType = FlushDiskType.valueOf(type);
    }

    public int getSyncFlushTimeout() {
        return syncFlushTimeout;
    }


    public void setSyncFlushTimeout(int syncFlushTimeout) {
        this.syncFlushTimeout = syncFlushTimeout;
    }


    public String getHaMasterAddress() {
        return haMasterAddress;
    }


    public void setHaMasterAddress(String haMasterAddress) {
        this.haMasterAddress = haMasterAddress;
    }


    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }


    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }


    public long getFlushDelayOffsetInterval() {
        return flushDelayOffsetInterval;
    }


    public void setFlushDelayOffsetInterval(long flushDelayOffsetInterval) {
        this.flushDelayOffsetInterval = flushDelayOffsetInterval;
    }


    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }


    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }


    public boolean isMessageIndexSafe() {
        return messageIndexSafe;
    }


    public void setMessageIndexSafe(boolean messageIndexSafe) {
        this.messageIndexSafe = messageIndexSafe;
    }


    public boolean isFlushCommitLogTimed() {
        return flushCommitLogTimed;
    }


    public void setFlushCommitLogTimed(boolean flushCommitLogTimed) {
        this.flushCommitLogTimed = flushCommitLogTimed;
    }


    public String getStorePathRootDir() {
        return storePathRootDir;
    }


    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }


    public int getFlushLeastPagesWhenWarmMapedFile() {
        return flushLeastPagesWhenWarmMapedFile;
    }


    public void setFlushLeastPagesWhenWarmMapedFile(int flushLeastPagesWhenWarmMapedFile) {
        this.flushLeastPagesWhenWarmMapedFile = flushLeastPagesWhenWarmMapedFile;
    }


    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }


    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }

    public int getDefaultQueryMaxNum() {
        return defaultQueryMaxNum;
    }

    public void setDefaultQueryMaxNum(int defaultQueryMaxNum) {
        this.defaultQueryMaxNum = defaultQueryMaxNum;
    }

    public String getTranStateTableStorePath() {
        return tranStateTableStorePath;
    }

    public void setTranStateTableStorePath(String tranStateTableStorePath) {
        this.tranStateTableStorePath = tranStateTableStorePath;
    }

    public String getTranRedoLogStorePath() {
        return tranRedoLogStorePath;
    }

    public void setTranRedoLogStorePath(String tranRedoLogStorePath) {
        this.tranRedoLogStorePath = tranRedoLogStorePath;
    }

    public int getTranStateTableMapedFileSize() {
        return tranStateTableMapedFileSize;
    }

    public void setTranStateTableMapedFileSize(int tranStateTableMapedFileSize) {
        this.tranStateTableMapedFileSize = tranStateTableMapedFileSize;
    }

    public int getTranRedoLogMapedFileSize() {
        return tranRedoLogMapedFileSize;
    }

    public void setTranRedoLogMapedFileSize(int tranRedoLogMapedFileSize) {
        this.tranRedoLogMapedFileSize = tranRedoLogMapedFileSize;
    }

    public long getCheckTransactionMessageAtleastInterval() {
        return checkTransactionMessageAtleastInterval;
    }

    public void setCheckTransactionMessageAtleastInterval(
            long checkTransactionMessageAtleastInterval) {
        this.checkTransactionMessageAtleastInterval = checkTransactionMessageAtleastInterval;
    }

    public long getCheckTransactionMessageTimerInterval() {
        return checkTransactionMessageTimerInterval;
    }

    public void setCheckTransactionMessageTimerInterval(
            long checkTransactionMessageTimerInterval) {
        this.checkTransactionMessageTimerInterval = checkTransactionMessageTimerInterval;
    }

    public boolean isCheckTransactionMessageEnable() {
        return checkTransactionMessageEnable;
    }

    public void setCheckTransactionMessageEnable(
            boolean checkTransactionMessageEnable) {
        this.checkTransactionMessageEnable = checkTransactionMessageEnable;
    }
}
