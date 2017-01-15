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
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


/**
 * @author shijia.wxr
 */
public class StoreStatsService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private static final int FrequencyOfSampling = 1000;

    private static final int MaxRecordsOfSampling = 60 * 10;
    private static final String[] PutMessageEntireTimeMaxDesc = new String[]{
            "[<=0ms]", // 0
            "[0~10ms]", // 1
            "[10~100ms]", // 2
            "[100~500ms]", // 3
            "[500ms~1s]", // 4
            "[1~2s]", // 5
            "[2~3s]", // 6
            "[3~4s]", // 7
            "[4~5s]", // 8
            "[5~10s]", // 9
            "[10s~]", // 10
    };

    private static int PrintTPSInterval = 60 * 1;

    private final AtomicLong putMessageFailedTimes = new AtomicLong(0);

    private final Map<String, AtomicLong> putMessageTopicTimesTotal =
            new ConcurrentHashMap<String, AtomicLong>(128);
    private final Map<String, AtomicLong> putMessageTopicSizeTotal =
            new ConcurrentHashMap<String, AtomicLong>(128);

    private final AtomicLong getMessageTimesTotalFound = new AtomicLong(0);
    private final AtomicLong getMessageTransferedMsgCount = new AtomicLong(0);
    private final AtomicLong getMessageTimesTotalMiss = new AtomicLong(0);
    private final LinkedList<CallSnapshot> putTimesList = new LinkedList<CallSnapshot>();

    private final LinkedList<CallSnapshot> getTimesFoundList = new LinkedList<CallSnapshot>();
    private final LinkedList<CallSnapshot> getTimesMissList = new LinkedList<CallSnapshot>();
    private final LinkedList<CallSnapshot> transferedMsgCountList = new LinkedList<CallSnapshot>();
    private volatile AtomicLong[] putMessageDistributeTime;
    private long messageStoreBootTimestamp = System.currentTimeMillis();
    private volatile long putMessageEntireTimeMax = 0;
    private volatile long getMessageEntireTimeMax = 0;
    // for putMessageEntireTimeMax
    private ReentrantLock lockPut = new ReentrantLock();
    // for getMessageEntireTimeMax
    private ReentrantLock lockGet = new ReentrantLock();

    private volatile long dispatchMaxBuffer = 0;

    private ReentrantLock lockSampling = new ReentrantLock();
    private long lastPrintTimestamp = System.currentTimeMillis();


    public StoreStatsService() {
        this.initPutMessageDistributeTime();
    }

    private AtomicLong[] initPutMessageDistributeTime() {
        AtomicLong[] next = new AtomicLong[11];
        for (int i = 0; i < next.length; i++) {
            next[i] = new AtomicLong(0);
        }

        AtomicLong[] old = this.putMessageDistributeTime;

        this.putMessageDistributeTime = next;

        return old;
    }

    public long getPutMessageEntireTimeMax() {
        return putMessageEntireTimeMax;
    }

    public void setPutMessageEntireTimeMax(long value) {
        final AtomicLong[] times = this.putMessageDistributeTime;

        if (null == times) return;

        // us
        if (value <= 0) {
            times[0].incrementAndGet();
        }

        else if (value < 10) {
            times[1].incrementAndGet();
        }
        else if (value < 100) {
            times[2].incrementAndGet();
        }
        else if (value < 500) {
            times[3].incrementAndGet();
        }
        else if (value < 1000) {
            times[4].incrementAndGet();
        }
        // 2s
        else if (value < 2000) {
            times[5].incrementAndGet();
        }
        // 3s
        else if (value < 3000) {
            times[6].incrementAndGet();
        }
        // 4s
        else if (value < 4000) {
            times[7].incrementAndGet();
        }
        // 5s
        else if (value < 5000) {
            times[8].incrementAndGet();
        }
        // 10s
        else if (value < 10000) {
            times[9].incrementAndGet();
        }

        else {
            times[10].incrementAndGet();
        }

        if (value > this.putMessageEntireTimeMax) {
            this.lockPut.lock();
            this.putMessageEntireTimeMax =
                    value > this.putMessageEntireTimeMax ? value : this.putMessageEntireTimeMax;
            this.lockPut.unlock();
        }
    }


    public long getGetMessageEntireTimeMax() {
        return getMessageEntireTimeMax;
    }


    public void setGetMessageEntireTimeMax(long value) {
        if (value > this.getMessageEntireTimeMax) {
            this.lockGet.lock();
            this.getMessageEntireTimeMax =
                    value > this.getMessageEntireTimeMax ? value : this.getMessageEntireTimeMax;
            this.lockGet.unlock();
        }
    }


    public long getDispatchMaxBuffer() {
        return dispatchMaxBuffer;
    }


    public void setDispatchMaxBuffer(long value) {
        this.dispatchMaxBuffer = value > this.dispatchMaxBuffer ? value : this.dispatchMaxBuffer;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1024);
        Long totalTimes = getPutMessageTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        sb.append("\truntime: " + this.getFormatRuntime() + "\r\n");
        sb.append("\tputMessageEntireTimeMax: " + this.putMessageEntireTimeMax + "\r\n");
        sb.append("\tputMessageTimesTotal: " + totalTimes + "\r\n");
        sb.append("\tputMessageSizeTotal: " + this.getPutMessageSizeTotal() + "\r\n");
        sb.append("\tputMessageDistributeTime: " + this.getPutMessageDistributeTimeStringInfo(totalTimes)
                + "\r\n");
        sb.append("\tputMessageAverageSize: " + (this.getPutMessageSizeTotal() / totalTimes.doubleValue())
                + "\r\n");
        sb.append("\tdispatchMaxBuffer: " + this.dispatchMaxBuffer + "\r\n");
        sb.append("\tgetMessageEntireTimeMax: " + this.getMessageEntireTimeMax + "\r\n");
        sb.append("\tputTps: " + this.getPutTps() + "\r\n");
        sb.append("\tgetFoundTps: " + this.getGetFoundTps() + "\r\n");
        sb.append("\tgetMissTps: " + this.getGetMissTps() + "\r\n");
        sb.append("\tgetTotalTps: " + this.getGetTotalTps() + "\r\n");
        sb.append("\tgetTransferedTps: " + this.getGetTransferedTps() + "\r\n");
        return sb.toString();
    }

    public long getPutMessageTimesTotal() {
        long rs = 0;
        for (AtomicLong data : putMessageTopicTimesTotal.values()) {
            rs += data.get();
        }
        return rs;
    }

    private String getFormatRuntime() {
        final long MILLISECOND = 1;
        final long SECOND = 1000 * MILLISECOND;
        final long MINUTE = 60 * SECOND;
        final long HOUR = 60 * MINUTE;
        final long DAY = 24 * HOUR;
        final MessageFormat TIME = new MessageFormat("[ {0} days, {1} hours, {2} minutes, {3} seconds ]");

        long time = System.currentTimeMillis() - this.messageStoreBootTimestamp;
        long days = time / DAY;
        long hours = (time % DAY) / HOUR;
        long minutes = (time % HOUR) / MINUTE;
        long seconds = (time % MINUTE) / SECOND;
        return TIME.format(new Long[]{days, hours, minutes, seconds});
    }

    public long getPutMessageSizeTotal() {
        long rs = 0;
        for (AtomicLong data : putMessageTopicSizeTotal.values()) {
            rs += data.get();
        }
        return rs;
    }

    private String getPutMessageDistributeTimeStringInfo(Long total) {
        final StringBuilder sb = new StringBuilder(512);

        for (AtomicLong i : this.putMessageDistributeTime) {
            long value = i.get();
            double ratio = value / total.doubleValue();
            sb.append("\r\n\t\t");
            sb.append(value + "(" + (ratio * 100) + "%)");
        }

        return sb.toString();
    }

    private String getPutTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getPutTps(10));
        sb.append(" ");


        sb.append(this.getPutTps(60));
        sb.append(" ");


        sb.append(this.getPutTps(600));

        return sb.toString();
    }

    private String getGetFoundTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetFoundTps(10));
        sb.append(" ");


        sb.append(this.getGetFoundTps(60));
        sb.append(" ");


        sb.append(this.getGetFoundTps(600));

        return sb.toString();
    }

    private String getGetMissTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetMissTps(10));
        sb.append(" ");


        sb.append(this.getGetMissTps(60));
        sb.append(" ");


        sb.append(this.getGetMissTps(600));

        return sb.toString();
    }

    private String getGetTotalTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetTotalTps(10));
        sb.append(" ");


        sb.append(this.getGetTotalTps(60));
        sb.append(" ");


        sb.append(this.getGetTotalTps(600));

        return sb.toString();
    }

    private String getGetTransferedTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetTransferedTps(10));
        sb.append(" ");


        sb.append(this.getGetTransferedTps(60));
        sb.append(" ");


        sb.append(this.getGetTransferedTps(600));

        return sb.toString();
    }

    private String getPutTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.putTimesList.getLast();

            if (this.putTimesList.size() > time) {
                CallSnapshot lastBefore = this.putTimesList.get(this.putTimesList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.lockSampling.unlock();
        }
        return result;
    }

    private String getGetFoundTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.getTimesFoundList.getLast();

            if (this.getTimesFoundList.size() > time) {
                CallSnapshot lastBefore =
                        this.getTimesFoundList.get(this.getTimesFoundList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }
        } finally {
            this.lockSampling.unlock();
        }

        return result;
    }

    private String getGetMissTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.getTimesMissList.getLast();

            if (this.getTimesMissList.size() > time) {
                CallSnapshot lastBefore =
                        this.getTimesMissList.get(this.getTimesMissList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.lockSampling.unlock();
        }

        return result;
    }

    private String getGetTotalTps(int time) {
        this.lockSampling.lock();
        double found = 0;
        double miss = 0;
        try {
            {
                CallSnapshot last = this.getTimesFoundList.getLast();

                if (this.getTimesFoundList.size() > time) {
                    CallSnapshot lastBefore =
                            this.getTimesFoundList.get(this.getTimesFoundList.size() - (time + 1));
                    found = CallSnapshot.getTPS(lastBefore, last);
                }
            }
            {
                CallSnapshot last = this.getTimesMissList.getLast();

                if (this.getTimesMissList.size() > time) {
                    CallSnapshot lastBefore =
                            this.getTimesMissList.get(this.getTimesMissList.size() - (time + 1));
                    miss = CallSnapshot.getTPS(lastBefore, last);
                }
            }

        } finally {
            this.lockSampling.unlock();
        }

        return Double.toString(found + miss);
    }

    private String getGetTransferedTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.transferedMsgCountList.getLast();

            if (this.transferedMsgCountList.size() > time) {
                CallSnapshot lastBefore =
                        this.transferedMsgCountList.get(this.transferedMsgCountList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.lockSampling.unlock();
        }

        return result;
    }

    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = new HashMap<String, String>(64);

        Long totalTimes = getPutMessageTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        result.put("bootTimestamp", String.valueOf(this.messageStoreBootTimestamp));
        result.put("runtime", this.getFormatRuntime());
        result.put("putMessageEntireTimeMax", String.valueOf(this.putMessageEntireTimeMax));
        result.put("putMessageTimesTotal", String.valueOf(totalTimes));
        result.put("putMessageSizeTotal", String.valueOf(this.getPutMessageSizeTotal()));
        result.put("putMessageDistributeTime",
                String.valueOf(this.getPutMessageDistributeTimeStringInfo(totalTimes)));
        result.put("putMessageAverageSize",
                String.valueOf((this.getPutMessageSizeTotal() / totalTimes.doubleValue())));
        result.put("dispatchMaxBuffer", String.valueOf(this.dispatchMaxBuffer));
        result.put("getMessageEntireTimeMax", String.valueOf(this.getMessageEntireTimeMax));
        result.put("putTps", String.valueOf(this.getPutTps()));
        result.put("getFoundTps", String.valueOf(this.getGetFoundTps()));
        result.put("getMissTps", String.valueOf(this.getGetMissTps()));
        result.put("getTotalTps", String.valueOf(this.getGetTotalTps()));
        result.put("getTransferedTps", String.valueOf(this.getGetTransferedTps()));

        return result;
    }


    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStoped()) {
            try {
                this.waitForRunning(FrequencyOfSampling);

                this.sampling();

                this.printTps();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return StoreStatsService.class.getSimpleName();
    }

    private void sampling() {
        this.lockSampling.lock();
        try {
            this.putTimesList.add(new CallSnapshot(System.currentTimeMillis(), getPutMessageTimesTotal()));
            if (this.putTimesList.size() > (MaxRecordsOfSampling + 1)) {
                this.putTimesList.removeFirst();
            }

            this.getTimesFoundList.add(new CallSnapshot(System.currentTimeMillis(),
                    this.getMessageTimesTotalFound.get()));
            if (this.getTimesFoundList.size() > (MaxRecordsOfSampling + 1)) {
                this.getTimesFoundList.removeFirst();
            }

            this.getTimesMissList.add(new CallSnapshot(System.currentTimeMillis(),
                    this.getMessageTimesTotalMiss.get()));
            if (this.getTimesMissList.size() > (MaxRecordsOfSampling + 1)) {
                this.getTimesMissList.removeFirst();
            }

            this.transferedMsgCountList.add(new CallSnapshot(System.currentTimeMillis(),
                    this.getMessageTransferedMsgCount.get()));
            if (this.transferedMsgCountList.size() > (MaxRecordsOfSampling + 1)) {
                this.transferedMsgCountList.removeFirst();
            }

        } finally {
            this.lockSampling.unlock();
        }
    }
    private void printTps() {
        if (System.currentTimeMillis() > (this.lastPrintTimestamp + PrintTPSInterval * 1000)) {
            this.lastPrintTimestamp = System.currentTimeMillis();

            log.info("[STORETPS] put_tps {} get_found_tps {} get_miss_tps {} get_transfered_tps {}" //
                    , this.getPutTps(PrintTPSInterval) //
                    , this.getGetFoundTps(PrintTPSInterval) //
                    , this.getGetMissTps(PrintTPSInterval) //
                    , this.getGetTransferedTps(PrintTPSInterval)
            );

            final AtomicLong[] times = this.initPutMessageDistributeTime();
            if (null == times) return;

            final StringBuilder sb = new StringBuilder();
            long totalPut = 0;
            for (int i = 0; i < times.length; i++) {
                long value = times[i].get();
                totalPut += value;
                sb.append(String.format("%s:%d", PutMessageEntireTimeMaxDesc[i], value));
                sb.append(" ");
            }

            log.info("[PAGECACHERT] TotalPut {}, PutMessageDistributeTime {}", totalPut, sb.toString());
        }
    }

    public AtomicLong getGetMessageTimesTotalFound() {
        return getMessageTimesTotalFound;
    }


    public AtomicLong getGetMessageTimesTotalMiss() {
        return getMessageTimesTotalMiss;
    }


    public AtomicLong getGetMessageTransferedMsgCount() {
        return getMessageTransferedMsgCount;
    }


    public AtomicLong getPutMessageFailedTimes() {
        return putMessageFailedTimes;
    }


    public AtomicLong getSinglePutMessageTopicSizeTotal(String topic) {
        AtomicLong rs = putMessageTopicSizeTotal.get(topic);
        if (null == rs) {
            rs = new AtomicLong(0);
            putMessageTopicSizeTotal.put(topic, rs);
        }
        return rs;
    }


    public AtomicLong getSinglePutMessageTopicTimesTotal(String topic) {
        AtomicLong rs = putMessageTopicTimesTotal.get(topic);
        if (null == rs) {
            rs = new AtomicLong(0);
            putMessageTopicTimesTotal.put(topic, rs);
        }
        return rs;
    }


    public Map<String, AtomicLong> getPutMessageTopicTimesTotal() {
        return putMessageTopicTimesTotal;
    }


    public Map<String, AtomicLong> getPutMessageTopicSizeTotal() {
        return putMessageTopicSizeTotal;
    }

    static class CallSnapshot {
        public final long timestamp;
        public final long callTimesTotal;


        public CallSnapshot(long timestamp, long callTimesTotal) {
            this.timestamp = timestamp;
            this.callTimesTotal = callTimesTotal;
        }


        public static double getTPS(final CallSnapshot begin, final CallSnapshot end) {
            long total = end.callTimesTotal - begin.callTimesTotal;
            Long time = end.timestamp - begin.timestamp;

            double tps = total / time.doubleValue();

            return tps * 1000;
        }
    }
}
