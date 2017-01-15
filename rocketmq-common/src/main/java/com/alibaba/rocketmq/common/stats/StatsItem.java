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

package com.alibaba.rocketmq.common.stats;

import com.alibaba.rocketmq.common.UtilAll;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class StatsItem {

    private final AtomicLong value = new AtomicLong(0);

    private final AtomicLong times = new AtomicLong(0);

    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<CallSnapshot>();


    private final LinkedList<CallSnapshot> csListHour = new LinkedList<CallSnapshot>();


    private final LinkedList<CallSnapshot> csListDay = new LinkedList<CallSnapshot>();

    private final String statsName;
    private final String statsKey;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Logger log;


    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService,
                     Logger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }

    public StatsSnapshot getStatsDataInMinute() {
        return computeStatsData(this.csListMinute);
    }

    private static StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
        StatsSnapshot statsSnapshot = new StatsSnapshot();
        synchronized (csList) {
            double tps = 0;
            double avgpt = 0;
            long sum = 0;
            if (!csList.isEmpty()) {
                CallSnapshot first = csList.getFirst();
                CallSnapshot last = csList.getLast();
                sum = last.getValue() - first.getValue();
                tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

                long timesDiff = last.getTimes() - first.getTimes();
                if (timesDiff > 0) {
                    avgpt = (sum * 1.0d) / (timesDiff);
                }
            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
        }

        return statsSnapshot;
    }

    public StatsSnapshot getStatsDataInHour() {
        return computeStatsData(this.csListHour);
    }

    public StatsSnapshot getStatsDataInDay() {
        return computeStatsData(this.csListDay);
    }

    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                } catch (Throwable e) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);


        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInMinutes();
                } catch (Throwable e) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);


        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInHour();
                } catch (Throwable e) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);


        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                                                              @Override
                                                              public void run() {
                                                                  try {
                                                                      printAtMinutes();
                                                                  } catch (Throwable e) {
                                                                  }
                                                              }
                                                          }, Math.abs(UtilAll.computNextMinutesTimeMillis() - System.currentTimeMillis()), //
                1000 * 60, TimeUnit.MILLISECONDS);


        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                                                              @Override
                                                              public void run() {
                                                                  try {
                                                                      printAtHour();
                                                                  } catch (Throwable e) {
                                                                  }
                                                              }
                                                          }, Math.abs(UtilAll.computNextHourTimeMillis() - System.currentTimeMillis()), //
                1000 * 60 * 60, TimeUnit.MILLISECONDS);


        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                                                              @Override
                                                              public void run() {
                                                                  try {
                                                                      printAtDay();
                                                                  } catch (Throwable e) {
                                                                  }
                                                              }
                                                          }, Math.abs(UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, //
                1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    public void samplingInSeconds() {
        synchronized (this.csListMinute) {
            this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                    .get()));
            if (this.csListMinute.size() > 7) {
                this.csListMinute.removeFirst();
            }
        }
    }

    public void samplingInMinutes() {
        synchronized (this.csListHour) {
            this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                    .get()));
            if (this.csListHour.size() > 7) {
                this.csListHour.removeFirst();
            }
        }
    }

    public void samplingInHour() {
        synchronized (this.csListDay) {
            this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value
                    .get()));
            if (this.csListDay.size() > 25) {
                this.csListDay.removeFirst();
            }
        }
    }

    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        log.info(String.format("[%s] [%s] Stats In One Minute, SUM: %d TPS: %.2f AVGPT: %.2f", //
                this.statsName,//
                this.statsKey,//
                ss.getSum(),//
                ss.getTps(),//
                ss.getAvgpt()));
    }

    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        log.info(String.format("[%s] [%s] Stats In One Hour, SUM: %d TPS: %.2f AVGPT: %.2f", //
                this.statsName,//
                this.statsKey,//
                ss.getSum(),//
                ss.getTps(),//
                ss.getAvgpt()));
    }

    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        log.info(String.format("[%s] [%s] Stats In One Day, SUM: %d TPS: %.2f AVGPT: %.2f", //
                this.statsName,//
                this.statsKey,//
                ss.getSum(),//
                ss.getTps(),//
                ss.getAvgpt()));
    }

    public AtomicLong getValue() {
        return value;
    }


    public String getStatsKey() {
        return statsKey;
    }


    public String getStatsName() {
        return statsName;
    }


    public AtomicLong getTimes() {
        return times;
    }
}


class CallSnapshot {
    private final long timestamp;
    private final long times;

    private final long value;


    public CallSnapshot(long timestamp, long times, long value) {
        super();
        this.timestamp = timestamp;
        this.times = times;
        this.value = value;
    }


    public long getTimestamp() {
        return timestamp;
    }


    public long getTimes() {
        return times;
    }


    public long getValue() {
        return value;
    }
}
