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

package com.alibaba.rocketmq.common;

import java.util.concurrent.atomic.AtomicBoolean;

public class BrokerConfigSingleton {
    private static AtomicBoolean isInit = new AtomicBoolean();
    private static BrokerConfig brokerConfig;

    public static BrokerConfig getBrokerConfig() {
        if (brokerConfig == null) {
            throw new IllegalArgumentException("brokerConfig Cannot be null !");
        }
        return brokerConfig;
    }

    public static void setBrokerConfig(BrokerConfig brokerConfig) {
        if (!isInit.compareAndSet(false, true)) {
            throw new IllegalArgumentException("broker config have inited !");
        }
        BrokerConfigSingleton.brokerConfig = brokerConfig;
    }
}
