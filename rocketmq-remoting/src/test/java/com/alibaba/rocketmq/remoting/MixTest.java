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

/**
 * $Id: MixTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting;

import org.junit.Test;

import static com.alibaba.rocketmq.remoting.common.RemotingUtil.isInnerIP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * @author shijia.wxr
 */
public class MixTest {
    @Test
    public void test_extFieldsValue() {

    }

    @Test
    public void test_InnerIp() {
        assertTrue(isInnerIP("192.168.1.1"));
        assertTrue(isInnerIP("10.1.1.1"));
        assertTrue(isInnerIP("172.18.1.2"));
        assertFalse(isInnerIP("4.4.4.4"));
        assertFalse(isInnerIP("120.234.48.162"));
    }
}
