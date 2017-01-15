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

import java.nio.ByteBuffer;


/**
 * Write messages callback interface
 *
 * @author shijia.wxr
 *
 */
public interface AppendMessageCallback {

    /**
     * After message serialization, write MapedByteBuffer
     *
     * @param byteBuffer
     * @param maxBlank
     * @param msg
     *
     * @return How many bytes to write
     */
    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                                        final int maxBlank, final Object msg);
}
