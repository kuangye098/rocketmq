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

package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.common.UtilAll;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class VirtualEnvUtil {
    public static final String VIRTUAL_APPGROUP_PREFIX = "%%PROJECT_%s%%";


    /**
     * @param origin
     * @param projectGroup
     * @return
     */
    public static String buildWithProjectGroup(String origin, String projectGroup) {
        if (!UtilAll.isBlank(projectGroup)) {
            String prefix = String.format(VIRTUAL_APPGROUP_PREFIX, projectGroup);
            if (!origin.endsWith(prefix)) {
                return origin + prefix;
            } else {
                return origin;
            }
        } else {
            return origin;
        }
    }


    /**
     * @param origin
     * @param projectGroup
     * @return
     */
    public static String clearProjectGroup(String origin, String projectGroup) {
        String prefix = String.format(VIRTUAL_APPGROUP_PREFIX, projectGroup);
        if (!UtilAll.isBlank(prefix) && origin.endsWith(prefix)) {
            return origin.substring(0, origin.lastIndexOf(prefix));
        } else {
            return origin;
        }
    }
}
