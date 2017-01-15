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

import com.alibaba.rocketmq.remoting.common.RemotingHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;


/**
 * @author shijia.wxr
 */
public class UtilAll {
    public static final String yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss";
    public static final String yyyy_MM_dd_HH_mm_ss_SSS = "yyyy-MM-dd#HH:mm:ss:SSS";
    public static final String yyyyMMddHHmmss = "yyyyMMddHHmmss";


    public static int getPid() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();        
        String name = runtime.getName(); // format: "pid@hostname"
        try {
            return Integer.parseInt(name.substring(0, name.indexOf('@')));
        } catch (Exception e) {
            return -1;
        }
    }    
    
    public static String currentStackTrace() {
        StringBuilder sb = new StringBuilder();
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement ste : stackTrace) {
            sb.append("\n\t");
            sb.append(ste.toString());
        }

        return sb.toString();
    }

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static long computeEclipseTimeMilliseconds(final long beginTime) {
        return (System.currentTimeMillis() - beginTime);
    }


    public static boolean isItTimeToDo(final String when) {
        String[] whiles = when.split(";");
        if (whiles != null && whiles.length > 0) {
            Calendar now = Calendar.getInstance();
            for (String w : whiles) {
                int nowHour = Integer.parseInt(w);
                if (nowHour == now.get(Calendar.HOUR_OF_DAY)) {
                    return true;
                }
            }
        }

        return false;
    }


    public static String timeMillisToHumanString() {
        return timeMillisToHumanString(System.currentTimeMillis());
    }


    public static String timeMillisToHumanString(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d%02d%02d%02d%02d%02d%03d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
                cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND),
                cal.get(Calendar.MILLISECOND));
    }


    public static long computNextMorningTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }


    public static long computNextMinutesTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 0);
        cal.add(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }


    public static long computNextHourTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }


    public static long computNextHalfHourTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        cal.set(Calendar.MINUTE, 30);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }


    public static String timeMillisToHumanString2(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d,%03d",//
                cal.get(Calendar.YEAR),//
                cal.get(Calendar.MONTH) + 1,//
                cal.get(Calendar.DAY_OF_MONTH),//
                cal.get(Calendar.HOUR_OF_DAY),//
                cal.get(Calendar.MINUTE),//
                cal.get(Calendar.SECOND),//
                cal.get(Calendar.MILLISECOND));
    }


    public static String timeMillisToHumanString3(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d%02d%02d%02d%02d%02d",//
                cal.get(Calendar.YEAR),//
                cal.get(Calendar.MONTH) + 1,//
                cal.get(Calendar.DAY_OF_MONTH),//
                cal.get(Calendar.HOUR_OF_DAY),//
                cal.get(Calendar.MINUTE),//
                cal.get(Calendar.SECOND));
    }


    public static double getDiskPartitionSpaceUsedPercent(final String path) {
        if (null == path || path.isEmpty())
            return -1;

        try {
            File file = new File(path);
            if (!file.exists()) {
                boolean result = file.mkdirs();
                if (!result) {
                }
            }

            long totalSpace = file.getTotalSpace();
            long freeSpace = file.getFreeSpace();
            long usedSpace = totalSpace - freeSpace;
            if (totalSpace > 0) {
                return usedSpace / (double) totalSpace;
            }
        } catch (Exception e) {
            return -1;
        }

        return -1;
    }


    public static final int crc32(byte[] array) {
        if (array != null) {
            return crc32(array, 0, array.length);
        }

        return 0;
    }


    public static final int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }

    public static String bytes2string(byte[] src) {
        StringBuilder sb = new StringBuilder();
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                sb.append(0);
            }
            sb.append(hv.toUpperCase());
        }
        return sb.toString();
    }

    public static byte[] string2bytes(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }


    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }


    public static byte[] uncompress(final byte[] src) throws IOException {
        byte[] result = src;
        byte[] uncompressData = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

        try {
            while (true) {
                int len = inflaterInputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                byteArrayOutputStream.write(uncompressData, 0, len);
            }
            byteArrayOutputStream.flush();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {
            }
            try {
                inflaterInputStream.close();
            } catch (IOException e) {
            }
            try {
                byteArrayOutputStream.close();
            } catch (IOException e) {
            }
        }

        return result;
    }


    public static byte[] compress(final byte[] src, final int level) throws IOException {
        byte[] result = src;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        java.util.zip.Deflater deflater = new java.util.zip.Deflater(level);
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream, deflater);
        try {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            deflaterOutputStream.close();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            deflater.end();
            throw e;
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException e) {
            }

            deflater.end();
        }

        return result;
    }


    public static int asInt(String str, int defaultValue) {
        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    public static long asLong(String str, long defaultValue) {
        try {
            return Long.parseLong(str);
        } catch (Exception e) {
            return defaultValue;
        }
    }


    public static String formatDate(Date date, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        return df.format(date);
    }


    public static Date parseDate(String date, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        try {
            return df.parse(date);
        } catch (ParseException e) {
            return null;
        }
    }


    public static String responseCode2String(final int code) {
        return Integer.toString(code);
    }


    public static String frontStringAtLeast(final String str, final int size) {
        if (str != null) {
            if (str.length() > size) {
                return str.substring(0, size);
            }
        }

        return str;
    }


    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }


    public static String jstack() {
        return jstack(Thread.getAllStackTraces());
    }


    public static String jstack(Map<Thread, StackTraceElement[]> map) {
        StringBuilder result = new StringBuilder();
        try {
            Iterator<Map.Entry<Thread, StackTraceElement[]>> ite = map.entrySet().iterator();
            while (ite.hasNext()) {
                Map.Entry<Thread, StackTraceElement[]> entry = ite.next();
                StackTraceElement[] elements = entry.getValue();
                Thread thread = entry.getKey();
                if (elements != null && elements.length > 0) {
                    String threadName = entry.getKey().getName();
                    result.append(String.format("%-40sTID: %d STATE: %s%n", threadName, thread.getId(), thread.getState()));
                    for (StackTraceElement el : elements) {
                        result.append(String.format("%-40s%s%n", threadName, el.toString()));
                    }
                    result.append("\n");
                }
            }
        } catch (Throwable e) {
            result.append(RemotingHelper.exceptionSimpleDesc(e));
        }

        return result.toString();
    }

    public static boolean isInternalIP(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }
        

        //10.0.0.0~10.255.255.255
        //172.16.0.0~172.31.255.255
        //192.168.0.0~192.168.255.255
        if (ip[0] == (byte)10) {

            return true;
        }
        else if (ip[0] == (byte)172) {
            if (ip[1] >= (byte)16 && ip[1] <= (byte)31) {
                return true;
            }
        }
        else if (ip[0] == (byte)192) {
            if (ip[1] == (byte)168) {
                return true;
            }
        }
        return false;
    }

    private static boolean ipCheck(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }
        
//        if (ip[0] == (byte)30 && ip[1] == (byte)10 && ip[2] == (byte)163 && ip[3] == (byte)120) {
//            System.out.println("right!");
//        }
        

        if (ip[0] >= (byte)1 && ip[0] <= (byte)126) {
            if (ip[1] == (byte)1 && ip[2] == (byte)1 && ip[3] == (byte)1) {
                return false;
            }
            if (ip[1] == (byte)0 && ip[2] == (byte)0 && ip[3] == (byte)0) { 
                return false;
            }
            return true;
        }
        else if (ip[0] >= (byte)128 && ip[0] <= (byte)191) {
            if (ip[2] == (byte)1 && ip[3] == (byte)1) {
                return false;
            }
            if (ip[2] == (byte)0 && ip[3] == (byte)0) {
                return false;
            }
            return true;
        }
        else if (ip[0] >= (byte)192 && ip[0] <= (byte)223) {
            if (ip[3] == (byte)1) {
                return false;
            }
            if (ip[3] == (byte)0) {
                return false;
            }
            return true;
        }
        return false;
    }

    public static String ipToIPv4Str(byte[] ip) {
        if (ip.length != 4) {
            return null;
        }
        return new StringBuilder().append(ip[0] & 0xFF).append(".").append(   
                ip[1] & 0xFF).append(".").append(ip[2] & 0xFF)   
                .append(".").append(ip[3] & 0xFF).toString(); 
    }
    
    public static byte[] getIP() {
        try {
            Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            byte[] internalIP = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                Enumeration addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = (InetAddress) addresses.nextElement();
                    if (ip != null && ip instanceof Inet4Address) {
                        byte[] ipByte = ip.getAddress();
                        if (ipByte.length == 4) {
                            if (ipCheck(ipByte)) {
                                   if (!isInternalIP(ipByte)) {
                                        return ipByte;
                                    }
                                   else if (internalIP == null){
                                       internalIP = ipByte;
                                   }
                           }
                        }
                    } 
                }
            }
            if (internalIP != null) {
                return internalIP;
            }
            else {
                throw new RuntimeException("Can not get local ip");
            }
        }catch (Exception e) {
            throw new RuntimeException("Can not get local ip", e);
        }
}}
