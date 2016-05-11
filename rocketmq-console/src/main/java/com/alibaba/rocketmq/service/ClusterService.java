package com.alibaba.rocketmq.service;

import static com.alibaba.rocketmq.common.Tool.str;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.cluster.ClusterListSubCommand;
import com.alibaba.rocketmq.validate.CmdTrace;


/**
 * 集群服务类
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-8
 */
@Service
public class ClusterService extends AbstractService {

    static final Logger logger = LoggerFactory.getLogger(ClusterService.class);


    @CmdTrace(cmdClazz = ClusterListSubCommand.class)
    public Table list() throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            Table table = doList(defaultMQAdminExt);
            return table;
        }
        catch (Throwable e) {
            logger.error(e.getMessage(), e);
            t = e;
        }
        finally {
            shutdownDefaultMQAdminExt(defaultMQAdminExt);
        }
        throw t;
    }


    private Table doList(DefaultMQAdminExt defaultMQAdminExt) throws Exception {

        ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
        // System.out.printf("%-16s  %-32s  %-4s  %-22s %-22s %11s %11s\n",//
        // "#Cluster Name",//
        // "#Broker Name",//
        // "#BID",//
        // "#Addr",//
        // "#Version",//
        // "#InTPS",//
        // "#OutTPS"//
        // );
        // String[] thead =
        // new String[] { "#Cluster Name", "#Broker Name", "#BID", "#Addr",
        // "#Version", "#InTPS",
        // "#OutTPS", "#InTotalYest", "#OutTotalYest", "#InTotalToday",
        // "#OutTotalToday" };
        String[] instanceThead =
                new String[] { "#BID", "#Addr", "#Version", "#InTPS", "#OutTPS", "#InTotalYest",
                              "#OutTotalYest", "#InTotalToday", "#OutTotalToday" };

        Set<Map.Entry<String, Set<String>>> clusterSet =
                clusterInfoSerializeWrapper.getClusterAddrTable().entrySet();

        int clusterRow = clusterSet.size();
        Table clusterTable = new Table(new String[] { "#Cluster Name", "#Broker Detail" }, clusterRow);
        Iterator<Map.Entry<String, Set<String>>> itCluster = clusterSet.iterator();

        while (itCluster.hasNext()) {
            Map.Entry<String, Set<String>> next = itCluster.next();
            String clusterName = next.getKey();
            Set<String> brokerNameSet = new HashSet<String>();
            brokerNameSet.addAll(next.getValue());

            Object[] clusterTR = clusterTable.createTR();
            clusterTR[0] = clusterName;
            Table brokerTable =
                    new Table(new String[] { "#Broker Name", "#Broker Instance" }, brokerNameSet.size());
            clusterTR[1] = brokerTable;
            clusterTable.insertTR(clusterTR);// A

            for (String brokerName : brokerNameSet) {
                Object[] brokerTR = brokerTable.createTR();
                brokerTR[0] = brokerName;
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {
                    Set<Map.Entry<Long, String>> brokerAddrSet = brokerData.getBrokerAddrs().entrySet();
                    Iterator<Map.Entry<Long, String>> itAddr = brokerAddrSet.iterator();

                    Table instanceTable = new Table(instanceThead, brokerAddrSet.size());
                    brokerTR[1] = instanceTable;
                    brokerTable.insertTR(brokerTR);// B

                    while (itAddr.hasNext()) {
                        Object[] instanceTR = instanceTable.createTR();
                        Map.Entry<Long, String> next1 = itAddr.next();
                        double in = 0;
                        double out = 0;
                        String version = "";

                        long InTotalYest = 0;
                        long OutTotalYest = 0;
                        long InTotalToday = 0;
                        long OutTotalToday = 0;

                        try {
                            KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
                            String putTps = kvTable.getTable().get("putTps");
                            String getTransferedTps = kvTable.getTable().get("getTransferedTps");
                            version = kvTable.getTable().get("brokerVersionDesc");
                            {
                                String[] tpss = putTps.split(" ");
                                if (tpss != null && tpss.length > 0) {
                                    in = Double.parseDouble(tpss[0]);
                                }
                            }

                            {
                                String[] tpss = getTransferedTps.split(" ");
                                if (tpss != null && tpss.length > 0) {
                                    out = Double.parseDouble(tpss[0]);
                                }
                            }

                            instanceTR[0] = str(next1.getKey().longValue());
                            instanceTR[1] = next1.getValue();
                            instanceTR[2] = version;
                            instanceTR[3] = str(in);
                            instanceTR[4] = str(out);

                            String msgPutTotalYesterdayMorning =
                                    kvTable.getTable().get("msgPutTotalYesterdayMorning");
                            String msgPutTotalTodayMorning =
                                    kvTable.getTable().get("msgPutTotalTodayMorning");
                            String msgPutTotalTodayNow = kvTable.getTable().get("msgPutTotalTodayNow");
                            String msgGetTotalYesterdayMorning =
                                    kvTable.getTable().get("msgGetTotalYesterdayMorning");
                            String msgGetTotalTodayMorning =
                                    kvTable.getTable().get("msgGetTotalTodayMorning");
                            String msgGetTotalTodayNow = kvTable.getTable().get("msgGetTotalTodayNow");

                            InTotalYest =
                                    Long.parseLong(msgPutTotalTodayMorning)
                                            - Long.parseLong(msgPutTotalYesterdayMorning);
                            OutTotalYest =
                                    Long.parseLong(msgGetTotalTodayMorning)
                                            - Long.parseLong(msgGetTotalYesterdayMorning);

                            InTotalToday =
                                    Long.parseLong(msgPutTotalTodayNow)
                                            - Long.parseLong(msgPutTotalTodayMorning);
                            OutTotalToday =
                                    Long.parseLong(msgGetTotalTodayNow)
                                            - Long.parseLong(msgGetTotalTodayMorning);

                            instanceTR[5] = str(InTotalYest);
                            instanceTR[6] = str(OutTotalYest);
                            instanceTR[7] = str(InTotalToday);
                            instanceTR[8] = str(OutTotalToday);
                            instanceTable.insertTR(instanceTR);// C
                        }
                        catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                        //
                        // System.out.printf("%-16s  %-32s  %-4s  %-22s %-22s %11.2f %11.2f\n",//
                        // clusterName,//
                        // brokerName,//
                        // next1.getKey().longValue(),//
                        // next1.getValue(),//
                        // version,//
                        // in,//
                        // out//
                        // );

                        // System.out.printf("%-16s  %-32s %14d %14d %14d %14d\n",//
                        // clusterName,//
                        // brokerName,//
                        // InTotalYest,//
                        // OutTotalYest,//
                        // InTotalToday,//
                        // OutTotalToday//
                        // );
                    }
                }
            }
        }
        return clusterTable;
    }

}
