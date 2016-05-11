package com.alibaba.rocketmq.service;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.admin.RollbackStats;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.offset.ResetOffsetByTimeOldCommand;
import com.alibaba.rocketmq.validate.CmdTrace;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;

import static com.alibaba.rocketmq.common.Tool.bool;
import static com.alibaba.rocketmq.common.Tool.str;
import static org.apache.commons.lang.StringUtils.isNotBlank;


/**
 * @see com.alibaba.rocketmq.tools.command.offset.ResetOffsetByTimeOldCommand
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-19
 */
@Service
public class OffsetService extends AbstractService {

    static final Logger logger = LoggerFactory.getLogger(OffsetService.class);

    static final ResetOffsetByTimeOldCommand resetOffsetByTimeSubCommand = new ResetOffsetByTimeOldCommand();


    public Collection<Option> getOptionsForResetOffsetByTime() {
        return getOptions(resetOffsetByTimeSubCommand);
    }


    @CmdTrace(cmdClazz = ResetOffsetByTimeOldCommand.class)
    public Table resetOffsetByTime(String consumerGroup, String topic, String timeStampStr, String forceStr)
            throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            long timestamp = 0;
            try {
                // 直接输入 long 类型的 timestamp
                timestamp = Long.valueOf(timeStampStr);
            }
            catch (NumberFormatException e) {
                // 输入的为日期格式，精确到毫秒
                timestamp = UtilAll.parseDate(timeStampStr, UtilAll.yyyy_MM_dd_HH_mm_ss_SSS).getTime();
            }

            boolean force = true;
            if (isNotBlank(forceStr)) {
                force = bool(forceStr.trim());
            }
            defaultMQAdminExt.start();
            List<RollbackStats> rollbackStatsList =
                    defaultMQAdminExt.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, force);
            // System.out
            // .printf(
            // "rollback consumer offset by specified consumerGroup[%s], topic[%s], force[%s], timestamp(string)[%s], timestamp(long)[%s]\n",
            // consumerGroup, topic, force, timeStampStr, timestamp);
            //
            // System.out.printf("%-20s  %-20s  %-20s  %-20s  %-20s  %-20s\n",//
            // "#brokerName",//
            // "#queueId",//
            // "#brokerOffset",//
            // "#consumerOffset",//
            // "#timestampOffset",//
            // "#rollbackOffset" //
            // );
            String[] thead =
                    new String[] { "#brokerName", "#queueId", "#brokerOffset", "#consumerOffset",
                                  "#timestampOffset", "#rollbackOffset" };
            Table table = new Table(thead, rollbackStatsList.size());

            for (RollbackStats rollbackStats : rollbackStatsList) {
                // System.out.printf("%-20s  %-20d  %-20d  %-20d  %-20d  %-20d\n",//
                // UtilAll.frontStringAtLeast(rollbackStats.getBrokerName(),
                // 32),//
                // rollbackStats.getQueueId(),//
                // rollbackStats.getBrokerOffset(),//
                // rollbackStats.getConsumerOffset(),//
                // rollbackStats.getTimestampOffset(),//
                // rollbackStats.getRollbackOffset() //
                // );
                Object[] tr = table.createTR();
                tr[0] = UtilAll.frontStringAtLeast(rollbackStats.getBrokerName(), 32);
                tr[1] = str(rollbackStats.getQueueId());
                tr[2] = str(rollbackStats.getBrokerOffset());
                tr[3] = str(rollbackStats.getConsumerOffset());
                tr[4] = str(rollbackStats.getTimestampOffset());
                tr[5] = str(rollbackStats.getRollbackOffset());
                table.insertTR(tr);
            }
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
}
