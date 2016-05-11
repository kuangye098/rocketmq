package com.alibaba.rocketmq.action;

import java.util.Collection;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.cli.Option;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.service.ConsumerService;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-18
 */
@Controller
@RequestMapping("/consumer")
public class ConsumerAction extends AbstractAction {

    @Autowired
    ConsumerService consumerService;


    @Override
    protected String getFlag() {
        return "consumer_flag";
    }


    @RequestMapping(value = "/consumerProgress.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String consumerProgress(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String groupName) {
        Collection<Option> options = consumerService.getOptionsForConsumerProgress();
        putPublicAttribute(map, "consumerProgress", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                Table table = consumerService.consumerProgress(groupName);
                putTable(map, table);
            }
            else {
                throwUnknowRequestMethodException(request);
            }
        }
        catch (Throwable t) {
            putAlertMsg(t, map);
        }
        return TEMPLATE;
    }


    @RequestMapping(value = "/deleteSubGroup.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String deleteSubGroup(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String groupName,
            @RequestParam(required = false) String brokerAddr,
            @RequestParam(required = false) String clusterName) {
        Collection<Option> options = consumerService.getOptionsForDeleteSubGroup();
        putPublicAttribute(map, "deleteSubGroup", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                consumerService.deleteSubGroup(groupName, brokerAddr, clusterName);
                putAlertTrue(map);
            }
            else {
                throwUnknowRequestMethodException(request);
            }
        }
        catch (Throwable t) {
            putAlertMsg(t, map);
        }
        return TEMPLATE;
    }


    @RequestMapping(value = "/updateSubGroup.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String updateSubGroup(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String brokerAddr,
            @RequestParam(required = false) String clusterName,
            @RequestParam(required = false) String groupName,
            @RequestParam(required = false) String consumeEnable,
            @RequestParam(required = false) String consumeFromMinEnable,
            @RequestParam(required = false) String consumeBroadcastEnable,
            @RequestParam(required = false) String retryQueueNums,
            @RequestParam(required = false) String retryMaxTimes,
            @RequestParam(required = false) String brokerId,
            @RequestParam(required = false) String whichBrokerWhenConsumeSlowly) {
        Collection<Option> options = consumerService.getOptionsForUpdateSubGroup();
        putPublicAttribute(map, "updateSubGroup", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                consumerService.updateSubGroup(brokerAddr, clusterName, groupName, consumeEnable,
                    consumeFromMinEnable, consumeBroadcastEnable, retryQueueNums, retryMaxTimes, brokerId,
                    whichBrokerWhenConsumeSlowly);
                putAlertTrue(map);
            }
            else {
                throwUnknowRequestMethodException(request);
            }
        }
        catch (Throwable t) {
            putAlertMsg(t, map);
        }
        return TEMPLATE;
    }


    @Override
    protected String getName() {
        return "Consumer";
    }
}
