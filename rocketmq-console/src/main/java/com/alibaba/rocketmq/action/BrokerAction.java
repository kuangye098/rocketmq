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
import com.alibaba.rocketmq.service.BrokerService;


@Controller
@RequestMapping("/broker")
public class BrokerAction extends AbstractAction {

    @Autowired
    BrokerService brokerService;


    protected String getFlag() {
        return "broker_flag";
    }


    @RequestMapping(value = "/brokerStats.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String brokerStats(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String brokerAddr) {
        Collection<Option> options = brokerService.getOptionsForBrokerStats();
        putPublicAttribute(map, "brokerStats", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                Table table = brokerService.brokerStats(brokerAddr);
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


    @RequestMapping(value = "/updateBrokerConfig.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String updateBrokerConfig(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String brokerAddr,
            @RequestParam(required = false) String clusterName, @RequestParam(required = false) String key,
            @RequestParam(required = false) String value) {
        Collection<Option> options = brokerService.getOptionsForUpdateBrokerConfig();
        putPublicAttribute(map, "updateBrokerConfig", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                brokerService.updateBrokerConfig(brokerAddr, clusterName, key, value);
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
        return "Broker";
    }
}
