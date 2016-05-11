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
import com.alibaba.rocketmq.service.OffsetService;


/**
 * 
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-19
 */
@Controller
@RequestMapping("/offset")
public class OffsetAction extends AbstractAction {

    @Autowired
    OffsetService offsetService;


    @Override
    protected String getFlag() {
        return "offset_flag";
    }


    @RequestMapping(value = "/resetOffsetByTime.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String resetOffsetByTime(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String group, @RequestParam(required = false) String topic,
            @RequestParam(required = false) String timestamp, @RequestParam(required = false) String force) {
        Collection<Option> options = offsetService.getOptionsForResetOffsetByTime();
        putPublicAttribute(map, "resetOffsetByTime", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                Table table = offsetService.resetOffsetByTime(group, topic, timestamp, force);
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


    @Override
    protected String getName() {
        return "Offset";
    }
}
