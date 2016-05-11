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
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.service.TopicService;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-11
 */
@Controller
@RequestMapping("/topic")
public class TopicAction extends AbstractAction {

    @Autowired
    TopicService topicService;


    protected String getFlag() {
        return "topic_flag";
    }


    @Override
    protected String getName() {
        return "Topic";
    }


    @RequestMapping(value = "/list.do", method = RequestMethod.GET)
    public String list(ModelMap map) {
        putPublicAttribute(map, "list");
        try {
            Table table = topicService.list();
            putTable(map, table);
        }
        catch (Throwable t) {
            putAlertMsg(t, map);
        }
        return TEMPLATE;
    }


    @RequestMapping(value = "/stats.do", method = RequestMethod.GET)
    public String stats(ModelMap map, @RequestParam String topic) {
        putPublicAttribute(map, "stats");
        try {
            Table table = topicService.stats(topic);
            putTable(map, table);
        }
        catch (Throwable t) {
            putAlertMsg(t, map);
        }
        return TEMPLATE;
    }


    @RequestMapping(value = "/add.do", method = RequestMethod.GET)
    public String add(ModelMap map) {
        putPublicAttribute(map, "add");
        Collection<Option> options = topicService.getOptionsForUpdate();
        putOptions(map, options);
        map.put(FORM_ACTION, "update.do");// add as update
        return TEMPLATE;
    }


    @RequestMapping(value = "/route.do", method = RequestMethod.GET)
    public String route(ModelMap map, @RequestParam String topic) {
        putPublicAttribute(map, "route");
        try {
            TopicRouteData topicRouteData = topicService.route(topic);
            map.put("topicRouteData", topicRouteData);
        }
        catch (Throwable t) {
            putAlertMsg(t, map);
        }
        return TEMPLATE;
    }


    @RequestMapping(value = "/delete.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String delete(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String clusterName, @RequestParam String topic) {
        Collection<Option> options = topicService.getOptionsForDelete();
        putPublicAttribute(map, "delete", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                topicService.delete(topic, clusterName);
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


    @RequestMapping(value = "/update.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String update(ModelMap map, HttpServletRequest request, @RequestParam String topic,
            @RequestParam(required = false) String readQueueNums,
            @RequestParam(required = false) String writeQueueNums,
            @RequestParam(required = false) String perm, @RequestParam(required = false) String brokerAddr,
            @RequestParam(required = false) String clusterName) {
        Collection<Option> options = topicService.getOptionsForUpdate();
        putPublicAttribute(map, "update", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                topicService.update(topic, readQueueNums, writeQueueNums, perm, brokerAddr, clusterName);
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

}
