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
import com.alibaba.rocketmq.service.NamesrvService;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-17
 */
@Controller
@RequestMapping("/namesrv")
public class NamesrvAction extends AbstractAction {

    @Autowired
    NamesrvService namesrvService;


    @Override
    protected String getFlag() {
        return "namesrv_flag";
    }


    @RequestMapping(value = "/updateKvConfig.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String updateKvConfig(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String namespace, @RequestParam(required = false) String key,
            @RequestParam(required = false) String value) {
        Collection<Option> options = namesrvService.getOptionsForUpdateKvConfig();
        putPublicAttribute(map, "updateKvConfig", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                namesrvService.updateKvConfig(namespace, key, value);
                putAlertTrue(map);
            }
            else {
                throwUnknowRequestMethodException(request);
            }
        }
        catch (Throwable e) {
            putAlertMsg(e, map);
        }

        return TEMPLATE;
    }


    @RequestMapping(value = "/deleteKvConfig.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String deleteKvConfig(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String namespace, @RequestParam(required = false) String key) {
        Collection<Option> options = namesrvService.getOptionsForDeleteKvConfig();
        putPublicAttribute(map, "deleteKvConfig", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                namesrvService.deleteKvConfig(namespace, key);
                putAlertTrue(map);
            }
            else {
                throwUnknowRequestMethodException(request);
            }
        }
        catch (Throwable e) {
            putAlertMsg(e, map);
        }
        return TEMPLATE;
    }


    @RequestMapping(value = "/getProjectGroup.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String getProjectGroup(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String ip, @RequestParam(required = false) String project) {
        Collection<Option> options = namesrvService.getOptionsForGetProjectGroup();
        putPublicAttribute(map, "getProjectGroup", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                String text = namesrvService.getProjectGroup(ip, project);
                map.put("resultText", text);
            }
            else {
                throwUnknowRequestMethodException(request);
            }
        }
        catch (Throwable e) {
            putAlertMsg(e, map);
        }
        return TEMPLATE;
    }


    @RequestMapping(value = "/updateProjectGroup.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String updateProjectGroup(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String ip, @RequestParam(required = false) String project) {
        Collection<Option> options = namesrvService.getOptionsForGetProjectGroup();
        putPublicAttribute(map, "updateProjectGroup", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                namesrvService.updateProjectGroup(ip, project);
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


    @RequestMapping(value = "/deleteProjectGroup.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String deleteProjectGroup(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String ip, @RequestParam(required = false) String project) {
        Collection<Option> options = namesrvService.getOptionsForDeleteProjectGroup();
        putPublicAttribute(map, "deleteProjectGroup", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                namesrvService.deleteProjectGroup(ip, project);
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


    @RequestMapping(value = "/wipeWritePerm.do", method = { RequestMethod.GET, RequestMethod.POST })
    public String wipeWritePerm(ModelMap map, HttpServletRequest request,
            @RequestParam(required = false) String brokerName) {
        Collection<Option> options = namesrvService.getOptionsForWipeWritePerm();
        putPublicAttribute(map, "wipeWritePerm", options, request);
        try {
            if (request.getMethod().equals(GET)) {

            }
            else if (request.getMethod().equals(POST)) {
                checkOptions(options);
                Table table = namesrvService.wipeWritePerm(brokerName);
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
        return "Namesrv";
    }

}
