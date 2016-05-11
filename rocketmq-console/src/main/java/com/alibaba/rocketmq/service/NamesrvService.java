package com.alibaba.rocketmq.service;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.alibaba.rocketmq.common.Table;
import com.alibaba.rocketmq.common.namesrv.NamesrvUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.namesrv.DeleteKvConfigCommand;
import com.alibaba.rocketmq.tools.command.namesrv.DeleteProjectGroupCommand;
import com.alibaba.rocketmq.tools.command.namesrv.GetProjectGroupCommand;
import com.alibaba.rocketmq.tools.command.namesrv.UpdateKvConfigCommand;
import com.alibaba.rocketmq.tools.command.namesrv.UpdateProjectGroupCommand;
import com.alibaba.rocketmq.tools.command.namesrv.WipeWritePermSubCommand;
import com.alibaba.rocketmq.validate.CmdTrace;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-17
 */
@Service
public class NamesrvService extends AbstractService {

    static final Logger logger = LoggerFactory.getLogger(NamesrvService.class);

    static final DeleteKvConfigCommand deleteKvConfigCommand = new DeleteKvConfigCommand();


    public Collection<Option> getOptionsForDeleteKvConfig() {
        return getOptions(deleteKvConfigCommand);
    }


    @CmdTrace(cmdClazz = DeleteKvConfigCommand.class)
    public boolean deleteKvConfig(String namespace, String key) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            defaultMQAdminExt.deleteKvConfig(namespace, key);
            return true;
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

    static final DeleteProjectGroupCommand deleteProjectGroupCommand = new DeleteProjectGroupCommand();
    
    public Collection<Option> getOptionsForDeleteProjectGroup() {
        return getOptions(deleteProjectGroupCommand);
    }
    
    @CmdTrace(cmdClazz = DeleteProjectGroupCommand.class)
    public boolean deleteProjectGroup(String ip, String project) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        String namespace = NamesrvUtil.NAMESPACE_PROJECT_CONFIG;
        try {
            if (StringUtils.isNotBlank(ip)) {
                defaultMQAdminExt.start();
                defaultMQAdminExt.deleteKvConfig(namespace, ip);
                return true;
            }
            else if (StringUtils.isNotBlank(project)) {
                defaultMQAdminExt.start();
                defaultMQAdminExt.deleteIpsByProjectGroup(project);
                return true;
            }
            else {
                throw new IllegalStateException("project or ip can not be all blank!");
            }
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

    static final GetProjectGroupCommand getProjectGroupCommand = new GetProjectGroupCommand();


    public Collection<Option> getOptionsForGetProjectGroup() {
        return getOptions(getProjectGroupCommand);
    }


    @CmdTrace(cmdClazz = GetProjectGroupCommand.class)
    public String getProjectGroup(String ip, String project) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            if (StringUtils.isNotBlank(ip)) {
                defaultMQAdminExt.start();
                String projectInfo = defaultMQAdminExt.getProjectGroupByIp(ip);
                return projectInfo;
            }
            else if (StringUtils.isNotBlank(project)) {
                defaultMQAdminExt.start();
                String ips = defaultMQAdminExt.getIpsByProjectGroup(project);
                return ips;
            }
            else {
                throw new IllegalStateException("project or ip can not be all blank!");
            }
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

    static final UpdateKvConfigCommand updateKvConfigCommand = new UpdateKvConfigCommand();


    public Collection<Option> getOptionsForUpdateKvConfig() {
        return getOptions(updateKvConfigCommand);
    }


    @CmdTrace(cmdClazz = UpdateKvConfigCommand.class)
    public boolean updateKvConfig(String namespace, String key, String value) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            defaultMQAdminExt.createAndUpdateKvConfig(namespace, key, value);
            return true;
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

    static final UpdateProjectGroupCommand updateProjectGroupCommand = new UpdateProjectGroupCommand();

    public Collection<Option> getOptionsForUpdateProjectGroup() {
        return getOptions(updateProjectGroupCommand);
    }
    
    @CmdTrace(cmdClazz = UpdateProjectGroupCommand.class)
    public boolean updateProjectGroup(String ip, String project) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        String namespace = NamesrvUtil.NAMESPACE_PROJECT_CONFIG;
        try {
            defaultMQAdminExt.start();
            defaultMQAdminExt.createAndUpdateKvConfig(namespace, ip, project);
            return true;
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

    static final WipeWritePermSubCommand wipeWritePermSubCommand = new WipeWritePermSubCommand();
    
    public Collection<Option> getOptionsForWipeWritePerm() {
        return getOptions(wipeWritePermSubCommand);
    }
    
    @CmdTrace(cmdClazz = WipeWritePermSubCommand.class)
    public Table wipeWritePerm(String brokerName) throws Throwable {
        Throwable t = null;
        DefaultMQAdminExt defaultMQAdminExt = getDefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();
            List<String> namesrvList = defaultMQAdminExt.getNameServerAddressList();
            if (namesrvList != null) {
                List<Map<String, String>> result = new ArrayList<Map<String, String>>();
                for (String namesrvAddr : namesrvList) {
                    try {
                        int wipeTopicCount = defaultMQAdminExt.wipeWritePermOfBroker(namesrvAddr, brokerName);
                        Map<String, String> map = new HashMap<String, String>();
                        map.put("brokerName", brokerName);
                        map.put("namesrvAddr", namesrvAddr);
                        map.put("wipeTopicCount", String.valueOf(wipeTopicCount));
                        result.add(map);
                        // System.out.printf("wipe write perm of broker[%s] in name server[%s] OK, %d\n",//
                        // brokerName,//
                        // namesrvAddr,//
                        // wipeTopicCount//
                        // );
                    }
                    catch (Exception e) {
                        System.out.printf("wipe write perm of broker[%s] in name server[%s] Failed\n",//
                            brokerName,//
                            namesrvAddr//
                            );

                        logger.error(e.getMessage(), e);
                    }
                }
                Table table = Table.Maps2HTable(result);
                return table;
            }
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
