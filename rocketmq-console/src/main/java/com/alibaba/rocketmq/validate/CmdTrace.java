package com.alibaba.rocketmq.validate;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 
 * @author yankai913@gmail.com
 * @date 2014-2-25
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CmdTrace {
    Class<? extends SubCommand> cmdClazz();
}
