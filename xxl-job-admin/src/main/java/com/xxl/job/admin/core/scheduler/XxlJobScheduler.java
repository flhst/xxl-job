package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.thread.*;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.client.ExecutorBizClient;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xuxueli 2018-10-28 00:18:17
 */

public class XxlJobScheduler  {
    private static final Logger logger = LoggerFactory.getLogger(XxlJobScheduler.class);


    // 初始化XXL-JOB管理端的各项服务
    // 1、初始化国际化配置：
    //      调用 initI18n 方法，遍历 ExecutorBlockStrategyEnum 枚举值，
    //      设置每个枚举值的标题为对应的国际化字符串。
    // 2、启动触发池：
    //      调用 JobTriggerPoolHelper.toStart() 方法，
    //      启动触发池，用于任务的触发和执行。
    // 3、启动注册监控：
    //      调用 JobRegistryHelper.getInstance().start() 方法，
    //      启动注册监控，监控执行器的注册状态。
    // 4、启动失败监控：
    //      调用 JobFailMonitorHelper.getInstance().start() 方法，
    //      启动失败监控，监控任务的失败情况。
    // 5、启动丢失监控：
    //      调用 JobCompleteHelper.getInstance().start() 方法，
    //      启动丢失监控，依赖于触发池，监控任务的丢失情况。
    // 6、启动日志报告：
    //      调用 JobLogReportHelper.getInstance().start() 方法，
    //      启动日志报告，生成任务执行的日志报告。
    // 7、启动调度：
    //      调用 JobScheduleHelper.getInstance().start() 方法，
    //      启动调度，依赖于触发池，进行任务的调度。
    // 8、记录初始化成功日志：
    //      记录一条日志信息，表示初始化成功。
    //      通过以上步骤，XxlJobScheduler 类的 init 方法确保了
    //      XXL-JOB 管理端的各项服务能够正常启动并运行。
    public void init() throws Exception {
        // init i18n
        initI18n();

        // admin trigger pool start
        // 初始化任务触发线程池 用于任务的trigger
        JobTriggerPoolHelper.toStart();

        // admin registry monitor run
        // 启动注册监控
        JobRegistryHelper.getInstance().start();

        // admin fail-monitor run
        JobFailMonitorHelper.getInstance().start();

        // admin lose-monitor run ( depend on JobTriggerPoolHelper )
        JobCompleteHelper.getInstance().start();

        // admin log report start
        JobLogReportHelper.getInstance().start();

        // start-schedule  ( depend on JobTriggerPoolHelper )
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init xxl-job admin success.");
    }

    
    public void destroy() throws Exception {

        // stop-schedule
        JobScheduleHelper.getInstance().toStop();

        // admin log report stop
        JobLogReportHelper.getInstance().toStop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().toStop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().toStop();

        // admin registry stop
        JobRegistryHelper.getInstance().toStop();

        // admin trigger pool stop
        JobTriggerPoolHelper.toStop();

    }

    // ---------------------- I18n ----------------------

    private void initI18n(){
        for (ExecutorBlockStrategyEnum item:ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    // 一个地址一个ExecutorBiz
    private static ConcurrentMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<String, ExecutorBiz>();

    // 获取执行器Biz客户端
    public static ExecutorBiz getExecutorBiz(String address) throws Exception {
        // valid
        if (address==null || address.trim().length()==0) {
            return null;
        }

        // load-cache
        address = address.trim();
        ExecutorBiz executorBiz = executorBizRepository.get(address);
        if (executorBiz != null) {
            return executorBiz;
        }

        // set-cache
        executorBiz = new ExecutorBizClient(address, XxlJobAdminConfig.getAdminConfig().getAccessToken());

        executorBizRepository.put(address, executorBiz);
        return executorBiz;
    }

}
