package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 *
 * @author xuxueli 2018-07-03 21:08:07
 *
 * 管理任务触发线程池
 *
 * 为什么要创建快慢两个线程池？
 * 线程池隔离–快慢分离
 *      目的：部分慢执行的线程，会拖慢整个线程池，因此我们需要将快慢分离。
 *      需要区分出哪些是慢线程，这里给一个依据是一分钟内的慢执行（耗时大于500ms）次数为10次。
 *
 * 哪些情况下会进入慢线程池
 *  1.任务执行频率高每分钟执行次数要大于10次以上
 *  (jobTimeoutCountMap，存放每个任务的执行慢次数，60秒后自动清空该容器)
 *  2.job Client端执行任务耗时较长，形成堆积 或者 网络延迟卡顿严重
 *  3.数据库相关查询出现耗时严重，遇到cpu100%或者拿不到连接等瓶颈问题
 */
public class JobTriggerPoolHelper {
    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


    // ---------------------- trigger pool ----------------------

    // fast/slow thread pool
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    // 初始化两个线程池
    public void start(){
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
                    }
                });

        slowTriggerPool = new ThreadPoolExecutor(
                10,
                XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
                    }
                });
    }


    // 停止线程池
    public void stop() {
        //triggerPool.shutdown();
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
    }


    // job timeout count
    // 当前时间分钟数
    private volatile long minTim = System.currentTimeMillis()/60000;     // ms > min
    // 线程安全的map，存放每个任务的执行慢次数
    private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();


    /**
     * add trigger
     *
     * 触发流程到这里开始出现多线程，因为使用了线程池进行触发
     * 所有触发的任务共同使用线程池中的线程进行触发
     */
    public void addTrigger(final int jobId,
                           final TriggerTypeEnum triggerType,
                           final int failRetryCount,
                           final String executorShardingParam,
                           final String executorParam,
                           final String addressList) {

        // choose thread pool
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount!=null && jobTimeoutCount.get() > 10) {      // job-timeout 10 times in 1 min
            triggerPool_ = slowTriggerPool;
        }

        // trigger
        // 在选中的线程池执行任务
        // 使用线程池中的线程执行任务
        triggerPool_.execute(new Runnable() {
            @Override
            public void run() {

                // 记录任务的开始时间start
                long start = System.currentTimeMillis();

                try {
                    // do trigger
                    XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {

                    // check timeout-count-map
                    long minTim_now = System.currentTimeMillis()/60000;
                    if (minTim != minTim_now) {
                        minTim = minTim_now;
                        jobTimeoutCountMap.clear();
                    }

                    // incr timeout-count-map
                    long cost = System.currentTimeMillis()-start;
                    if (cost > 500) {       // ob-timeout threshold 500ms
                        AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                        if (timeoutCount != null) {
                            timeoutCount.incrementAndGet();
                        }
                    }

                }

            }
        });
    }



    // ---------------------- helper ----------------------

    private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();

    public static void toStart() {
        helper.start();
    }
    public static void toStop() {
        helper.stop();
    }

    /**
     * 触发一个任务
     * @param jobId 任务id
     * @param triggerType 触发类型
     * @param failRetryCount 失败重试次数
     * 			>=0: use this param
     * 			<0: use param from job info config
     * @param executorShardingParam 执行器分片参数
     * @param executorParam 执行器参数，为null时使用任务参数，不为null时覆盖任务参数。
     *          null: use job param
     *          not null: cover job param
     * @param addressList 执行器地址列表，多地址逗号分隔（只对job_group=0生效）
     */
    public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
        helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

}
