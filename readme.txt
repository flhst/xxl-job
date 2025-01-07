https://www.cnblogs.com/zzyang/p/17876807.html

xxl-job-admin 调度中心 ：
    主要就是从界面添加用户、执行器(可以理解为一个任务组)、
    任务等都是服务写入到数据库的。

    XXL-JOB调度模块默认采用并行机制，
    在多线程调度的情况下，调度模块被阻塞的几率很低，
    大大提高了调度系统的承载量。
xxl-job-executor-samples 执行器

XxlJob注册及发现原理
https://cloud.tencent.com/developer/article/2364105

线程池的使用：
    ThreadPoolExecutor 线程池
        使用到的地方：
            JobTriggerPoolHelper 管理任务触发线程池 主要包括快线程池和慢线程池：
                            addTrigger // 用于触发任务，使用线程池进行任务的触发（调用执行器的run方法），任务触发流程到这里需要开始使用多线程

            EmbedServer 主要用来接收调度中心的调用:
                EmbedHttpServerHandler -->
                    channelRead0 // 由线程池进行异步处理，防止阻塞IO -->
                        process // 委托给了ExecutorBiz





先看下官网的总体设计和优点

总体流程，哪里需要加锁

xxl-job 阻塞策略的应用

xxl-job 分片、分布式部署

xxl-job 线程的使用

任务超时（executorTimeout）的原因，需要的处理
1、JobThread.run()方法处理



任务触发：
    https://www.cnblogs.com/wanghongsen/p/12510533.html

