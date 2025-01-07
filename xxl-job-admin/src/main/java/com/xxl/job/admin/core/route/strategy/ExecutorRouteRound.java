package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xuxueli on 17/3/10.
 */
public class ExecutorRouteRound extends ExecutorRouter {

    private static ConcurrentMap<Integer, AtomicInteger> routeCountEachJob = new ConcurrentHashMap<>();
    private static long CACHE_VALID_TIME = 0;

    // 为每个 jobId 计算并返回一个递增的计数值。
    // 具体步骤如下：
    //      1、缓存清理：
    //          检查当前时间是否超过了缓存的有效时间（24小时）。
    //          如果是，则清空 routeCountEachJob 缓存，
    //          并更新 CACHE_VALID_TIME。
    //      2、获取计数器：
    //          从 routeCountEachJob 中获取与 jobId 对应的 AtomicInteger 计数器。
    //      3、初始化计数器：
    //          如果计数器不存在或超过 1000000，
    //          则重新初始化为一个随机值（0 到 99 之间）。
    //      4、递增计数器：
    //          如果计数器存在且未超过 1000000，
    //          则递增计数器。
    //      5、更新缓存：
    //          将更新后的计数器放回 routeCountEachJob。
    //      6、返回计数值：
    //          返回当前计数器的值。
    private static int count(int jobId) {
        // cache clear
        if (System.currentTimeMillis() > CACHE_VALID_TIME) {
            routeCountEachJob.clear();
            CACHE_VALID_TIME = System.currentTimeMillis() + 1000*60*60*24;
        }

        AtomicInteger count = routeCountEachJob.get(jobId);
        if (count == null || count.get() > 1000000) {
            // 初始化时主动Random一次，缓解首次压力
            count = new AtomicInteger(new Random().nextInt(100));
        } else {
            // count++
            count.addAndGet(1);
        }
        routeCountEachJob.put(jobId, count);
        return count.get();
    }

    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        String address = addressList.get(count(triggerParam.getJobId())%addressList.size());
        return new ReturnT<String>(address);
    }

}
