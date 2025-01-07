package com.xxl.job.executor.sample.frameless.test;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.xxl.job.core.context.XxlJobContext;

/**
 * @author hst
 * @create 2024-12-16 1:24
 * @Description:
 */
public class XxlJobContextTest {

    public static void main(String[] args) {


        XxlJobContext xxlJobContext = new XxlJobContext(
                1L, "", "", 1, 2);
        XxlJobContext.setXxlJobContext(xxlJobContext);


        Thread futureThread = null;
        FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                System.out.println(XxlJobContext.getXxlJobContext().getJobId());
                return true;
            }
        });
        futureThread = new Thread(futureTask);
        futureThread.start();

        // {}
        System.out.println(XxlJobContext.getXxlJobContext().getJobId());

    }

}
