package com.xxl.job.core.thread;

import com.xxl.job.core.biz.model.HandleCallbackParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;


/**
 * handler thread
 * @author xuxueli 2016-1-16 19:52:47
 *
 * 执行任务线程
 *
 * 为什么下面的这些成员变量有些需要使用
 * volatile关键字（toStop）和
 * LinkedBlockingQueue（synchronized同步机制），有些不需要？
 *
 * private volatile boolean toStop = false;
 * 		作用：确保多个线程之间的可见性。当一个线程修改了 toStop 的值时，
 * 		其他线程可以立即看到这个变化。
 *
 * 不需要 volatile 或 synchronized 的变量：
 * 		适用于那些在对象创建后不会改变的变量
 */
public class JobThread extends Thread{
	private static Logger logger = LoggerFactory.getLogger(JobThread.class);

	private int jobId;
	private IJobHandler handler;
	// 基于链表的线程安全阻塞队列，支持高并发场景下的数据交换。
	// 基于阻塞队列实现的线程安全，用于存储待处理的任务。
	// 但是在使用的时候需要注意不要破坏线程安全（针对jdk8），具体可以点进去这个类，看看源码。
	private LinkedBlockingQueue<TriggerParam> triggerQueue;
	// 用于存储已经处理过的 TRIGGER_LOG_ID，避免重复处理。
	// Collections.synchronizedSet(new HashSet<Long>());
	// 使用 Collections.synchronizedSet 方法将一个普通的 HashSet 转换为一个线程安全的 Set。
	private Set<Long> triggerLogIdSet;		// avoid repeat trigger for the same TRIGGER_LOG_ID

	private volatile boolean toStop = false;
	private String stopReason;

    private boolean running = false;    // if running job
	// 记录空闲次数
	private int idleTimes = 0;			// idel times


	public JobThread(int jobId, IJobHandler handler) {
		this.jobId = jobId;
		this.handler = handler;
		this.triggerQueue = new LinkedBlockingQueue<TriggerParam>();
		this.triggerLogIdSet = Collections.synchronizedSet(new HashSet<Long>());

		// assign job thread name
		this.setName("xxl-job, JobThread-"+jobId+"-"+System.currentTimeMillis());
	}
	public IJobHandler getHandler() {
		return handler;
	}

    /**
     * kill job thread
     *
     * @param stopReason
     */
	public void toStop(String stopReason) {
		/**
		 * Thread.interrupt只支持终止线程的阻塞状态(wait、join、sleep)，
		 * 在阻塞出抛出InterruptedException异常,但是并不会终止运行的线程本身；
		 * 所以需要注意，此处彻底销毁本线程，需要通过共享变量方式；
		 */
		this.toStop = true;
		this.stopReason = stopReason;
	}

	/**
	 * new trigger to queue
	 *
	 * 将 TriggerParam 对象添加到任务队列中
	 * 1、避免重复触发：
	 * 		检查 triggerLogIdSet 是否已包含 triggerParam.getLogId()，
	 * 		如果包含则记录日志并返回失败结果。
	 * 2、添加触发参数：
	 * 		如果 triggerParam.getLogId() 不在 triggerLogIdSet 中，
	 * 		则将其添加到 triggerLogIdSet 和 triggerQueue 中，并返回成功结果。
	 *
	 * @param triggerParam
	 * @return
	 */
	public ReturnT<String> pushTriggerQueue(TriggerParam triggerParam) {
		// avoid repeat
		if (triggerLogIdSet.contains(triggerParam.getLogId())) {
			logger.info(">>>>>>>>>>> repeate trigger job, logId:{}", triggerParam.getLogId());
			return new ReturnT<String>(ReturnT.FAIL_CODE, "repeate trigger job, logId:" + triggerParam.getLogId());
		}

		triggerLogIdSet.add(triggerParam.getLogId());
		triggerQueue.add(triggerParam);
		return ReturnT.SUCCESS;
	}

    /**
     * is running job
     * @return
     */
    public boolean isRunningOrHasQueue() {
        return running || triggerQueue.size()>0;
    }

    @Override
	public void run() {

    	// init
    	try {
			handler.init();
		} catch (Throwable e) {
    		logger.error(e.getMessage(), e);
		}

		// execute
		while(!toStop){
			running = false;
			idleTimes++;

            TriggerParam triggerParam = null;
            try {
				// to check toStop signal, we need cycle, so wo cannot use queue.take(), instand of poll(timeout)
				// 从 triggerQueue 队列中尝试获取一个 TriggerParam 对象，
				// 等待时间最长为 3 秒。如果在 3 秒内没有获取到对象，
				// 则返回 null。
				triggerParam = triggerQueue.poll(3L, TimeUnit.SECONDS);
				if (triggerParam!=null) {
					running = true;
					idleTimes = 0;
					triggerLogIdSet.remove(triggerParam.getLogId());

					// log filename, like "logPath/yyyy-MM-dd/9999.log"
					String logFileName = XxlJobFileAppender.makeLogFileName(new Date(triggerParam.getLogDateTime()), triggerParam.getLogId());
					XxlJobContext xxlJobContext = new XxlJobContext(
							triggerParam.getJobId(),
							triggerParam.getExecutorParams(),
							logFileName,
							triggerParam.getBroadcastIndex(),
							triggerParam.getBroadcastTotal());

					// init job context
					XxlJobContext.setXxlJobContext(xxlJobContext);

					// execute
					XxlJobHelper.log("<br>----------- xxl-job job execute start -----------<br>----------- Param:" + xxlJobContext.getJobParam());

					// 超时处理：
					//		当 triggerParam.getExecutorTimeout() 大于 0 时，
					//		任务会在一个单独的线程中执行，并且会有一个超时限制。
					//		如果任务在指定时间内未完成，会捕获 TimeoutException 并处理超时情况。
					//		这种方式适用于需要严格控制任务执行时间的场景，
					//		确保任务不会无限期地运行，从而影响系统的稳定性和性能。
					// 直接执行：
					//		当 triggerParam.getExecutorTimeout() 小于等于 0 时，
					//		任务会直接在当前线程中执行，没有超时限制。
					//		这种方式适用于不需要严格控制任务执行时间的场景，
					//		任务可以自由地运行直到完成。
					//
					// 超时后为什么要创建一个新的线程？并设置执行超时事件
					//
					//		因为任务超时有可能是下面问题导致的：
					//			1、是该任务执行比较耗时，如果直接在当前线程中执行，可能会导致主线程阻塞，影响系统的吞吐量。
					//      	2、该任务执行失败、异常，会导致任务无法被后续处理。
					//
					//      如果使用同步执行，可能会导致主线程阻塞或者一直阻塞，影响系统的吞吐量和响应速度。
					//      而使用新启动的线程并且设置任务超时时间后中断可以避免上面的问题。
					//
					//		新启动一个线程好处：
					// 		异步执行：
					//			创建新的线程可以实现任务的异步执行，这样主线程不会被阻塞，可以继续处理其他任务或逻辑。
					//			异步执行提高了系统的并发能力和响应速度。
					//		超时控制：
					//			使用 FutureTask 和 Future 接口可以方便地设置任务的超时时间。
					//			futureTask.get(triggerParam.getExecutorTimeout(), TimeUnit.SECONDS)
					//			会阻塞主线程，直到任务完成或超时。
					//			如果任务在指定时间内未完成，会抛出 TimeoutException，从而可以捕获并处理超时情况。
					//		资源管理：
					//			通过创建新的线程，可以更好地管理任务的生命周期。
					//			在超时后，可以通过调用 futureThread.interrupt() 中断任务线程，释放系统资源。
					//		避免阻塞：
					//			如果不创建新的线程，而是在主线程中直接执行任务，那么主线程会被阻塞，无法处理其他任务或逻辑。
					//			创建新的线程可以确保主线程的流畅运行，提高系统的整体性能。
					if (triggerParam.getExecutorTimeout() > 0) {
						// limit timeout
						Thread futureThread = null;
						try {
							FutureTask<Boolean> futureTask = new FutureTask<Boolean>(new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {

									// init job context
									// 为什么上面主线程设置完XxlJobContext后再次设置
									// 		因为handler.execute();这个方法中可能需要用到XxlJobContext
									//      ScriptJobHandler的execute()方法中会用到
									// 		而且XxlJobContext是属于线程的，不用加锁
									//
									// （重要）不要想错误了，
									// valid execute handle data
									// if (XxlJobContext.getXxlJobContext().getHandleCode() <= 0) {
									// 这里用到的XxlJobContext.getXxlJobContext().getHandleCode()是主线程的
									// 子线程futureTask不会执行这段代码，或者说不用执行，
									// futureTask只需要调用需要定时执行的方法就行了，后面的流程统一交给主线程处理。
									XxlJobContext.setXxlJobContext(xxlJobContext);

									handler.execute();
									return true;
								}
							});
							futureThread = new Thread(futureTask);
							futureThread.start();

							Boolean tempResult = futureTask.get(triggerParam.getExecutorTimeout(), TimeUnit.SECONDS);
						} catch (TimeoutException e) {

							XxlJobHelper.log("<br>----------- xxl-job job execute timeout");
							XxlJobHelper.log(e);

							// handle result
							XxlJobHelper.handleTimeout("job execute timeout ");
						} finally {
							futureThread.interrupt();
						}
					} else {
						// just execute
						handler.execute();
					}

					// valid execute handle data
					// 检查执行结果，记录日志
					if (XxlJobContext.getXxlJobContext().getHandleCode() <= 0) {
						XxlJobHelper.handleFail("job handle result lost.");
					} else {
						String tempHandleMsg = XxlJobContext.getXxlJobContext().getHandleMsg();
						tempHandleMsg = (tempHandleMsg!=null&&tempHandleMsg.length()>50000)
								?tempHandleMsg.substring(0, 50000).concat("...")
								:tempHandleMsg;
						XxlJobContext.getXxlJobContext().setHandleMsg(tempHandleMsg);
					}
					XxlJobHelper.log("<br>----------- xxl-job job execute end(finish) -----------<br>----------- Result: handleCode="
							+ XxlJobContext.getXxlJobContext().getHandleCode()
							+ ", handleMsg = "
							+ XxlJobContext.getXxlJobContext().getHandleMsg()
					);

				} else {
					// 空闲次数大于30次且触发队列为空的时候
					// XxlJobExecutor.removeJobThread 方法移除当前 JobThread
					// 原因：
					// 		1、资源管理：长时间空闲的线程会占用系统资源，包括内存和CPU时间。通过移除长时间空闲的线程，可以释放这些资源，提高系统的整体性能和资源利用率。
					//		2、避免僵尸线程：如果一个线程长时间没有任务可执行，它可能会变成“僵尸线程”，即不再有用但仍然占用资源的线程。移除这些线程可以防止系统中积累大量无用的线程，影响系统的稳定性和性能。
					//		3、动态调整线程池大小：通过动态移除空闲线程，可以根据实际任务负载动态调整线程池的大小。当任务量减少时，减少线程数；当任务量增加时，可以重新创建新的线程。这种动态调整有助于优化系统资源的使用。
					//
					if (idleTimes > 30) {
						if(triggerQueue.size() == 0) {	// avoid concurrent trigger causes jobId-lost
							XxlJobExecutor.removeJobThread(jobId, "excutor idel times over limit.");
						}
					}
				}
			} catch (Throwable e) {
				if (toStop) {
					XxlJobHelper.log("<br>----------- JobThread toStop, stopReason:" + stopReason);
				}

				// handle result
				StringWriter stringWriter = new StringWriter();
				e.printStackTrace(new PrintWriter(stringWriter));
				String errorMsg = stringWriter.toString();

				XxlJobHelper.handleFail(errorMsg);

				XxlJobHelper.log("<br>----------- JobThread Exception:" + errorMsg + "<br>----------- xxl-job job execute end(error) -----------");
			} finally {
                if(triggerParam != null) {
                    // callback handler info
                    if (!toStop) {
                        // commonm
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
                        		triggerParam.getLogId(),
								triggerParam.getLogDateTime(),
								XxlJobContext.getXxlJobContext().getHandleCode(),
								XxlJobContext.getXxlJobContext().getHandleMsg() )
						);
                    } else {
                        // is killed
                        TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
                        		triggerParam.getLogId(),
								triggerParam.getLogDateTime(),
								XxlJobContext.HANDLE_CODE_FAIL,
								stopReason + " [job running, killed]" )
						);
                    }
                }
            }
        }

		// callback trigger request in queue
		// 执行器在接收到任务执行请求后，执行任务，在任务执行结束之后会将执行结果回调通知"调度中心"
		//  JobThread 停止时，处理 triggerQueue 中剩余的 TriggerParam 对象
		// 在队列中存在未处理的任务时，通过回调线程将执行结果通知"调度中心"。
		// 也可以看出上面使用while(!toStop)控制的好处
		while(triggerQueue !=null && triggerQueue.size()>0){
			TriggerParam triggerParam = triggerQueue.poll();
			if (triggerParam!=null) {
				// is killed
				TriggerCallbackThread.pushCallBack(new HandleCallbackParam(
						triggerParam.getLogId(),
						triggerParam.getLogDateTime(),
						XxlJobContext.HANDLE_CODE_FAIL,
						stopReason + " [job not executed, in the job queue, killed.]")
				);
			}
		}

		// destroy
		try {
			handler.destroy();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}

		logger.info(">>>>>>>>>>> xxl-job JobThread stoped, hashCode:{}", Thread.currentThread());
	}
}
