package com.yucl.log.handle.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) {		
		ThreadPoolExecutor pool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors()*2, 10000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(10000));
			pool.setRejectedExecutionHandler((r, executor) -> {
                if (!executor.isShutdown()) {
                    logger.warn("executes task r in the caller's thread " + r.toString());
                    r.run();
                } else {
                    logger.warn("the executor has been shut down, the task is discarded " + r.toString());
                }

            });

		Properties props = new Properties();
		props.put("zookeeper.connect", args[0]);
		props.put("group.id", args[1]);
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
			
		LogConsumer acclogConsumer = new AccLogConsumer("acclog",pool,props);
		acclogConsumer.start();
		LogConsumer applogConsumer = new AppLogConsumer("applog",pool,props);
		applogConsumer.start();
		LogConsumer GCConsumer = new AppLogConsumer("gclog",pool,props);
		applogConsumer.start();
		LogConsumer containerlogConsumer = new ContainerLogConsumer("containerlog",pool,props);
		containerlogConsumer.start();
		LogConsumer syslogConsumer = new SysLogConsumer("hostsyslog",pool,props);
		syslogConsumer.start();

	}



}
