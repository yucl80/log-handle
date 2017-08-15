
package com.yucl.log.handle.async;

import com.jayway.jsonpath.DocumentContext;

import java.util.concurrent.ThreadPoolExecutor;

public class SysLogConsumer extends LogConsumer {	
	
	public SysLogConsumer(String topic,  String redisHost, int redisPort, ThreadPoolExecutor threadPoolExecutor) {
		super(topic,redisHost,redisPort , threadPoolExecutor);
	}

	@Override
	public String buildChannelFromMsg(DocumentContext msgJsonContext) {
		return  "syslog";
	}
}
