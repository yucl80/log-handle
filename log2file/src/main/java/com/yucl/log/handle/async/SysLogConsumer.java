
package com.yucl.log.handle.async;

import com.jayway.jsonpath.DocumentContext;

import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

public class SysLogConsumer extends LogConsumer {	
	
	public SysLogConsumer(String topic, ThreadPoolExecutor threadPoolExecutor,Properties props) {
		super(topic, threadPoolExecutor,  props);
	}



	@Override
	public String buildFilePathFromMsg(DocumentContext msgJsonContext, String rootDir) {
		String host = msgJsonContext.read("$.host", String.class);
		return rootDir + "/app/logs/hosts/"+host+msgJsonContext.read("$.path", String.class);
	}
}
