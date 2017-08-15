package com.yucl.log.handle.async;

import com.jayway.jsonpath.DocumentContext;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ThreadPoolExecutor;

public class ContainerLogConsumer extends LogConsumer {
	
	public ContainerLogConsumer(String topic, String redisHost,int redisPort, ThreadPoolExecutor threadPoolExecutor) {
		super(topic,redisHost,redisPort , threadPoolExecutor);
	}



	@Override
	public String buildChannelFromMsg(DocumentContext msgJsonContext) {
		String channel = new StringBuilder()
				.append(msgJsonContext.read("$.stack", String.class)).append("/")
		        .append(msgJsonContext.read("$.service", String.class))
		        .toString();
		return channel;
	}

	@Override
	public byte[] getBytesToWrite(String msg) {
		/*DocumentContext jsonContext = JsonPath.parse(rawMsg);
		String log = jsonContext.read("$.log",String.class);*/
		byte[] bytes = new byte[0];
		try {
			bytes =msg.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {

		}
		return bytes;
	}
}
