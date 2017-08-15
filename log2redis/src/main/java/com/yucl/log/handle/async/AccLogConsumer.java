
package com.yucl.log.handle.async;

import com.jayway.jsonpath.DocumentContext;

import java.util.concurrent.ThreadPoolExecutor;

public class AccLogConsumer extends LogConsumer {


    public AccLogConsumer(String topic,String redisHost, int redisPort, ThreadPoolExecutor threadPoolExecutor) {
        super(topic,redisHost, redisPort, threadPoolExecutor);
    }

    @Override
    public String buildChannelFromMsg(DocumentContext msgJsonContext) {
        String rawPath = msgJsonContext.read("$.path", String.class);
        String channel = new StringBuilder()
                .append(msgJsonContext.read("$.stack", String.class)).append("/")
                .append(msgJsonContext.read("$.service", String.class)).toString();
        return channel;
    }

}
