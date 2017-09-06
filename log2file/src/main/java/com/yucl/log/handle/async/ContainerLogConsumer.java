package com.yucl.log.handle.async;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

public class ContainerLogConsumer extends LogConsumer {
	
	public ContainerLogConsumer(String topic, ThreadPoolExecutor threadPoolExecutor,Properties props) {
		super(topic, threadPoolExecutor,props );
	}

	@Override
	public String buildFilePathFromMsg(DocumentContext msgJsonContext, String rootDir) {
        String filePath ;
        String date =null;
        String timestamp = msgJsonContext.read("$.@timestamp", String.class);
        if (!timestamp.isEmpty()) {
            Instant instant = Instant.parse(timestamp);
            ZonedDateTime localTime = instant.atZone(ZoneId.of("Asia/Shanghai"));
            date = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        } else {
            date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        }
	    if(! msgJsonContext.read("$.stack", String.class).isEmpty() ){
             filePath = new StringBuilder().append(rootDir).append("/app/logs/")
                    .append(msgJsonContext.read("$.stack", String.class)).append("/")
                    .append(msgJsonContext.read("$.service", String.class)).append("/")
                    .append(msgJsonContext.read("$.service", String.class)).append("-")
                    .append(msgJsonContext.read("$.index", String.class)).append(".")
                    .append("console.out").append(".")
                    .append(date)
                    .toString();
        } else {
	        String containerName = msgJsonContext.read("$.name", String.class);
            filePath = new StringBuilder().append(rootDir).append("/app/logs/")
                    .append("others/")
                    .append(msgJsonContext.read("$.host",String.class)).append("/")
                    .append(containerName)
                    .append(".").append(date)
                    .append(".out")
                    .toString();
        }
		return filePath;
	}

	@Override
	public byte[] getBytesToWrite(String rawMsg) {
		DocumentContext jsonContext = JsonPath.parse(rawMsg);
		String log = jsonContext.read("$.message",String.class);
		byte[] bytes = new byte[0];
		try {
			bytes =log.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {

		}
		return bytes;
	}
}
