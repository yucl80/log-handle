package com.yucl.log.handle.async;

import com.jayway.jsonpath.DocumentContext;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AppLogConsumer extends LogConsumer {
    Pattern pattern = Pattern.compile("^\\[\\d{2}/\\d{2} ");
    Pattern fullDatePattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");

	public AppLogConsumer(String topic, ThreadPoolExecutor threadPoolExecutor, Properties props) {
		super(topic, threadPoolExecutor,props );
	}


	@Override
	public String buildFilePathFromMsg(DocumentContext msgJsonContext, String rootDir) {
		String rawMsg = msgJsonContext.read("$.message", String.class);
		String date =null;
		String timestamp = msgJsonContext.read("$.@timestamp", String.class);
		if (!timestamp.isEmpty()) {
			Instant instant = Instant.parse(timestamp);
			OffsetDateTime localTime = instant.atOffset(ZoneOffset.UTC);
			date = localTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		}else {
            Matcher matcher = fullDatePattern.matcher(rawMsg);
            if(matcher.find()){
                date = matcher.group(1);
            } else if (pattern.matcher(rawMsg).find()){
                String year = String.valueOf(Calendar.getInstance().get(Calendar.YEAR));
                String eventTime = year+"-"+rawMsg.substring(1,6);
                date = eventTime.replaceAll("/","-");
            }
        }
		String rawPath = msgJsonContext.read("$.path", String.class);
        String fileName = rawPath.substring(rawPath.lastIndexOf('/') + 1);
        if(!fullDatePattern.matcher(fileName).find() && date!=null){
            fileName = fileName + "."+date ;
        }
        String filePath = new StringBuilder().append(rootDir).append("/app/logs/")
				.append(msgJsonContext.read("$.stack", String.class)).append("/")
				.append(msgJsonContext.read("$.service", String.class)).append("/")
				.append(msgJsonContext.read("$.service", String.class)).append("-")
				.append(msgJsonContext.read("$.index", String.class)).append(".")
				.append(fileName).toString();
		return filePath;
	}
}
