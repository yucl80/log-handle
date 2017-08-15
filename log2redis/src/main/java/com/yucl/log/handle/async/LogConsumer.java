
package com.yucl.log.handle.async;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class LogConsumer extends Thread {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConsumerConnector consumer;
    private final String topic;
    private ThreadPoolExecutor threadPoolExecutor;
    private JedisPool jedisPool;



    public LogConsumer(String topic, String redisHost, int redisPort, ThreadPoolExecutor threadPoolExecutor) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        jedisPool = new JedisPool(config, redisHost,redisPort,100000);
        this.threadPoolExecutor = threadPoolExecutor;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", AppConf.zkConnect);
        props.put("group.id", AppConf.groupId);
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);

    }

    public abstract String buildChannelFromMsg(DocumentContext msgJsonContext);

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String msg = new String(it.next().message());
            if (logger.isDebugEnabled()) {
                logger.debug(msg);
            }
            Jedis jedis = jedisPool.getResource();
            try {
                DocumentContext jsonContext = JsonPath.parse(msg);
                byte[] bytes = getBytesToWrite(msg);
                byte[] channel = buildChannelFromMsg(jsonContext).getBytes();
                jedis.publish(channel, bytes);

            } catch (Throwable t) {
                logger.error(msg, t);
            } finally {
                 jedis.close();
            }
        }
    }

    public byte[] getBytesToWrite(String msg) {
        return (msg + "\n").getBytes();
    }
}
