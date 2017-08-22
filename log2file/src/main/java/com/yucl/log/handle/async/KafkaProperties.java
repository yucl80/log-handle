package com.yucl.log.handle.async;

public interface KafkaProperties
{
  final static String zkConnect = "10.62.14.53:2181,10.62.14.62:2181,10.62.14.64:2181";
  final static  String groupId = "log2filedev";
  final static int connectionTimeOut = 100000;
  final static int reconnectInterval = 10000;
}
