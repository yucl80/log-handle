package com.yucl.demo;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;

import java.io.UnsupportedEncodingException;

public class RedisSub {

    public static void main(String[] args){
        Jedis jedis = new Jedis("10.62.14.40",6379);

        jedis.subscribe(new BinaryJedisPubSub() {

            @Override
            public void onMessage(byte[] channel, byte[] message) {
                try {
                    System.out.println(new String(message,"UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }


        },"syslog".getBytes());


    }
}
