package com.pdn.apitest.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
    private String host;
    private int port;

    public RedisUtils(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Jedis getJedisConnect(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//        jedisPoolConfig.set

        JedisPool jedisPool = new JedisPool(jedisPoolConfig,host,port);

        return jedisPool.getResource();

    }
}
