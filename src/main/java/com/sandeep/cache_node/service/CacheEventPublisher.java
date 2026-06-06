package com.sandeep.cache_node.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class CacheEventPublisher {

    private static final String CHANNEL =
            "cache-events";

    private final StringRedisTemplate redisTemplate;

    public CacheEventPublisher(
            StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void publish(String key) {

        System.out.println(
                "[publish] " + key
        );

        redisTemplate.convertAndSend(
                CHANNEL,
                key
        );
    }
}