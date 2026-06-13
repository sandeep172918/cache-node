package com.sandeep.cache_node.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class BackingStoreService {

    private final StringRedisTemplate redisTemplate;

    public BackingStoreService(
            StringRedisTemplate redisTemplate){

        this.redisTemplate = redisTemplate;
    }

    public void save(
            String key,
            String value){

        redisTemplate.opsForHash()
                .put(
                    "cache-data",
                    key,
                    value
                );
    }

    public String load(
            String key){

        Object value =
                redisTemplate.opsForHash()
                .get(
                    "cache-data",
                    key
                );

        return value == null
                ? null
                : value.toString();
    }
}