package com.sandeep.cache_node.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class VersionService {

    private static final String VERSION_KEY =
            "global-version";

    private final StringRedisTemplate redisTemplate;

    public VersionService(
            StringRedisTemplate redisTemplate) {

        this.redisTemplate = redisTemplate;
    }

    public long nextVersion() {

        Long version =
                redisTemplate.opsForValue()
                        .increment(VERSION_KEY);

        return version;
    }
}