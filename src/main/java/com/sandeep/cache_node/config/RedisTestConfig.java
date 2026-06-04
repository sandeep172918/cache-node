package com.sandeep.cache_node.config;

import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class RedisTestConfig implements CommandLineRunner {

    private final StringRedisTemplate redisTemplate;

    public RedisTestConfig(
            StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run(String... args) {

        redisTemplate.opsForValue()
                .set("test", "hello");

        String value =
                redisTemplate.opsForValue()
                        .get("test");

        System.out.println(
                "REDIS TEST => " + value
        );
    }
}