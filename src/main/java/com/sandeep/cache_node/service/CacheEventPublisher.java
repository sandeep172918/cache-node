package com.sandeep.cache_node.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sandeep.cache_node.model.CacheInvalidationEvent;

@Service
public class CacheEventPublisher {

    private static final String CHANNEL =
            "cache-events";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper =
            new ObjectMapper();

    @Value("${node.id}")
    private String nodeId;

    public CacheEventPublisher(
            StringRedisTemplate redisTemplate) {

        this.redisTemplate = redisTemplate;
    }

    public void publish(String key,long version) {

        try {

           CacheInvalidationEvent event =
            new CacheInvalidationEvent(
                key,
                nodeId,
                version
         );

            String json =
                    mapper.writeValueAsString(
                            event
                    );

            System.out.println(
                    "[PUBLISH] " + json
            );

            redisTemplate.convertAndSend(
                    CHANNEL,
                    json
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}