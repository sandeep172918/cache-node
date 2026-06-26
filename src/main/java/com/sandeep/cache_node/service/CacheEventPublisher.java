package com.sandeep.cache_node.service;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sandeep.cache_node.model.CacheInvalidationEvent;
import com.sandeep.cache_node.model.EventType;

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
    public void publishWriteRequest(
            String key,
            long version) {

        CacheInvalidationEvent event =
                new CacheInvalidationEvent(
                        EventType.WRITE_REQUEST,
                        key,
                        nodeId,
                        version,
                        null
                );

        publish(event);
    }

    public void publishReadRequest(String key) {

        CacheInvalidationEvent event =
                new CacheInvalidationEvent(
                        EventType.READ_REQUEST,
                        key,
                        nodeId,
                        0,
                        null
                );

        publish(event);
    }
    public void publishDataResponse(
            String key,
            String value,
            long version) {

        CacheInvalidationEvent event =
                new CacheInvalidationEvent(
                        EventType.DATA_RESPONSE,
                        key,
                        nodeId,
                        version,
                        value
                );

        publish(event);
    }

    private void publish(CacheInvalidationEvent event) {

        try {

            String json =
                    mapper.writeValueAsString(event);

            System.out.println(
                    "[PUBLISH " + event.getType()
                            + "] " + json
            );

            redisTemplate.convertAndSend(
                    CHANNEL,
                    json
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void publishSyncRequest() {
    publish(new CacheInvalidationEvent(EventType.SYNC_REQUEST, null, nodeId, 0, null));
}

   public void publishSyncResponse(Map<String, Long> activeVersions) {
    try {
        String jsonMap = mapper.writeValueAsString(activeVersions);
        publish(new CacheInvalidationEvent(EventType.SYNC_RESPONSE, null, nodeId, 0, jsonMap));
    } catch (Exception e) {
        System.err.println("[PUBLISH SYNC ERR] " + e.getMessage());
    }
}
}