package com.sandeep.cache_node.service;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class RecoveryService {

    private final CacheEventPublisher publisher;
    private final StringRedisTemplate redisTemplate;
    private boolean wasConnected = true;

    public RecoveryService(CacheEventPublisher publisher, StringRedisTemplate redisTemplate) {
        this.publisher = publisher;
        this.redisTemplate = redisTemplate;
    }
    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        System.out.println("[RECOVERY] Node started, broadcasting SYNC_REQUEST...");
        try {
            publisher.publishSyncRequest();
        } catch (Exception e) {
            System.err.println("[RECOVERY STARTUP ERR] Redis offline, will retry via monitor: " + e.getMessage());
            wasConnected = false;
        }
    }
    @Scheduled(fixedRate = 5000)
    public void checkConnection() {
        boolean currentlyConnected = false;
         try (var connection = redisTemplate.getConnectionFactory().getConnection()) {
           connection.ping();
           currentlyConnected = true;
        } catch (Exception e) {
           currentlyConnected = false;
        }

        if (currentlyConnected && !wasConnected) {
            System.out.println("[HEAL] Connection to Redis restored! Triggering SYNC_REQUEST...");
            try {
                publisher.publishSyncRequest();
                wasConnected = true;
            } catch (Exception e) {
                System.err.println("[HEAL ERR] Failed to publish sync request: " + e.getMessage());
            }
        } else if (!currentlyConnected && wasConnected) {
            System.err.println("[PARTITION DETECTED] Lost connection to Redis! Local hits will remain active.");
            wasConnected = false;
        }
    }
}