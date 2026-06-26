package com.sandeep.cache_node.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sandeep.cache_node.model.CacheInvalidationEvent;
import com.sandeep.cache_node.model.EventType;

@Service
public class CacheEventSubscriber {

    private final CacheService cacheService;
    private final CacheEventPublisher publisher; // added

@Value("${node.id}")
private String nodeId;

private final ObjectMapper mapper = new ObjectMapper();


public CacheEventSubscriber(
        CacheService cacheService,
        CacheEventPublisher publisher) {

    this.cacheService = cacheService;
    this.publisher = publisher;
}

public void receiveMessage(String message) {

        try {

            CacheInvalidationEvent event =
                    mapper.readValue(
                            message,
                            CacheInvalidationEvent.class
                    );
            if (event.getSenderId()
                    .equals(nodeId)) {

                System.out.println(
                        "[IGNORE OWN EVENT] "
                                + event.getType()
                                + " key="
                                + event.getKey()
                );

                return;
            }

            EventType type = event.getType();

            if (type == null) {
                System.out.println(
                    "[LEGACY EVENT] key="
                    + event.getKey()
                    + " — treating as WRITE_REQUEST"
                );

                cacheService.handleRemoteWriteRequest(
                    event.getKey(),
                    event.getVersion()
                );
                return;
            }

            switch (type) {

                case WRITE_REQUEST:
                    System.out.println(
                        "[RECV WRITE_REQUEST from "
                        + event.getSenderId()
                        + "] key=" + event.getKey()
                        + " version=" + event.getVersion()
                    );

                    cacheService.handleRemoteWriteRequest(
                        event.getKey(),
                        event.getVersion()
                    );
                    break;

                case READ_REQUEST:
                    System.out.println(
                        "[RECV READ_REQUEST from "
                        + event.getSenderId()
                        + "] key=" + event.getKey()
                    );

                    cacheService.handleRemoteReadRequest(
                        event.getKey(),
                        event.getSenderId()
                    );
                    break;

                case DATA_RESPONSE:
                    System.out.println(
                        "[RECV DATA_RESPONSE from "
                        + event.getSenderId()
                        + "] key=" + event.getKey()
                        + " version=" + event.getVersion()
                    );

                    cacheService.handleDataResponse(
                        event.getKey(),
                        event.getValue(),
                        event.getVersion()
                    );
                    break;

                case INVALIDATE:
                    System.out.println(
                        "[RECV INVALIDATE from "
                        + event.getSenderId()
                        + "] key=" + event.getKey()
                        + " version=" + event.getVersion()
                    );

                    cacheService.handleRemoteWriteRequest(
                        event.getKey(),
                        event.getVersion()
                    );
                    break;
                case SYNC_REQUEST:
                    System.out.println("[RECV SYNC_REQUEST from " + event.getSenderId() + "]");
                    Map<String, Long> activeVersions = cacheService.getActiveVersions();
                    publisher.publishSyncResponse(activeVersions);
                    break;

               case SYNC_RESPONSE:
                   System.out.println("[RECV SYNC_RESPONSE from " + event.getSenderId() + "]");
                    try {
                       Map<String, Object> rawMap = mapper.readValue(event.getValue(), Map.class);
                       Map<String, Long> remoteVersions = new HashMap<>();
                       rawMap.forEach((k, v) -> {
                             if (v instanceof Number num) {
                             remoteVersions.put(k, num.longValue());
                              }
                        });
                    cacheService.handleSyncResponse(remoteVersions);
                   } catch (Exception e) {
                      System.err.println("[SUBSCRIBER SYNC RECONCILE ERR] " + e.getMessage());
                   }
                    break;
 
            }

        } catch (Exception e) {
            System.err.println(
                "[SUBSCRIBER ERROR] " + e.getMessage()
            );
            throw new RuntimeException(e);
        }
    }
}
