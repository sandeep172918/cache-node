package com.sandeep.cache_node.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sandeep.cache_node.model.CacheInvalidationEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CacheEventSubscriber {

    private final CacheService cacheService;

    @Value("${node.id}")
    private String nodeId;

    private final ObjectMapper mapper =
            new ObjectMapper();

    public CacheEventSubscriber(
            CacheService cacheService) {

        this.cacheService = cacheService;
    }

    public void receiveMessage(String message) {

        try {

            CacheInvalidationEvent event =
                    mapper.readValue(
                            message,
                            CacheInvalidationEvent.class
                    );

            if(event.getSenderId()
                    .equals(nodeId)) {

                System.out.println(
                        "[IGNORE OWN EVENT] "
                                + event.getKey()
                );

                return;
            }

           System.out.println(
             "[INVALIDATE FROM "
             + event.getSenderId()
              + "] key="
              + event.getKey()
             + " version="
             + event.getVersion()
            );

           cacheService.invalidate(
        event.getKey(),
        event.getVersion()
);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}