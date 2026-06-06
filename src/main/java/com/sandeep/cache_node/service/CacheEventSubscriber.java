package com.sandeep.cache_node.service;

import org.springframework.stereotype.Service;

@Service
public class CacheEventSubscriber {

    public void receiveMessage(String key) {

        System.out.println(
                "[event received] " + key
        );
    }
}