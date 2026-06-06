package com.sandeep.cache_node.model;

public class CacheInvalidationEvent {

    private String key;

    public CacheInvalidationEvent() {
    }

    public CacheInvalidationEvent(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}