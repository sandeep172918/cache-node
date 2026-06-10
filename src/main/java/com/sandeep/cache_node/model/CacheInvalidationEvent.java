package com.sandeep.cache_node.model;

public class CacheInvalidationEvent {

    private String key;
    private String senderId;
    private long version;

    public CacheInvalidationEvent() {
    }

    public CacheInvalidationEvent(
            String key,
            String senderId) {

        this.key = key;
        this.senderId = senderId;
    }
     public CacheInvalidationEvent(
            String key,
            String senderId,
            long version) {

        this.key = key;
        this.senderId = senderId;
        this.version=version;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }
     public long getVersion(){
        return version;
    }
    public void setVersion(long version){
        this.version=version;
    }
}