package com.sandeep.cache_node.model;

public class CacheEntry {

    private String value;

    private long version;

    private CacheState state;

    public CacheEntry() {
    }

    public CacheEntry(
            String value,
            long version,
            CacheState state) {

        this.value = value;
        this.version = version;
        this.state = state;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public CacheState getState() {
        return state;
    }

    public void setState(CacheState state) {
        this.state = state;
    }
}