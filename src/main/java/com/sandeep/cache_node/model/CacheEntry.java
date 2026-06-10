package com.sandeep.cache_node.model;

public class CacheEntry {

    private String value;
    private long version;

    public CacheEntry() {
    }
    public CacheEntry(String value) {
        this.value = value;
    }
    public CacheEntry(String value,long version) {
        this.value = value;
        this.version= version;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
    public long getVersion(){
        return version;
    }
    public void setVersion(long version){
        this.version=version;
    }
}