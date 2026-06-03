package com.sandeep.cache_node.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import com.sandeep.cache_node.model.CacheEntry;

@Service
public class CacheService {

    private final Map<String, CacheEntry> cache =
            new ConcurrentHashMap<>();

    public void put(String key, String value) {
        cache.put(key, new CacheEntry(value));
    }

    public String get(String key) {

        CacheEntry entry = cache.get(key);

        if (entry == null)
            return null;

        return entry.getValue();
    }

    public void delete(String key) {
        cache.remove(key);
    }

    public Map<String, CacheEntry> getAll() {
        return cache;
    }
}