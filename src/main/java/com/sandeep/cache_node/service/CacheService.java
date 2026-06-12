package com.sandeep.cache_node.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import com.sandeep.cache_node.model.CacheEntry;
import com.sandeep.cache_node.model.CacheState;

@Service
public class CacheService {

    private final CacheEventPublisher publisher;
    private final VersionService versionService;
    

    private final Map<String, CacheEntry> cache =
            new ConcurrentHashMap<>();

    private final Map<String, Long>
        latestVersions =
        new ConcurrentHashMap<>();        

   public CacheService(
        CacheEventPublisher publisher,
        VersionService versionService) {

    this.publisher = publisher;
    this.versionService = versionService;
}      

   public void put(String key, String value) {
     long version = versionService.nextVersion();

      System.out.println("[PUT] key="+ key+ " version="+ version);

      latestVersions.put(
        key,
        version
      );

    cache.put(
    key,
    new CacheEntry(
        value,
        version,
        CacheState.MODIFIED
    )
);

     publisher.publish(key,version);
   }

    public String get(String key) {

    System.out.println(
            "[GET] Thread=" +
            Thread.currentThread().getName() +
            " key=" + key
    );

    CacheEntry entry = cache.get(key);

    if (entry == null)
        return null;

    return entry.getValue();
}

   public void delete(String key) {

    System.out.println(
            "[DELETE] Thread=" +
            Thread.currentThread().getName() +
            " key=" + key
    );

   CacheEntry entry =
        cache.get(key);

if(entry != null){

    entry.setState(
            CacheState.INVALID
    );
}
}

  public void invalidate(
        String key,
        long version) {

    Long currentVersion =
            latestVersions.get(key);

    if(currentVersion != null
       && version <= currentVersion) {

        System.out.println(
            "[STALE EVENT IGNORED] key="
            + key
            + " incoming="
            + version
            + " current="
            + currentVersion
        );

        return;
    }

    latestVersions.put(
            key,
            version
    );

    System.out.println(
        "[INVALIDATE] key="
        + key
        + " version="
        + version
    );

   CacheEntry entry =
        cache.get(key);

if(entry != null){

    entry.setState(
            CacheState.INVALID
    );
}
}

    public Map<String, CacheEntry> getAll() {
        return cache;
    }
    public Map<String, Long> getVersions() {
    return latestVersions;
   }
}