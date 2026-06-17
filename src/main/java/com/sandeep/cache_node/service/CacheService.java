package com.sandeep.cache_node.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import com.sandeep.cache_node.model.CacheEntry;
import com.sandeep.cache_node.model.CacheState;

@Service
public class CacheService {

    private final CacheEventPublisher publisher;
    private final VersionService versionService;
    private final BackingStoreService backingStore;

    private final Map<String, CacheEntry> cache =
            new ConcurrentHashMap<>();

    private final Map<String, Long>
        latestVersions =
        new ConcurrentHashMap<>();

    private final Map<String, CountDownLatch>
        pendingReads =
        new ConcurrentHashMap<>();

    private final Map<String, CacheEntry>
        pendingData =
        new ConcurrentHashMap<>();

    public CacheService(
            CacheEventPublisher publisher,
            VersionService versionService,
            BackingStoreService backingStore) {

        this.publisher = publisher;
        this.versionService = versionService;
        this.backingStore = backingStore;
    }

    public void put(String key, String value) {

        long version = versionService.nextVersion();

        CacheEntry entry = cache.get(key);

        if (entry != null) {

            CacheState current = entry.getState();

            switch (current) {

                case MODIFIED:
                    System.out.println(
                        "[PUT M→M] key=" + key
                        + " version=" + version
                    );
                    entry.setValue(value);
                    entry.setVersion(version);
                    break;

                case EXCLUSIVE:
                    
                    System.out.println(
                        "[PUT E→M] key=" + key
                        + " version=" + version
                    );
                    entry.setValue(value);
                    entry.setVersion(version);
                    entry.setState(CacheState.MODIFIED);
                    break;

                case SHARED:
                    System.out.println(
                        "[PUT S→M] key=" + key
                        + " version=" + version
                    );
                    publisher.publishWriteRequest(
                        key, version
                    );
                    entry.setValue(value);
                    entry.setVersion(version);
                    entry.setState(CacheState.MODIFIED);
                    break;

                case INVALID:
                    System.out.println(
                        "[PUT I→M] key=" + key
                        + " version=" + version
                    );
                    publisher.publishWriteRequest(
                        key, version
                    );
                    entry.setValue(value);
                    entry.setVersion(version);
                    entry.setState(CacheState.MODIFIED);
                    break;
            }

        } else {
            
            System.out.println(
                "[PUT new→M] key=" + key
                + " version=" + version
            );

            publisher.publishWriteRequest(
                key, version
            );

            cache.put(
                key,
                new CacheEntry(
                    value,
                    version,
                    CacheState.MODIFIED
                )
            );
        }

        latestVersions.put(key, version);
        backingStore.save(key, value);
    }

    public String get(String key) {

        System.out.println(
            "[GET] Thread="
            + Thread.currentThread().getName()
            + " key=" + key
        );

        CacheEntry entry = cache.get(key);
        if (entry != null
            && entry.getState() != CacheState.INVALID) {

            System.out.println(
                "[GET HIT] key=" + key
                + " state=" + entry.getState()
            );

            return entry.getValue();
        }
        System.out.println(
            "[GET MISS] key=" + key
            + " — sending READ_REQUEST"
        );

        CountDownLatch latch = new CountDownLatch(1);
        pendingReads.put(key, latch);

        publisher.publishReadRequest(key);

        try {
            boolean received =
                latch.await(500, TimeUnit.MILLISECONDS);

            if (received) {
                CacheEntry shared =
                    pendingData.remove(key);

                if (shared != null) {

                    System.out.println(
                        "[GET SHARED] key=" + key
                        + " from remote node"
                    );

                    cache.put(key, shared);

                    latestVersions.put(
                        key, shared.getVersion()
                    );

                    return shared.getValue();
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pendingReads.remove(key);
        }

        String backingValue = backingStore.load(key);

        if (backingValue != null) {

            long version = versionService.nextVersion();

            System.out.println(
                "[GET EXCLUSIVE] key=" + key
                + " from backing store"
            );

            cache.put(
                key,
                new CacheEntry(
                    backingValue,
                    version,
                    CacheState.EXCLUSIVE
                )
            );

            latestVersions.put(key, version);

            return backingValue;
        }

        System.out.println(
            "[GET] key=" + key + " not found anywhere"
        );

        return null;
    }
    public void delete(String key) {

        System.out.println(
            "[DELETE] Thread="
            + Thread.currentThread().getName()
            + " key=" + key
        );

        CacheEntry entry = cache.get(key);

        if (entry != null) {
            if (entry.getState()
                    == CacheState.MODIFIED) {

                backingStore.save(
                    key, entry.getValue()
                );
            }

            entry.setState(CacheState.INVALID);

            long version = versionService.nextVersion();

            latestVersions.put(key, version);
            publisher.publishWriteRequest(
                key, version
            );
        }
    }

    public void handleRemoteWriteRequest(
            String key,
            long version) {

        Long currentVersion =
                latestVersions.get(key);

        if (currentVersion != null
            && version <= currentVersion) {

            System.out.println(
                "[STALE WRITE_REQUEST IGNORED] key="
                + key
                + " incoming=" + version
                + " current=" + currentVersion
            );

            return;
        }

        latestVersions.put(key, version);

        CacheEntry entry = cache.get(key);

        if (entry == null
            || entry.getState()
                    == CacheState.INVALID) {

            return;
        }
        if (entry.getState()
                == CacheState.MODIFIED) {

            System.out.println(
                "[FLUSH M→I] key=" + key
            );

            backingStore.save(
                key, entry.getValue()
            );
        }

        System.out.println(
            "[WRITE_REQUEST " + entry.getState()
            + "→I] key=" + key
            + " version=" + version
        );

        entry.setState(CacheState.INVALID);
    }
    public void handleRemoteReadRequest(
            String key,
            String requesterId) {

        CacheEntry entry = cache.get(key);

        if (entry == null
            || entry.getState()
                    == CacheState.INVALID) {
            return;
        }

        CacheState current = entry.getState();

        switch (current) {

            case MODIFIED:
                System.out.println(
                    "[READ_REQUEST M→S] key=" + key
                );

                backingStore.save(
                    key, entry.getValue()
                );

                entry.setState(CacheState.SHARED);

                publisher.publishDataResponse(
                    key,
                    entry.getValue(),
                    entry.getVersion()
                );
                break;

            case EXCLUSIVE:
                System.out.println(
                    "[READ_REQUEST E→S] key=" + key
                );

                entry.setState(CacheState.SHARED);

                publisher.publishDataResponse(
                    key,
                    entry.getValue(),
                    entry.getVersion()
                );
                break;

            case SHARED:
                System.out.println(
                    "[READ_REQUEST S→S] key=" + key
                );

                publisher.publishDataResponse(
                    key,
                    entry.getValue(),
                    entry.getVersion()
                );
                break;

            default:
                break;
        }
    }
    public void handleDataResponse(
            String key,
            String value,
            long version) {

        CountDownLatch latch =
                pendingReads.get(key);

        if (latch == null) {
            System.out.println(
                "[DATA_RESPONSE IGNORED] key=" + key
                + " — no pending read"
            );
            return;
        }

        System.out.println(
            "[DATA_RESPONSE RECEIVED] key=" + key
            + " version=" + version
        );
        pendingData.put(
            key,
            new CacheEntry(
                value,
                version,
                CacheState.SHARED
            )
        );
        latch.countDown();
    }
    public void invalidate(
            String key,
            long version) {

        handleRemoteWriteRequest(key, version);
    }
    public Map<String, CacheEntry> getAll() {
        return cache;
    }

    public Map<String, Long> getVersions() {
        return latestVersions;
    }
}