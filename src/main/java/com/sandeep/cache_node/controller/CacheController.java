package com.sandeep.cache_node.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sandeep.cache_node.dto.CacheRequest;
import com.sandeep.cache_node.model.CacheEntry;
import com.sandeep.cache_node.service.CacheService;
import com.sandeep.cache_node.service.VersionService;

@RestController
@RequestMapping("/cache")
public class CacheController {

    private final CacheService cacheService;
    private final VersionService versionService;

    public CacheController(CacheService cacheService,VersionService versionService) {
        this.cacheService = cacheService;
        this.versionService = versionService;
    }

    @PutMapping("/{key}")
    public ResponseEntity<String> put(
            @PathVariable String key,
            @RequestBody CacheRequest request) {

        cacheService.put(key, request.getValue());

        return ResponseEntity.ok("Stored");
    }

    @GetMapping("/{key}")
    public ResponseEntity<?> get(
            @PathVariable String key) {

        String value = cacheService.get(key);

        if (value == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(value);
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(
            @PathVariable String key) {

        cacheService.delete(key);

        return ResponseEntity.ok("Deleted");
    }

    @GetMapping
    public Map<String, CacheEntry> getAll() {
        return cacheService.getAll();
    }

   @GetMapping("/versions")
public Map<String, Long> versions() {
    return cacheService.getVersions();
}

}