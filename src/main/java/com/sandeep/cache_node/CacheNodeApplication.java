package com.sandeep.cache_node;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CacheNodeApplication {

	public static void main(String[] args) {
		SpringApplication.run(CacheNodeApplication.class, args);
	}

}
