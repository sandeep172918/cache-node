package com.sandeep.cache_node.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import com.sandeep.cache_node.service.CacheEventSubscriber;

@Configuration
public class RedisPubSubConfig {

    @Bean
    ChannelTopic topic() {
        return new ChannelTopic("cache-events");
    }

    @Bean
    MessageListenerAdapter listenerAdapter(
            CacheEventSubscriber subscriber) {

        return new MessageListenerAdapter(
                subscriber,
                "receiveMessage"
        );
    }

    @Bean
    RedisMessageListenerContainer redisContainer(
            RedisConnectionFactory connectionFactory,
            MessageListenerAdapter listenerAdapter,
            ChannelTopic topic) {

        RedisMessageListenerContainer container =
                new RedisMessageListenerContainer();

        container.setConnectionFactory(
                connectionFactory
        );

        container.addMessageListener(
                listenerAdapter,
                topic
        );

        return container;
    }
}