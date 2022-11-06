
package com.sample.redis.config;

import com.sample.redis.consumer.VideoEventConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

@Configuration
public class RedisConfig {

    @Value("${stream.key:video-streams}")
    private String streamKey;

    @Value("${stream.consumer.group:video-streams-consumer-group}")
    private String streamConsumerGroup;
    @Autowired
    private VideoEventConsumer streamListener;

    @Bean
    public Subscription subscription(RedisConnectionFactory redisConnectionFactory) throws UnknownHostException {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, String>> options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofSeconds(1))
                .targetType(String.class)
                .build();
        StreamMessageListenerContainer<String, ObjectRecord<String, String>> listenerContainer = StreamMessageListenerContainer
                .create(redisConnectionFactory, options);
        /*Subscription subscription = listenerContainer.receive(
                Consumer.from(streamKey, InetAddress.getLocalHost()
                        .getHostName()),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                streamListener);*/
        Subscription subscription = listenerContainer.receiveAutoAck(Consumer.from(streamConsumerGroup, InetAddress.getLocalHost()
                .getHostName()), StreamOffset.create(streamKey, ReadOffset.lastConsumed()), streamListener);
        listenerContainer.start();
        return subscription;
    }
}
