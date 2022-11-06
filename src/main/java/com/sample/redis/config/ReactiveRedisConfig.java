
package com.sample.redis.config;

import com.sample.redis.consumer.VideoEventConsumer;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamReceiver;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;


@Component
public class ReactiveRedisConfig {

    @Value("${stream.key:video-streams}")
    private String streamKey;

    @Value("${stream.consumer.group:video-streams-consumer-group}")
    private String streamConsumerGroup;

    @Autowired
    private VideoEventConsumer streamListener;

    private ReactiveRedisConnectionFactory redisConnectionFactory;

    public ReactiveRedisConfig(ReactiveRedisConnectionFactory redisConnectionFactory) {
        this.redisConnectionFactory = redisConnectionFactory;
    }

    @PostConstruct
    @SneakyThrows
    public void subscription() {
        System.out.println("started...............");
        StreamReceiver.StreamReceiverOptions<String, ObjectRecord<String, String>> options = StreamReceiver.StreamReceiverOptions
                .builder()
                .pollTimeout(Duration.ofSeconds(1))
                .targetType(String.class)
                .build();
        StreamReceiver<String, ObjectRecord<String, String>> receiver = StreamReceiver
                .create(redisConnectionFactory, options);
        Flux<ObjectRecord<String, String>> objectRecordFlux = receiver.receiveAutoAck(Consumer.from(streamConsumerGroup, InetAddress.getLocalHost()
                .getHostName()), StreamOffset.create(streamKey, ReadOffset.lastConsumed()));
        objectRecordFlux.subscribe(e -> {
            streamListener.onMessage(e);
        });
    }
}
