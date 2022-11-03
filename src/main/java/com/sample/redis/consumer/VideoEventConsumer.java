package com.sample.redis.consumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class VideoEventConsumer implements StreamListener<String, ObjectRecord<String, String>> {

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Autowired
    private ReactiveRedisTemplate<String, String> redisTemplate;

    @Override
    @SneakyThrows
    public void onMessage(ObjectRecord<String, String> record) {
        log.info(InetAddress.getLocalHost().getHostName() + " - consumed :" + record.getValue());
        System.out.println(record.getValue());
        this.redisTemplate
                .opsForZSet().incrementScore(record.getValue(),record.getValue(),1);

        atomicInteger.incrementAndGet();
    }

    @Scheduled(fixedRate = 10000)
    public void showPublishedEventsSoFar() {
        log.info("Total Consumer :: " + atomicInteger.get());
    }

}
