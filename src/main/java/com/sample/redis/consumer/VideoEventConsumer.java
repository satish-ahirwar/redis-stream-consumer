package com.sample.redis.consumer;

import com.sample.redis.service.MessageProcessService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class VideoEventConsumer  {

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Autowired
    private MessageProcessService messageProcessService;


    @SneakyThrows
    public void onMessage(ObjectRecord<String, String> record) {
        log.info(InetAddress.getLocalHost().getHostName() + " - consumed :" + record.getValue());
        System.out.println("message::"+record.getId());
        messageProcessService.process(record.getValue(), record.getId());
        atomicInteger.incrementAndGet();
    }

    @Scheduled(fixedRate = 10000)
    public void showPublishedEventsSoFar() {
        log.info("Total Consumer :: " + atomicInteger.get());
    }

}
