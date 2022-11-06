package com.sample.redis.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.swing.*;

@Service
@Slf4j
public class MessageProcessService {

    @SneakyThrows
    public void process(String message, RecordId recordId) {
        log.info("Start process" + recordId);
        Thread.sleep(5000L);
        log.info("End process" + recordId);
    }
}
