package com.aroussi.twitter;

import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class TwitterConsumer {

    @KafkaListener(topics = "twitter", groupId = "g1")
    public void receiveMsg(String msg){
        log.info("hello : {}", msg);
    }
}
