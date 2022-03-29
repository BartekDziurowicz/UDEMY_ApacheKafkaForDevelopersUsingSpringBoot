package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    private String topic = "library-events";

    @Autowired
    ObjectMapper objectMapper;

//    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
//        Integer key = libraryEvent.getLibraryEventId();
//        String value = objectMapper.writeValueAsString(libraryEvent);
//        ListenableFuture<SendResult<Integer, String>> listenableFuture =  kafkaTemplate.sendDefault(key, value);
//        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
//            @Override
//            public void onFailure(Throwable ex) {
//                handleFailure(key, value, ex);
//            }
//
//            @Override
//            public void onSuccess(SendResult<Integer, String> result) {
//                handleSuccess(key, value, result);
//            }
//        });
//    }

    //approach 3 using "send", we can add topic
    public void sendLibraryEvent_Send(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key, value, topic);

        ListenableFuture<SendResult<Integer,String>> listenableFuture =  kafkaTemplate.send(producerRecord);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        return new ProducerRecord<>(topic, null, key, value, null);
    }

    private ProducerRecord<Integer, String> buildProducerRecordWithHeaders(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    //synchronicznie
//    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
//        Integer key = libraryEvent.getLibraryEventId();
//        String value = objectMapper.writeValueAsString(libraryEvent);
//        SendResult<Integer, String> sendResult = null;
//        try {
//            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
//        } catch (ExecutionException | InterruptedException e) {
//            log.info("ExecutionException | InterruptedException Sending the Message and the exception is {}", e.getMessage());
//            throw e;
//        } catch (Exception e) {
//            log.info("Exception Sending the Message and the exception is {}", e.getMessage());
//            throw e;
//        }
//
//        return sendResult;
//    }

    private void handleFailure(Integer key, String value, Throwable ex){
        log.info("Error Sending the Message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable){
            log.error("Error in onFailure: {}", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent Successfully for key : {} and the valueis {}, partition is {}", key, value, result.getRecordMetadata().partition());
    }
}
