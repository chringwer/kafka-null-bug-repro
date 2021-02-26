package com.example.demo;

import static org.springframework.kafka.support.KafkaHeaders.MESSAGE_KEY;

import java.util.function.Function;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.KafkaNullConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

@SpringBootApplication
@Import(KafkaNullConverter.class)
public class DemoApplication
{
    public static void main(final String[] args)
    {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public Function<Tuple2<Flux<Message<?>>, Flux<Message<?>>>, Flux<Message<?>>> multiInput()
    {
        return input -> input.getT1().map(message ->
        {
            return MessageBuilder.withPayload(message.getHeaders().get(MESSAGE_KEY)).build();
        });
    }

    @Bean
    public Function<Flux<Message<?>>, Flux<Message<?>>> nullOutput()
    {
        return input -> input.map(message ->
        {
            return MessageBuilder.withPayload(KafkaNull.INSTANCE)
                                 .setHeader(MESSAGE_KEY, message.getHeaders().get(MESSAGE_KEY))
                                 .build();
        });
    }

    @Bean
    public Function<Flux<Message<?>>, Flux<Message<?>>> singleInput()
    {
        return input -> input.map(message ->
        {
            return MessageBuilder.withPayload(message.getHeaders().get(MESSAGE_KEY)).build();
        });
    }
}
