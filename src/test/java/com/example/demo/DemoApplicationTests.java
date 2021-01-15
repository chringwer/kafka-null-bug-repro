package com.example.demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.integration.context.IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME;
import static org.springframework.kafka.support.KafkaHeaders.MESSAGE_KEY;
import static org.springframework.test.annotation.DirtiesContext.MethodMode.AFTER_METHOD;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.KafkaNullConverter;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
class DemoApplicationTests
{
    @Autowired
    InputDestination input;

    @Autowired
    OutputDestination output;

    @Autowired
    @Qualifier(ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME)
    private CompositeMessageConverter messageConverter;

    @ParameterizedTest
    @ValueSource(strings = { "multiInput", "singleInput" })
    public void shouldEchoMessageKey(final String functionName)
    {
        input.send(MessageBuilder.withPayload(KafkaNull.INSTANCE).setHeader(MESSAGE_KEY, "key").build(),
                   functionName + "-in-0");

        final Message<byte[]> message = output.receive(1000L, functionName + "-out-0");

        assertThat(message).isNotNull();

        assertThat(new String(message.getPayload())).isEqualTo("key");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    @DirtiesContext(methodMode = AFTER_METHOD)
    public void shouldReturnTombstone(final boolean registerKafkaNullConverter)
    {
        if (registerKafkaNullConverter)
        {
            messageConverter.getConverters().add(new KafkaNullConverter());
        }

        input.send(MessageBuilder.withPayload(KafkaNull.INSTANCE).setHeader(MESSAGE_KEY, "key").build(),
                   "nullOutput-in-0");

        final Message<?> message = output.receive(1000L, "nullOutput-out-0");

        assertThat(message).isNotNull();
        assertThat(message.getHeaders()).containsEntry(MESSAGE_KEY, "key");
        assertThat(message.getPayload()).isInstanceOf(KafkaNull.class);
    }
}
