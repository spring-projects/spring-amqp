/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.rabbit.stream.producer;

import java.util.function.BiConsumer;

import com.rabbitmq.stream.ProducerBuilder;

/**
 * Called to enable customization of the {@link ProducerBuilder} when a new producer is
 * created. The first parameter should be the bean name of the component that calls this
 * customizer. Refer to the RabbitMQ Stream Java Client for customization options.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
@FunctionalInterface
public interface ProducerCustomizer extends BiConsumer<String, ProducerBuilder> {
}
