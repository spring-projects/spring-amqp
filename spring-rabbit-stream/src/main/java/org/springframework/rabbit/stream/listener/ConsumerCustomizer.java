/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.rabbit.stream.listener;

import java.util.function.BiConsumer;

import com.rabbitmq.stream.ConsumerBuilder;

/**
 * Customizer for {@link ConsumerBuilder}. The first parameter should be the bean name (or
 * listener id) of the component that calls this customizer. Refer to the RabbitMQ Stream
 * Java Client for customization options.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
@FunctionalInterface
public interface ConsumerCustomizer extends BiConsumer<String, ConsumerBuilder> {
}
