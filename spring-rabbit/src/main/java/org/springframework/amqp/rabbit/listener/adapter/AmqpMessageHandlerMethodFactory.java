/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

/**
 * Extension of the {@link DefaultMessageHandlerMethodFactory} for Spring AMQP requirements.
 *
 * @author Artem Bilan
 *
 * @since 3.0.5
 *
 * @deprecated since 4.1 in favor of Spring AMQP's {@link org.springframework.amqp.listener.adapter.AmqpMessageHandlerMethodFactory}.
 */
@Deprecated(forRemoval = true, since = "4.1")
public class AmqpMessageHandlerMethodFactory extends org.springframework.amqp.listener.adapter.AmqpMessageHandlerMethodFactory {

}
