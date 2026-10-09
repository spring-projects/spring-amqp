/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

/**
 * Internal abstraction used by the framework representing a message
 * listener container. Not meant to be implemented externally.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.4
 *
 * @deprecated since 4.1 in favor of {@link org.springframework.amqp.core.MessageListenerContainer}.
 */
@Deprecated(forRemoval = true, since = "4.1")
public interface MessageListenerContainer extends org.springframework.amqp.core.MessageListenerContainer {

}
