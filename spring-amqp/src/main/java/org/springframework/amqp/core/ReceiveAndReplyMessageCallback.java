/*
 * Copyright 2013-present the original author or authors.
 */

package org.springframework.amqp.core;

/**
 * {@link Message} exchanging contract of {@link ReceiveAndReplyCallback}.
 *
 * @author Artem Bilan
 * @author Gary Russell
 * @since 1.3
 */
@FunctionalInterface
public interface ReceiveAndReplyMessageCallback extends ReceiveAndReplyCallback<Message, Message> {

}
