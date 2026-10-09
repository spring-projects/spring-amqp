/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener.adapter;

import java.util.function.BiFunction;

import org.springframework.amqp.core.Message;

/**
 * A post-processor for replies. The first parameter to the function is the request
 * message, the second is the response message; it must return the modified (or a new)
 * message. Use this, for example, if you want to copy additional headers from the request
 * message.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 *
 */
public interface ReplyPostProcessor extends BiFunction<Message, Message, Message> {

}
