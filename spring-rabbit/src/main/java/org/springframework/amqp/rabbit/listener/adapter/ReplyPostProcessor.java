/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

import java.util.function.BiFunction;

import org.springframework.amqp.core.Message;

/**
 * A post processor for replies. The first parameter to the function is the request
 * message, the second is the response message; it must return the modified (or a new)
 * message. Use this, for example, if you want to copy additional headers from the request
 * message.
 *
 * @author Gary Russell
 * @since 2.2.5
 *
 */
public interface ReplyPostProcessor extends BiFunction<Message, Message, Message> {

}
