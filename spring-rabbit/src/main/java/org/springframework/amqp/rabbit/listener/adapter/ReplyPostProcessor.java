/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.adapter;

/**
 * A post processor for replies. The first parameter to the function is the request
 * message, the second is the response message; it must return the modified (or a new)
 * message. Use this, for example, if you want to copy additional headers from the request
 * message.
 *
 * @author Gary Russell
 *
 * @since 2.2.5
 *
 * @deprecated since 4.1 in favor of {@link org.springframework.amqp.listener.adapter.ReplyPostProcessor}.
 */
@Deprecated(since = "4.1", forRemoval = true)
public interface ReplyPostProcessor extends org.springframework.amqp.listener.adapter.ReplyPostProcessor {

}
