/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener.adapter;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.Message;

/**
 * Root object for reply expression evaluation.
 *
 * @param request the request message.
 * @param source the source data (e.g. {@code o.s.messaging.Message<?>}).
 * @param result the result.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public record ReplyExpressionRoot(Message request, @Nullable Object source, @Nullable Object result) {

}
