/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
