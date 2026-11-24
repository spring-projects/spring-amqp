/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
