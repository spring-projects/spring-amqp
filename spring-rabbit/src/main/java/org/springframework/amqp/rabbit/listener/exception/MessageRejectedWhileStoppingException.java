/*
 * Copyright 2010-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener.exception;

/**
 * Exception class that indicates a rejected message on shutdown. Used to trigger a rollback for an
 * external transaction manager in that case.
 *
 * @deprecated in favor of {@link org.springframework.amqp.listener.MessageRejectedWhileStoppingException}.
 */
@Deprecated(forRemoval = true, since = "4.1")
@SuppressWarnings("serial")
public class MessageRejectedWhileStoppingException extends org.springframework.amqp.listener.MessageRejectedWhileStoppingException {

}
