/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.util.ErrorHandler;

/**
 * {@link ErrorHandler} that conditionally wraps the Exception in an
 * {@link AmqpRejectAndDontRequeueException} if the configured rejection
 * strategy determines that the message is fatal and should not be requeued.
 * Such messages will be discarded or sent to a Dead Letter Exchange, depending
 * on broker configuration.
 * <p>
 * The default strategy will do this if the exception is a
 * {@link ListenerExecutionFailedException} with a cause of {@link MessageConversionException},
 * {@link org.springframework.messaging.converter.MessageConversionException},
 * {@link org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException},
 * {@link org.springframework.messaging.handler.annotation.support.MethodArgumentTypeMismatchException},
 * {@link NoSuchMethodException} or {@link ClassCastException}.
 * <p>
 * The exception will not be wrapped if the {@code cause} chain already contains an
 * {@link AmqpRejectAndDontRequeueException}.
 *
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 1.3.2
 *
 * @deprecated in favor of {@link org.springframework.amqp.listener.ConditionalRejectingErrorHandler}
 */
@Deprecated(forRemoval = true, since = "4.1")
public class ConditionalRejectingErrorHandler extends org.springframework.amqp.listener.ConditionalRejectingErrorHandler {

	/**
	 * Create a handler with the {@link DefaultExceptionStrategy}.
	 */
	public ConditionalRejectingErrorHandler() {
		super();
	}

	/**
	 * Create a handler with the supplied {@link org.springframework.amqp.listener.FatalExceptionStrategy} implementation.
	 * @param exceptionStrategy The strategy implementation.
	 */
	public ConditionalRejectingErrorHandler(org.springframework.amqp.listener.FatalExceptionStrategy exceptionStrategy) {
		super(exceptionStrategy);
	}

}
