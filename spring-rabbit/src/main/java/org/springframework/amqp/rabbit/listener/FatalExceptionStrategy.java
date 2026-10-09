/*
 * Copyright 2014-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;


/**
 * A strategy interface for the {@code ConditionalRejectingErrorHandler} to
 * decide whether an exception should be considered to be fatal and the
 * message should not be requeued.
 * @author Gary Russell
 * @since 1.3.2
 *
 */
@FunctionalInterface
public interface FatalExceptionStrategy {

	boolean isFatal(Throwable t);

}
