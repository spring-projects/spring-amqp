/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.listener;


/**
 * A strategy interface for the {@code ConditionalRejectingErrorHandler} to
 * decide whether an exception should be considered as fatal and the
 * message should not be requeued (released), rather discarded (rejected).
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 */
@FunctionalInterface
public interface FatalExceptionStrategy {

	boolean isFatal(Throwable throwable);

}
