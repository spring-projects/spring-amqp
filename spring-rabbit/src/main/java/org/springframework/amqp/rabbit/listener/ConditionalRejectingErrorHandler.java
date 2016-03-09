/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
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
 * {@link ListenerExecutionFailedException} with a cause of {@link MessageConversionException}.
 * <p>
 * The exception will not be wrapped if the {@code cause} chain already contains an
 * {@link AmqpRejectAndDontRequeueException}.
 *
 * @author Gary Russell
 * @since 1.3.2
 *
 */
public class ConditionalRejectingErrorHandler implements ErrorHandler {

	protected final Log logger = LogFactory.getLog(this.getClass());

	private final FatalExceptionStrategy exceptionStrategy;

	/**
	 * Create a handler with the {@link ConditionalRejectingErrorHandler.DefaultExceptionStrategy}.
	 */
	public ConditionalRejectingErrorHandler() {
		this.exceptionStrategy = new DefaultExceptionStrategy();
	}

	/**
	 * Create a handler with the supplied {@link FatalExceptionStrategy} implementation.
	 * @param exceptionStrategy The strategy implementation.
	 */
	public ConditionalRejectingErrorHandler(FatalExceptionStrategy exceptionStrategy) {
		this.exceptionStrategy = exceptionStrategy;
	}

	@Override
	public void handleError(Throwable t) {
		if (this.logger.isWarnEnabled()) {
			this.logger.warn("Execution of Rabbit message listener failed.", t);
		}
		if (!this.causeChainContainsARADRE(t) && this.exceptionStrategy.isFatal(t)) {
			throw new AmqpRejectAndDontRequeueException("Error Handler converted exception to fatal", t);
		}
	}

	/**
	 * @return true if the cause chain already contains an
	 * {@link AmqpRejectAndDontRequeueException}.
	 */
	private boolean causeChainContainsARADRE(Throwable t) {
		Throwable cause = t.getCause();
		while (cause != null) {
			if (cause instanceof AmqpRejectAndDontRequeueException) {
				return true;
			}
			cause = cause.getCause();
		}
		return false;
	}

	private class DefaultExceptionStrategy implements FatalExceptionStrategy {

		@Override
		public boolean isFatal(Throwable t) {
			if (t instanceof ListenerExecutionFailedException
					&& t.getCause() instanceof MessageConversionException) {
				if (ConditionalRejectingErrorHandler.this.logger.isWarnEnabled()) {
					ConditionalRejectingErrorHandler.this.logger.warn(
							"Fatal message conversion error; message rejected; "
							+ "it will be dropped or routed to a dead letter exchange, if so configured: "
							+ ((ListenerExecutionFailedException) t).getFailedMessage(), t);
				}
				return true;
			}
			return false;
		}

	}

}
