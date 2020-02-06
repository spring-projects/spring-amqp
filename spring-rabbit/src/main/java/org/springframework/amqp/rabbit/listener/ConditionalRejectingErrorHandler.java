/*
 * Copyright 2014-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
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
 * @since 1.3.2
 *
 */
public class ConditionalRejectingErrorHandler implements ErrorHandler {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private final FatalExceptionStrategy exceptionStrategy;

	private boolean discardFatalsWithXDeath = true;

	private boolean rejectManual = true;

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

	/**
	 * Set to false to disable the (now) default behavior of logging and discarding
	 * messages that cause fatal exceptions and have an `x-death` header; which
	 * usually means that the message has been republished after previously being
	 * sent to a DLQ.
	 * @param discardFatalsWithXDeath false to disable.
	 * @since 2.1
	 */
	public void setDiscardFatalsWithXDeath(boolean discardFatalsWithXDeath) {
		this.discardFatalsWithXDeath = discardFatalsWithXDeath;
	}

	/**
	 * Set to false to NOT reject a fatal message when MANUAL ack mode is being used.
	 * @param rejectManual false to leave the message in an unack'd state.
	 * @since 2.1.9
	 */
	public void setRejectManual(boolean rejectManual) {
		this.rejectManual = rejectManual;
	}

	@Override
	public void handleError(Throwable t) {
		log(t);
		if (!this.causeChainContainsARADRE(t) && this.exceptionStrategy.isFatal(t)) {
			if (this.discardFatalsWithXDeath && t instanceof ListenerExecutionFailedException) {
				Message failed = ((ListenerExecutionFailedException) t).getFailedMessage();
				if (failed != null) {
					List<Map<String, ?>> xDeath = failed.getMessageProperties().getXDeathHeader();
					if (xDeath != null && xDeath.size() > 0) {
						this.logger.error("x-death header detected on a message with a fatal exception; "
								+ "perhaps requeued from a DLQ? - discarding: " + failed);
						throw new ImmediateAcknowledgeAmqpException("Fatal and x-death present");
					}
				}
			}
			throw new AmqpRejectAndDontRequeueException("Error Handler converted exception to fatal", this.rejectManual,
					t);
		}
	}

	/**
	 * Log the throwable at WARN level, including stack trace.
	 * Subclasses can override this behavior.
	 * @param t the {@link Throwable}.
	 * @since 1.7.8
	 */
	protected void log(Throwable t) {
		if (this.logger.isWarnEnabled()) {
			this.logger.warn("Execution of Rabbit message listener failed.", t);
		}
	}

	/**
	 * Return true if there is already an {@link AmqpRejectAndDontRequeueException}
	 * present in the cause chain.
	 * @param t a {@link Throwable}.
	 * @return true if the cause chain already contains an
	 * {@link AmqpRejectAndDontRequeueException}.
	 * @since 1.7.8
	 */
	protected boolean causeChainContainsARADRE(Throwable t) {
		Throwable cause = t.getCause();
		while (cause != null) {
			if (cause instanceof AmqpRejectAndDontRequeueException) {
				return true;
			}
			cause = cause.getCause();
		}
		return false;
	}

	/**
	 * Default implementation of {@link FatalExceptionStrategy}.
	 * @since 1.6.3
	 */
	public static class DefaultExceptionStrategy implements FatalExceptionStrategy {

		protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

		@Override
		public boolean isFatal(Throwable t) {
			Throwable cause = t.getCause();
			while (cause instanceof MessagingException
					&& !(cause instanceof org.springframework.messaging.converter.MessageConversionException)
					&& !(cause instanceof MethodArgumentResolutionException)) {
				cause = cause.getCause();
			}
			if (t instanceof ListenerExecutionFailedException && isCauseFatal(cause)) {
				logFatalException((ListenerExecutionFailedException) t, cause);
				return true;
			}
			return false;
		}

		private boolean isCauseFatal(Throwable cause) {
			return cause instanceof MessageConversionException // NOSONAR boolean complexity
					|| cause instanceof org.springframework.messaging.converter.MessageConversionException
					|| cause instanceof MethodArgumentResolutionException
					|| cause instanceof NoSuchMethodException
					|| cause instanceof ClassCastException
					|| isUserCauseFatal(cause);
		}

		/**
		 * Log the fatal ListenerExecutionFailedException at WARN level, excluding stack
		 * trace. Subclasses can override this behavior.
		 * @param t the {@link ListenerExecutionFailedException}.
		 * @param cause the root cause (skipping any general {@link MessagingException}s).
		 * @since 2.2.4
		 */
		protected void logFatalException(ListenerExecutionFailedException t, Throwable cause) {
			if (this.logger.isWarnEnabled()) {
				this.logger.warn(
						"Fatal message conversion error; message rejected; "
								+ "it will be dropped or routed to a dead letter exchange, if so configured: "
								+ t.getFailedMessage());
			}
		}

		/**
		 * Subclasses can override this to add custom exceptions.
		 * @param cause the cause
		 * @return true if the cause is fatal.
		 */
		protected boolean isUserCauseFatal(Throwable cause) {
			return false;
		}

	}

}
