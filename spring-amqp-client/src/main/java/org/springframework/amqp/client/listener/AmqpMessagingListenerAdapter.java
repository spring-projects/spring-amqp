/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.listener;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.client.AmqpClientNackReceivedException;
import org.springframework.amqp.client.ProtonUtils;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.listener.ContainerUtils;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.listener.adapter.HandlerAdapter;
import org.springframework.amqp.listener.adapter.InvocationResult;
import org.springframework.amqp.listener.adapter.ReplyExpressionRoot;
import org.springframework.amqp.listener.adapter.ReplyFailureException;
import org.springframework.amqp.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.SimpleAmqpHeaderMapper;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.MessagingMessageConverterAdapter;
import org.springframework.amqp.support.converter.RemoteInvocationResult;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.amqp.utils.MonoHandler;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.MapAccessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.messaging.MessagingException;

/**
 * The {@link ProtonDeliveryListener} implementation for POJO listeners
 * delegating to the provided {@link HandlerAdapter}.
 * <p>
 * Wraps the incoming {@link Delivery} to {@link Message},
 * and further to the Spring's {@link org.springframework.messaging.Message} abstraction,
 * copying the standard headers using a configurable
 * {@link AmqpHeaderMapper AmqpHeaderMapper}.
 * <p>
 * The original {@link Delivery} and {@link Message Message}
 * are provided as additional arguments so that these can be injected as method arguments if necessary.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class AmqpMessagingListenerAdapter implements AcknowledgingProtonDeliveryListener {

	private static final LogAccessor LOGGER = new LogAccessor(AmqpMessagingListenerAdapter.class);

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private static final ParserContext PARSER_CONTEXT = new TemplateParserContext("!{", "}");

	private final StandardEvaluationContext evalContext = new StandardEvaluationContext();

	private final HandlerAdapter handlerAdapter;

	private final MessagingMessageConverterAdapter messagingMessageConverter;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private boolean isManualAck;

	private boolean defaultRequeueRejected = true;

	private @Nullable ReplyPostProcessor replyPostProcessor;

	private @Nullable AmqpListenerErrorHandler errorHandler;

	private boolean returnExceptions;

	private @Nullable String replyContentType;

	private @Nullable Expression responseExpression;

	/**
	 * Create an instance based on the provided {@link HandlerAdapter}.
	 * @param handlerAdapter the {@link HandlerAdapter} to invoke as POJO message listener.
	 */
	public AmqpMessagingListenerAdapter(HandlerAdapter handlerAdapter) {
		this.handlerAdapter = handlerAdapter;
		this.messagingMessageConverter =
				new MessagingMessageConverterAdapter(handlerAdapter.getBean(), handlerAdapter.getMethod(),
						false, Delivery.class);
		this.evalContext.setTypeConverter(new StandardTypeConverter());
		this.evalContext.addPropertyAccessor(new MapAccessor());
	}

	@Override
	public void containerAckMode(AcknowledgeMode mode) {
		this.isManualAck = AcknowledgeMode.MANUAL.equals(mode);
	}

	@Override
	public boolean isAsyncReplies() {
		return this.handlerAdapter.isAsyncReplies();
	}

	/**
	 * Set the default behavior for messages rejection, for example, when the listener
	 * threw an exception. When {@code true} (default), messages will be requeued, otherwise - rejected.
	 * Setting to {@code false} causes all rejections to not be requeued.
	 * When set to {@code true}, the default can be overridden by the listener throwing an
	 * {@link org.springframework.amqp.AmqpRejectAndDontRequeueException}.
	 * @param defaultRequeueRejected false to reject messages by default.
	 */
	public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
		this.defaultRequeueRejected = defaultRequeueRejected;
	}

	/**
	 * Set a {@link ReplyPostProcessor} to post process a response message before it is
	 * sent. It is called after {@link #postProcessResponse(Message, Message)} which sets
	 * up a {@code correlationId} property on the response message.
	 * @param replyPostProcessor the post-processor.
	 */
	public void setReplyPostProcessor(ReplyPostProcessor replyPostProcessor) {
		this.replyPostProcessor = replyPostProcessor;
	}

	/**
	 * Set the {@link AmqpListenerErrorHandler} to invoke if the listener method
	 * throws an exception.
	 * @param errorHandler the error handler.
	 */
	public void setErrorHandler(AmqpListenerErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set whether exceptions thrown by the listener should be returned as a response message body
	 * to the sender using the normal {@code replyTo/@SendTo} semantics.
	 * @param returnExceptions true to return exceptions.
	 */
	public void setReturnExceptions(boolean returnExceptions) {
		this.returnExceptions = returnExceptions;
	}

	/**
	 * Set the reply content type.
	 * Overrides the one populated by a message converter.
	 * @param replyContentType the content type.
	 */
	public void setReplyContentType(String replyContentType) {
		this.replyContentType = replyContentType;
	}

	/**
	 * Set the default replyTo address to use when sending response messages.
	 * This is only used if the replyTo from the received message is null.
	 * <p>
	 * Response destinations are only relevant for listener methods
	 * that return result objects, which will be wrapped in
	 * a response message and sent to a response destination.
	 * <p>
	 * It can be a string surrounded by "!{...}" in which case the expression is
	 * evaluated at runtime; see the reference manual for more information.
	 * @param defaultReplyTo The replyTo address.
	 */
	public void setResponseAddress(String defaultReplyTo) {
		if (defaultReplyTo.startsWith(PARSER_CONTEXT.getExpressionPrefix())) {
			this.responseExpression = PARSER.parseExpression(defaultReplyTo, PARSER_CONTEXT);
		}
		else {
			this.responseExpression = new LiteralExpression(defaultReplyTo);
		}
	}

	/**
	 * Set the {@link AmqpHeaderMapper} implementation to use to map the standard
	 * AMQP headers. By default, a {@link SimpleAmqpHeaderMapper} is used.
	 * @param headerMapper the {@link AmqpHeaderMapper} instance.
	 * @see SimpleAmqpHeaderMapper
	 */
	public void setHeaderMapper(AmqpHeaderMapper headerMapper) {
		this.messagingMessageConverter.setHeaderMapper(headerMapper);
	}

	/**
	 * Set the converter to convert incoming messages to listener method arguments,
	 * and objects returned from listener methods for response messages.
	 * The default converter is a {@link org.springframework.amqp.support.converter.SimpleMessageConverter}.
	 * @param messageConverter The message converter.
	 * @see MessagingMessageConverterAdapter
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messagingMessageConverter.setPayloadConverter(messageConverter);
		this.messageConverter = messageConverter;
	}

	/**
	 * Set a bean resolver for runtime SpEL expressions.
	 * @param beanResolver the resolver.
	 */
	public void setBeanResolver(BeanResolver beanResolver) {
		this.evalContext.setBeanResolver(beanResolver);
	}

	@Override
	public void onDelivery(Delivery delivery, @Nullable AmqpAcknowledgment acknowledgment) throws Exception {
		Message amqpMessage = ProtonUtils.fromProtonMessage(delivery.message());
		if (acknowledgment != null) {
			amqpMessage.getMessageProperties().setAmqpAcknowledgment(acknowledgment);
		}
		org.springframework.messaging.Message<?> message = null;
		try {
			message = (org.springframework.messaging.Message<?>) this.messagingMessageConverter.fromMessage(amqpMessage);
			invokeHandlerAndProcessResult(delivery, amqpMessage, message);
		}
		catch (ListenerExecutionFailedException ex) {
			handleException(delivery, amqpMessage, message, ex);
		}
		catch (ReplyFailureException ex) {
			throw ex;
		}
		catch (Exception ex) {
			handleException(delivery, amqpMessage, message,
					new ListenerExecutionFailedException("Failed to handle message", ex, amqpMessage));
		}
	}

	private void invokeHandlerAndProcessResult(Delivery delivery, Message amqpMessage,
			org.springframework.messaging.Message<?> message) {

		LOGGER.debug("Processing [" + message + "]");
		if (this.messagingMessageConverter.getMethod() == null) {
			amqpMessage.getMessageProperties().setTargetMethod(this.handlerAdapter.getMethodFor(message.getPayload()));
		}
		InvocationResult result = invokeHandler(delivery, amqpMessage, message);
		if (result.getReturnValue() != null) {
			handleResult(delivery, result, amqpMessage, message);
		}
		else {
			LOGGER.trace("No result object given - no result to handle");
		}
	}

	private InvocationResult invokeHandler(Delivery delivery, Message amqpMessage,
			org.springframework.messaging.Message<?> message) {

		MessageProperties messageProperties = amqpMessage.getMessageProperties();
		try {
			return this.handlerAdapter.invoke(message, amqpMessage, delivery, messageProperties,
					messageProperties.getAmqpAcknowledgment());
		}
		catch (MessagingException ex) {
			String errorMessage = """
					Listener method could not be invoked with the incoming message.
					Endpoint handler details:
					Method [%s]
					Bean [%s]
					"""
					.formatted(this.handlerAdapter.getMethodAsString(message.getPayload()),
							this.handlerAdapter.getBean());
			throw new ListenerExecutionFailedException(errorMessage, ex, amqpMessage);
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener method '" +
					this.handlerAdapter.getMethodAsString(message.getPayload()) + "' threw exception", ex, amqpMessage);
		}
	}

	private void handleException(Delivery delivery, Message amqpMessage,
			org.springframework.messaging.@Nullable Message<?> message,
			ListenerExecutionFailedException lefe) throws Exception {

		if (this.errorHandler != null) {
			try {
				Object errorResult = this.errorHandler.handleError(delivery, message, lefe);
				if (errorResult != null) {
					Object payload = message == null ? null : message.getPayload();
					InvocationResult resultArg = payload == null
							? new InvocationResult(errorResult, null, null, null, null)
							: this.handlerAdapter.getInvocationResultFor(errorResult, payload);
					handleResult(delivery, resultArg, amqpMessage, message);
				}
				else {
					LOGGER.trace("Error handler returned no result; acknowledging the message.");
					settleRequest(delivery, amqpMessage, AmqpAcknowledgment.Status.ACCEPT);
				}
			}
			catch (Exception ex) {
				returnOrThrow(delivery, amqpMessage, message, ex, ex);
			}
		}
		else {
			returnOrThrow(delivery, amqpMessage, message, lefe.getCause(), lefe);
		}
	}

	private void returnOrThrow(Delivery delivery, Message amqpMessage,
			org.springframework.messaging.@Nullable Message<?> message, @Nullable Throwable throwableToReturn,
			Exception exceptionToThrow)
			throws Exception {

		if (!this.returnExceptions) {
			throw exceptionToThrow;
		}
		Object payload = message == null ? null : message.getPayload();
		Type returnType = payload == null ? Object.class : this.handlerAdapter.getReturnTypeFor(payload);
		try {
			InvocationResult resultArg =
					new InvocationResult(new RemoteInvocationResult(throwableToReturn),
							null, returnType, this.handlerAdapter.getBean(),
							payload == null ? null : this.handlerAdapter.getMethodFor(payload));
			handleResult(delivery, resultArg, amqpMessage, message);
		}
		catch (ReplyFailureException rfe) {
			if (payload == null || void.class.equals(returnType)) {
				throw exceptionToThrow;
			}
			else {
				throw rfe;
			}
		}
	}

	private void handleResult(Delivery delivery, @Nullable InvocationResult resultArg, Message request,
			@Nullable Object source) {

		if (resultArg != null) {
			Object returnValue = resultArg.getReturnValue();
			if (returnValue instanceof CompletableFuture<?> completable) {
				if (!this.isManualAck) {
					LOGGER.warn("Container AcknowledgeMode must be MANUAL for a Future<?> return type; "
							+ "otherwise the container will ack the message immediately");
				}
				completable.whenComplete((result, throwable) -> {
					if (throwable == null) {
						asyncSuccess(delivery, resultArg, request, source, result);
						asyncAck(delivery, request);
					}
					else {
						asyncFailure(delivery, request, throwable, source);
					}
				});
			}
			else if (JavaUtils.MONO_PRESENT && MonoHandler.isMono(returnValue)) {
				if (!this.isManualAck) {
					LOGGER.warn("Container AcknowledgeMode must be MANUAL for a Mono<?> return type" +
							"(or Kotlin suspend function); otherwise the container will ack the message immediately");
				}
				MonoHandler.subscribe(Objects.requireNonNull(returnValue),
						result -> asyncSuccess(delivery, resultArg, request, source, result),
						throwable -> asyncFailure(delivery, request, throwable, source),
						() -> asyncAck(delivery, request));
			}
			else {
				doHandleResult(delivery, resultArg, request, source);
			}
		}
	}

	private void asyncSuccess(Delivery delivery, InvocationResult resultArg, Message request,
			@Nullable Object source, @Nullable Object deferredResult) {

		if (deferredResult == null) {
			LOGGER.debug("Async result is null, ignoring");
		}
		else {
			Type returnType = resultArg.getReturnType();
			// We only get here with Mono<?> and CompletableFuture<?> which have exactly one type argument.
			// Otherwise, it might be a Kotlin suspend function
			if (returnType != null && !Object.class.getName().equals(returnType.getTypeName())) {
				Type[] actualTypeArguments = ((ParameterizedType) returnType).getActualTypeArguments();
				if (actualTypeArguments.length > 0) {
					returnType = actualTypeArguments[0];
					if (returnType instanceof WildcardType) {
						// Set the return type to null so the converter will use the actual returned
						// object's class for type info
						returnType = null;
					}
				}
			}
			InvocationResult actualResult =
					new InvocationResult(deferredResult, resultArg.getSendTo(), returnType, resultArg.getBean(),
							resultArg.getMethod());
			doHandleResult(delivery, actualResult, request, source);
		}
	}

	private void asyncFailure(Delivery delivery, Message request, Throwable throwable, @Nullable Object source) {
		try {
			handleException(delivery, request, (org.springframework.messaging.Message<?>) source,
					new ListenerExecutionFailedException("Async Fail", throwable, request));
			return;
		}
		catch (Exception ex) {
			// Ignore and reject the message against the original error
		}

		LOGGER.error(throwable, "Future, Mono, or suspend function was completed with an exception for " + request);
		boolean shouldRequeue = ContainerUtils.shouldRequeue(this.defaultRequeueRejected, throwable, LOGGER.getLog());
		AmqpAcknowledgment.Status status =
				shouldRequeue
						? AmqpAcknowledgment.Status.REQUEUE
						: AmqpAcknowledgment.Status.REJECT;
		settleRequest(delivery, request, status);
	}

	private void doHandleResult(Delivery delivery, InvocationResult resultArg, Message request,
			@Nullable Object source) {

		LOGGER.debug(() -> "Listener method returned result [" + resultArg + "] - generating response message for it");
		String replyTo = null;
		try {
			Message response = buildMessage(resultArg);
			MessageProperties props = response.getMessageProperties();
			props.setTargetBean(resultArg.getBean());
			props.setTargetMethod(resultArg.getMethod());
			postProcessResponse(request, response);
			if (this.replyPostProcessor != null) {
				response = this.replyPostProcessor.apply(request, response);
			}
			replyTo = getReplyToAddress(request, source, resultArg);
			sendResponse(delivery, replyTo, response);
		}
		catch (Exception ex) {
			Address address = replyTo != null ? new Address(replyTo) : null;
			throw new ReplyFailureException("Failed to send reply with payload '" + resultArg + "'", address, ex);
		}
	}

	private Message buildMessage(InvocationResult result) {
		Object value = result.getReturnValue();
		if (value instanceof Message msg) {
			return msg;
		}

		MessageProperties messageProperties = new MessageProperties();
		// For a content-type-based routing logic
		if (this.replyContentType != null) {
			messageProperties.setContentType(this.replyContentType);
		}
		Message message;
		if (value == null) {
			message = new Message(new byte[0], messageProperties);
		}
		else {
			Type returnType = result.getReturnType();
			if (value instanceof org.springframework.messaging.Message<?>) {
				message = this.messagingMessageConverter.toMessage(value, messageProperties, returnType);
			}
			else {
				message = this.messageConverter.toMessage(value, messageProperties, returnType);
			}
		}

		// To override the set one by a message converter
		if (this.replyContentType != null) {
			message.getMessageProperties().setContentType(this.replyContentType);
		}
		return message;
	}

	private void postProcessResponse(Message request, Message response) {
		MessageProperties requestMessageProperties = request.getMessageProperties();

		String correlation = requestMessageProperties.getCorrelationId();
		if (correlation == null) {
			String messageId = requestMessageProperties.getMessageId();
			if (messageId != null) {
				correlation = messageId;
			}
		}
		response.getMessageProperties().setCorrelationId(correlation);
	}

	/**
	 * Determine a reply-to address for the given message.
	 * <p>
	 * The default implementation first checks the Reply-To address of the supplied request; if that is not
	 * <code>null</code> it is returned; otherwise, then the configured default response address.
	 * If result still <code>null</code>, the {@link AmqpException} is thrown.
	 * @param request the original incoming AMQP message.
	 * @param source the source data (e.g. {@code o.s.messaging.Message<?>}).
	 * @param result the result.
	 * @return the reply-to address (never <code>null</code>)
	 * @throws AmqpException if no address can be determined
	 * @see #setResponseAddress(String)
	 * @see Message#getMessageProperties()
	 * @see MessageProperties#getReplyTo()
	 */
	private String getReplyToAddress(Message request, @Nullable Object source, InvocationResult result) {
		String replyTo = request.getMessageProperties().getReplyTo();
		if (replyTo == null) {
			if (result.getSendTo() != null) {
				replyTo = evaluateReplyTo(request, source, result.getReturnValue(), result.getSendTo());
			}
			else if (this.responseExpression != null) {
				replyTo = evaluateReplyTo(request, source, result.getReturnValue(), this.responseExpression);
			}
		}

		if (replyTo == null) {
			throw new AmqpException(
					"Cannot determine ReplyTo message property value: " +
							"Request message does not contain reply-to property, " +
							"and no default reply address was set.");
		}
		return replyTo;
	}

	private @Nullable String evaluateReplyTo(Message request, @Nullable Object source, @Nullable Object result,
			Expression expression) {

		if (expression instanceof LiteralExpression) {
			return expression.getValue(String.class);
		}
		else {
			return expression.getValue(this.evalContext, new ReplyExpressionRoot(request, source, result), String.class);
		}
	}

	private static void asyncAck(Delivery delivery, Message request) {
		settleRequest(delivery, request, AmqpAcknowledgment.Status.ACCEPT);
	}

	private static void settleRequest(Delivery delivery, Message request, AmqpAcknowledgment.Status status) {
		AmqpAcknowledgment amqpAcknowledgment = request.getMessageProperties().getAmqpAcknowledgment();
		try {
			if (amqpAcknowledgment != null && !delivery.settled()) {
				amqpAcknowledgment.acknowledge(status);
			}
		}
		catch (ClientException ex) {
			throw ProtonUtils.toAmqpException(ex);
		}
	}

	private static void sendResponse(Delivery delivery, String replyTo, Message response) throws Exception {
		org.apache.qpid.protonj2.client.Message<?> protonMessage = ProtonUtils.toProtonMessage(response);
		Tracker tracker =
				delivery.receiver()
						.connection()
						.openSender(replyTo)
						.send(protonMessage)
						.settlementFuture()
						.get();

		DeliveryState.Type deliveryStateType = tracker.remoteState().getType();
		if (!DeliveryState.Type.ACCEPTED.equals(deliveryStateType)) {
			throw new AmqpClientNackReceivedException("The reply message was not accepted, but " + deliveryStateType,
					protonMessage);
		}
	}

}
