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

package org.springframework.amqp.client.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be the target of an AMQP 1.0 message listener on the
 * specified {@link #addresses()}.
 * The {@link #containerFactory()} identifies the
 * {@link org.springframework.amqp.client.config.MethodAmqpMessageListenerContainerFactory}
 * to build an AMQP listener container.
 * If not set, a <em>default</em> container factory is assumed to be available with a bean name
 * {@link org.springframework.amqp.client.config.AmqpDefaultConfiguration#DEFAULT_AMQP_LISTENER_CONTAINER_FACTORY_BEAN_NAME}.
 * <p>
 * Processing of {@code @AmqpListener} annotations is performed by registering a
 * {@link AmqpListenerAnnotationBeanPostProcessor}. This can be done manually or, more
 * conveniently, through the {@link org.springframework.amqp.client.config.EnableAmqp} annotation.
 * <p>
 * Annotated methods are allowed to have flexible signatures similar to what
 * {@link MessageMapping} provides, that is
 * <ul>
 * <li>{@link org.springframework.amqp.core.Message}</li>
 * <li>{@link org.springframework.messaging.Message} to use the messaging abstraction counterpart</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Payload}
 * -annotated method arguments including the support of validation</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Header}
 * -annotated method arguments to extract a specific header value, including standard AMQP headers
 * defined by {@link org.springframework.amqp.support.AmqpHeaders}</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Headers}
 * -annotated argument that must also be assignable to {@link java.util.Map} for getting access to
 * all headers.</li>
 * <li>{@link org.springframework.messaging.MessageHeaders} arguments for getting access to all headers.</li>
 * <li>{@link org.springframework.messaging.support.MessageHeaderAccessor} or
 * {@link org.springframework.amqp.support.AmqpMessageHeaderAccessor}
 * for convenient access to all method arguments.</li>
 * <li>{@link org.springframework.amqp.core.AmqpAcknowledgment} - a convenient abstraction for manual acks.</li>
 * <li>{@link org.apache.qpid.protonj2.client.Delivery} - native ProtonJ delivery container for low-level API.</li>
 * </ul>
 * <p>
 * Annotated methods may have a non {@code void} return type. When they do, the result of
 * the method invocation is sent as a reply to the queue defined by the
 * {@link org.springframework.amqp.core.MessageProperties#getReplyTo() ReplyTo} header of
 * the incoming message. When this value is not set, a default queue can be provided by
 * adding @{@link org.springframework.messaging.handler.annotation.SendTo} to the
 * method declaration.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see org.springframework.amqp.client.config.EnableAmqp
 * @see AmqpListenerAnnotationBeanPostProcessor
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@Repeatable(AmqpListeners.class)
public @interface AmqpListener {

	/**
	 * The unique identifier of the container managing for this endpoint.
	 * <p>If none is specified, an auto-generated one is provided.
	 * @return the {@code id} for the container managing for this endpoint.
	 */
	String id() default "";

	/**
	 * The bean name of the
	 * {@link org.springframework.amqp.client.config.MethodAmqpMessageListenerContainerFactory} to
	 * use to create the message listener container responsible to serve this endpoint.
	 * <p>
	 * If not specified, the default container factory is used, if any.
	 * If a SpEL expression is provided ({@code #{...}}), the expression can either evaluate to a
	 * container factory instance or a bean name.
	 * @return the {@link org.springframework.amqp.client.config.MethodAmqpMessageListenerContainerFactory} bean name.
	 */
	String containerFactory() default "";

	/**
	 * The AMQP 1.0 addresses to listen to.
	 * The entries can be 'queue name', 'property-placeholder keys' or 'expressions'.
	 * Each expression must be resolved to the address.
	 * The addresses must exist.
	 * @return the addresses or expressions (SpEL) for the target listener container to listen from.
	 * @see org.springframework.amqp.core.MessageListenerContainer
	 */
	String[] addresses() default {};

	/**
	 * The initial number of credits to grant to the AMQP receiver.
	 * Can be SpEL expression or properties placeholder-based.
	 * Evaluates to {@code int}.
	 * @return the initial number of credits to grant to the AMQP receiver.
	 */
	String initialCredits() default "";

	/**
	 * {@code true} to cause exceptions thrown by the listener to be sent to the sender
	 * using normal {@code replyTo/@SendTo} semantics.
	 * When {@code false}, the exception is thrown to the listener container
	 * and retry/DLQ processing could be performed on the AMQP broker side.
	 * Overrides the default set by the listener container factory.
	 * @return {@code true} to return exceptions. If the client side uses a
	 * {@code RemoteInvocationAwareMessageConverterAdapter} the exception will be re-thrown.
	 * Otherwise, the sender will receive a {@code RemoteInvocationResult} wrapping the
	 * exception.
	 */
	String returnExceptions() default "";

	/**
	 * Set the default behavior for messages rejection.
	 * Overrides the default set by the listener container factory.
	 * Can be SpEL or properties placeholder-based; evaluates to {@code boolean}.
	 * @return the default flag for messages rejection.
	 */
	String defaultRequeueRejected() default "";

	/**
	 * Set an
	 * {@link org.springframework.amqp.client.listener.AmqpListenerErrorHandler} to
	 * invoke if the listener method throws an exception.
	 * Overrides the default set by the listener container factory.
	 * A simple String representing the bean name.
	 * If a SpEL expression (#{...}) is provided, the expression must evaluate to a bean name or a
	 * {@link org.springframework.amqp.client.listener.AmqpListenerErrorHandler} instance.
	 * @return the error handler.
	 */
	String errorHandler() default "";

	/**
	 * Set the concurrency of the listener container for this listener. Overrides the
	 * default set by the listener container factory. Maps to the concurrency setting of
	 * the container type.
	 * @return the concurrency.
	 */
	String concurrency() default "";

	/**
	 * Set to {@code false} to not auto start, to override the default setting in the container factory.
	 * @return {@code false} to not auto start.
	 */
	String autoStartup() default "";

	/**
	 * Set the task executor bean name to use for the listener container; overrides any
	 * executor set on the container factory.
	 * If a SpEL expression is provided ({@code #{...}}), the expression can either evaluate
	 * to an executor instance or a bean name.
	 * @return the executor bean name.
	 */
	String executor() default "";

	/**
	 * Override the container factory {@code autoAccept} property.
	 * If a SpEL expression is provided, it must evaluate to a {@code boolean}.
	 * If the listener method is asynchronous ({@link java.util.concurrent.CompletableFuture},
	 * {@code reactor.core.publisher.Mono} or Kotlin {@code suspend} function),
	 * this option is overridden to {@code false} by the target
	 * {@link org.springframework.amqp.client.listener.AmqpMessageListenerContainer}.
	 * @return the autoAccept flag.
	 */
	String autoAccept() default "";

	/**
	 * The bean name of a {@link org.springframework.amqp.listener.adapter.ReplyPostProcessor} to post
	 * process a response before it is sent.
	 * If a SpEL expression is provided ({@code #{...}}),
	 * the expression can either evaluate to a post-processor instance or a bean name.
	 * @return the bean name.
	 * @see org.springframework.amqp.client.listener.AmqpMessagingListenerAdapter#setReplyPostProcessor(org.springframework.amqp.listener.adapter.ReplyPostProcessor)
	 */
	String replyPostProcessor() default "";

	/**
	 * Override the container factory's message converter used for this listener.
	 * If a SpEL expression is provided ({@code #{...}}),
	 * the expression can either evaluate to a converter instance or a bean name.
	 * @return the message converter bean name.
	 */
	String messageConverter() default "";

	/**
	 * Override the container factory's {@code headerMapper} used for this listener.
	 * If a SpEL expression is provided ({@code #{...}}),
	 * the expression can either evaluate to a header mapper instance or a bean name.
	 * @return the header mapper bean name.
	 */
	String headerMapper() default "";

	/**
	 * Used to set the content-type of a reply message. Useful when used in conjunction
	 * with message converters that can handle multiple content types, such as the
	 * {@link org.springframework.amqp.support.converter.ContentTypeDelegatingMessageConverter}.
	 * SpEL expressions and property placeholders are supported. Also, useful for
	 * controlling the final content type property when used with certain converters.
	 * This does not apply when the return type is {@link org.springframework.amqp.core.Message} or
	 * {@link org.springframework.messaging.Message}; the content-type must be set
	 * on the respective message property or header, in those cases.
	 * @return the content type.
	 */
	String replyContentType() default "";

	/**
	 * The timeout for deliveries from the broker in the target AMQP consumer.
	 * Can be SpEL expression or properties placeholder-based.
	 * Evaluates to {@code long} representing milliseconds; or ISO 8601 {@link java.time.Duration} format.
	 * @return the timeout for deliveries from the broker in the target AMQP consumer.
	 */
	String receiveTimeout() default "";

	/**
	 * A duration for how long to wait for all the consumers to shut down successfully on listener container stop.
	 * Can be SpEL expression or properties placeholder-based.
	 * Evaluates to {@code long} representing milliseconds; or ISO 8601 {@link java.time.Duration} format.
	 * @return the duration for how long to wait for all the consumers to shut down.
	 */
	String gracefulShutdownPeriod() default "";

	/**
	 * The {@link org.aopalliance.aop.Advice} bean names to apply to the listener container.
	 * @return the {@link org.aopalliance.aop.Advice} bean names to apply to the listener container.
	 * @see org.springframework.amqp.client.listener.AmqpMessageListenerContainer#setAdviceChain(org.aopalliance.aop.Advice...)
	 */
	String[] adviceChain() default {};

}
