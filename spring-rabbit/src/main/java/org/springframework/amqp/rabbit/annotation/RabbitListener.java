/*
 * Copyright 2014-2018 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be the target of a Rabbit message listener on the
 * specified {@link #queues()} (or {@link #bindings()}). The {@link #containerFactory()}
 * identifies the
 * {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory
 * RabbitListenerContainerFactory} to use to build the rabbit listener container. If not
 * set, a <em>default</em> container factory is assumed to be available with a bean name
 * of {@code rabbitListenerContainerFactory} unless an explicit default has been provided
 * through configuration.
 *
 * <p>
 * Processing of {@code @RabbitListener} annotations is performed by registering a
 * {@link RabbitListenerAnnotationBeanPostProcessor}. This can be done manually or, more
 * conveniently, through the {@code <rabbit:annotation-driven/>} element or
 * {@link EnableRabbit} annotation.
 *
 * <p>
 * Annotated methods are allowed to have flexible signatures similar to what
 * {@link MessageMapping} provides, that is
 * <ul>
 * <li>{@link com.rabbitmq.client.Channel} to get access to the Channel</li>
 * <li>{@link org.springframework.amqp.core.Message} or one if subclass to get access to
 * the raw AMQP message</li>
 * <li>{@link org.springframework.messaging.Message} to use the messaging abstraction
 * counterpart</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Payload @Payload}-annotated
 * method arguments including the support of validation</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Header @Header}-annotated
 * method arguments to extract a specific header value, including standard AMQP headers
 * defined by {@link org.springframework.amqp.support.AmqpHeaders AmqpHeaders}</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Headers @Headers}-annotated
 * argument that must also be assignable to {@link java.util.Map} for getting access to
 * all headers.</li>
 * <li>{@link org.springframework.messaging.MessageHeaders MessageHeaders} arguments for
 * getting access to all headers.</li>
 * <li>{@link org.springframework.messaging.support.MessageHeaderAccessor
 * MessageHeaderAccessor} or
 * {@link org.springframework.amqp.support.AmqpMessageHeaderAccessor
 * AmqpMessageHeaderAccessor} for convenient access to all method arguments.</li>
 * </ul>
 *
 * <p>
 * Annotated methods may have a non {@code void} return type. When they do, the result of
 * the method invocation is sent as a reply to the queue defined by the
 * {@link org.springframework.amqp.core.MessageProperties#getReplyTo() ReplyTo} header of
 * the incoming message. When this value is not set, a default queue can be provided by
 * adding @{@link org.springframework.messaging.handler.annotation.SendTo SendTo} to the
 * method declaration.
 *
 * <p>
 * When {@link #bindings()} are provided, and the application context contains a
 * {@link org.springframework.amqp.rabbit.core.RabbitAdmin}, the queue, exchange and
 * binding will be automatically declared.
 *
 * <p>When defined at the method level, a listener container is created for each method.
 * The {@link org.springframework.amqp.core.MessageListener} is a
 * {@link org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter},
 * configured with a
 * {@link org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint}.
 *
 * <p>When defined at the class level, a single message listener container is used to
 * service all methods annotated with {@code @RabbitHandler}. Method signatures of such
 * annotated methods must not cause any ambiguity such that a single method can be
 * resolved for a particular inbound message. The
 * {@link org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter}
 * is configured with a
 * {@link org.springframework.amqp.rabbit.listener.MultiMethodRabbitListenerEndpoint}.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 * @see EnableRabbit
 * @see RabbitListenerAnnotationBeanPostProcessor
 * @see RabbitListeners
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@Repeatable(RabbitListeners.class)
public @interface RabbitListener {

	/**
	 * The unique identifier of the container managing for this endpoint.
	 * <p>If none is specified an auto-generated one is provided.
	 * @return the {@code id} for the container managing for this endpoint.
	 * @see org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry#getListenerContainer(String)
	 */
	String id() default "";

	/**
	 * The bean name of the {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory}
	 * to use to create the message listener container responsible to serve this endpoint.
	 * <p>If not specified, the default container factory is used, if any.
	 * @return the {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory}
	 * bean name.
	 */
	String containerFactory() default "";

	/**
	 * The queues for this listener.
	 * The entries can be 'queue name', 'property-placeholder keys' or 'expressions'.
	 * Expression must be resolved to the queue name or {@code Queue} object.
	 * The queue(s) must exist, or be otherwise defined elsewhere as a bean(s) with
	 * a {@link org.springframework.amqp.rabbit.core.RabbitAdmin} in the application
	 * context.
	 * Mutually exclusive with {@link #bindings()} and {@link #queuesToDeclare()}.
	 * @return the queue names or expressions (SpEL) to listen to from target
	 * @see org.springframework.amqp.rabbit.listener.MessageListenerContainer
	 */
	String[] queues() default {};

	/**
	 * The queues for this listener.
	 * If there is a {@link org.springframework.amqp.rabbit.core.RabbitAdmin} in the
	 * application context, the queue will be declared on the broker with default
	 * binding (default exchange with the queue name as the routing key).
	 * Mutually exclusive with {@link #bindings()} and {@link #queues()}.
	 * @return the queue(s) to declare.
	 * @see org.springframework.amqp.rabbit.listener.MessageListenerContainer
	 * @since 2.0
	 */
	Queue[] queuesToDeclare() default {};

	/**
	 * When {@code true}, a single consumer in the container will have exclusive use of the
	 * {@link #queues()}, preventing other consumers from receiving messages from the
	 * queues. When {@code true}, requires a concurrency of 1. Default {@code false}.
	 * @return the {@code exclusive} boolean flag.
	 */
	boolean exclusive() default false;

	/**
	 * The priority of this endpoint. Requires RabbitMQ 3.2 or higher. Does not change
	 * the container priority by default. Larger numbers indicate higher priority, and
	 * both positive and negative numbers can be used.
	 * @return the priority for the endpoint.
	 */
	String priority() default "";

	/**
	 * Reference to a {@link org.springframework.amqp.rabbit.core.RabbitAdmin
	 * RabbitAdmin}. Required if the listener is using auto-delete
	 * queues and those queues are configured for conditional declaration. This
	 * is the admin that will (re)declare those queues when the container is
	 * (re)started. See the reference documentation for more information.
	 * @return the {@link org.springframework.amqp.rabbit.core.RabbitAdmin} bean name.
	 */
	String admin() default "";

	/**
	 * Array of {@link QueueBinding}s providing the listener's queue names, together
	 * with the exchange and optional binding information.
	 * Mutually exclusive with {@link #queues()} and {@link #queuesToDeclare()}.
	 * @return the bindings.
	 * @see org.springframework.amqp.rabbit.listener.MessageListenerContainer
	 * @since 1.5
	 */
	QueueBinding[] bindings() default {};

	/**
	 * If provided, the listener container for this listener will be added to a bean
	 * with this value as its name, of type {@code Collection<MessageListenerContainer>}.
	 * This allows, for example, iteration over the collection to start/stop a subset
	 * of containers.
	 * @return the bean name for the group.
	 * @since 1.5
	 */
	String group() default "";

	/**
	 * Set to "true" to cause exceptions thrown by the listener to be sent to the sender
	 * using normal {@code replyTo/@SendTo} semantics. When false, the exception is thrown
	 * to the listener container and normal retry/DLQ processing is performed.
	 * @return true to return exceptions. If the client side uses a
	 * {@code RemoteInvocationAwareMessageConverterAdapter} the exception will be re-thrown.
	 * Otherwise, the sender will receive a {@code RemoteInvocationResult} wrapping the
	 * exception.
	 * @since 2.0
	 */
	String returnExceptions() default "";

	/**
	 * Set an {@link org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler}
	 * to invoke if the listener method throws an exception.
	 * @return the error handler.
	 * @since 2.0
	 */
	String errorHandler() default "";

	/**
	 * Set the concurrency of the listener container for this listener. Overrides the
	 * default set by the listener container factory. Maps to the concurrency setting of
	 * the container type.
	 * <p>For a
	 * {@link org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
	 * SimpleMessageListenerContainer} if this value is a simple integer, it sets a fixed
	 * number of consumers in the {@code concurrentConsumers} property. If it is a string
	 * with the form {@code "m-n"}, the {@code concurrentConsumers} is set to {@code m}
	 * and the {@code maxConcurrentConsumers} is set to {@code n}.
	 * <p>For a
	 * {@link org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer
	 * DirectMessageListenerContainer} it sets the {@code consumersPerQueue} property.
	 * @return the concurrency.
	 * @since 2.0
	 */
	String concurrency() default "";

	/**
	 * Set to true or false, to override the default setting in the container factory.
	 * @return true to auto start, false to not auto start.
	 * @since 2.0
	 */
	String autoStartup() default "";

}
