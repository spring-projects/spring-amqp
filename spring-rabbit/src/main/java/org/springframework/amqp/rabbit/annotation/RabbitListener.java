/*
 * Copyright 2014 the original author or authors.
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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be the target of a Rabbit message
 * listener on the specified {@link #queues()} (or {@link #bindings()}).
 * The {@link #containerFactory()}
 * identifies the {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory
 * RabbitListenerContainerFactory} to use to build the rabbit listener container. If not
 * set, a <em>default</em> container factory is assumed to be available with a bean
 * name of {@code rabbitListenerContainerFactory} unless an explicit default has been
 * provided through configuration.
 *
 * <p>Processing of {@code @RabbitListener} annotations is performed by
 * registering a {@link RabbitListenerAnnotationBeanPostProcessor}. This can be
 * done manually or, more conveniently, through the {@code <rabbit:annotation-driven/>}
 * element or {@link EnableRabbit} annotation.
 *
 * <p>Annotated methods are allowed to have flexible signatures similar to what
 * {@link MessageMapping} provides, that is
 * <ul>
 * <li>{@link com.rabbitmq.client.Channel} to get access to the Channel</li>
 * <li>{@link org.springframework.amqp.core.Message} or one if subclass to get
 * access to the raw AMQP message</li>
 * <li>{@link org.springframework.messaging.Message} to use the messaging abstraction counterpart</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Payload @Payload}-annotated method
 * arguments including the support of validation</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Header @Header}-annotated method
 * arguments to extract a specific header value, including standard AMQP headers defined by
 * {@link org.springframework.amqp.support.AmqpHeaders AmqpHeaders}</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Headers @Headers}-annotated
 * argument that must also be assignable to {@link java.util.Map} for getting access to all
 * headers.</li>
 * <li>{@link org.springframework.messaging.MessageHeaders MessageHeaders} arguments for
 * getting access to all headers.</li>
 * <li>{@link org.springframework.messaging.support.MessageHeaderAccessor MessageHeaderAccessor}
 * or {@link org.springframework.amqp.support.AmqpMessageHeaderAccessor AmqpMessageHeaderAccessor}
 * for convenient access to all method arguments.</li>
 * </ul>
 *
 * <p>Annotated method may have a non {@code void} return type. When they do, the result of the
 * method invocation is sent as a reply to the queue defined by the
 * {@link org.springframework.amqp.core.MessageProperties#getReplyTo() ReplyTo}  header of the
 * incoming message. When this value is not set, a default queue can be provided by
 * adding @{@link org.springframework.messaging.handler.annotation.SendTo SendTo} to the method
 * declaration.
 *
 * <p>When {@link #bindings()} are provided, and the application context contains a
 * {@link org.springframework.amqp.rabbit.core.RabbitAdmin},
 * the queue, exchange and binding will be automatically declared.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 * @see EnableRabbit
 * @see RabbitListenerAnnotationBeanPostProcessor
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
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
	 * Mutually exclusive with {@link #bindings()}
	 * @return the queue names or expressions (SpEL) to listen to from target
	 * {@link org.springframework.amqp.rabbit.listener.MessageListenerContainer}.
	 */
	String[] queues() default {};

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
	  @return the {@link org.springframework.amqp.rabbit.core.RabbitAdmin} bean name.
	 */
	String admin() default "";

	/**
	 * Array of {@link QueueBinding}s providing the listener's queue names, together
	 * with the exchange and optional binding information.
	 * @return the bindings.
	 * @since 1.5
	 */
	QueueBinding[] bindings() default {};

}
