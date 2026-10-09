/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.context.annotation.Import;

/**
 * Annotate a {@code @Configuration} class with this to enable proxying
 * {@code @RabbitListener} beans to capture arguments and result (if any).
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.6
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@EnableRabbit
@Import(RabbitListenerTestSelector.class)
public @interface RabbitListenerTest {

	/**
	 * Set to true to create a Mockito spy on the listener.
	 * @return true to create the spy; default true.
	 */
	boolean spy() default true;

	/**
	 * Set to true to advise the listener with a capture advice,
	 * @return true to advise the listener; default false.
	 */
	boolean capture() default false;

}
