/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Represents an argument used when declaring queues etc within a
 * {@code QueueBinding}.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Argument {

	/**
	 * Return the argument name.
	 * @return the argument name.
	 */
	String name();

	/**
	 * The argument value, an empty string is translated to {@code null} for example
	 * to represent a present header test for a headers exchange.
	 * @return the argument value.
	 */
	String value() default "";

	/**
	 * Return the argument value.
	 * @return the type of the argument value.
	 */
	String type() default "java.lang.String";

}
