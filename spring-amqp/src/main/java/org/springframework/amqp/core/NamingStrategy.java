/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp.core;

/**
 * A strategy to generate names.
 *
 * @author Gary Russell
 *
 * @since 2.1
 */
@FunctionalInterface
public interface NamingStrategy {

	String generateName();

}
