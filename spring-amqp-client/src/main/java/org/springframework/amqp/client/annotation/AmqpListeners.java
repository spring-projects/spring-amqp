/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Container annotation that aggregates several {@link AmqpListener} annotations.
 * <p>
 * Can be used natively, declaring several nested {@link AmqpListener} annotations.
 * Can also be used in conjunction with Java support for repeatable annotations,
 * where {@link AmqpListener} can simply be declared several times on the same method
 * (or class), implicitly generating this container annotation.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see AmqpListener
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AmqpListeners {

	AmqpListener[] value();

}
