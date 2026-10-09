/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.client.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

/**
 * Enable AMQP 1.0 infrastructure beans for {@link org.springframework.amqp.client.annotation.AmqpListener}.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(AmqpDefaultConfiguration.class)
public @interface EnableAmqp {

}
