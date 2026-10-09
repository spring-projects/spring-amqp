/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.core;

import java.util.function.Function;

/**
 * Beans of this type are invoked by the {@link AmqpAdmin} before declaring the
 * {@link Declarable}, allowing customization thereof.
 *
 * @author Gary Russell
 * @since 2.2.2
 *
 */
@FunctionalInterface
public interface DeclarableCustomizer extends Function<Declarable, Declarable> {

}
