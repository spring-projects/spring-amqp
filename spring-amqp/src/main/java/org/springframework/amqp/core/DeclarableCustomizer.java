/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
