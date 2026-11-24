/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
