/*
 * Copyright 2015-2017 the original author or authors.
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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.core.annotation.AliasFor;

/**
 * An exchange to which to bind a {@code RabbitListener} queue.
 *
 * @author Gary Russell
 * @author Alex Panchenko
 *
 * @since 1.5
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Exchange {

	/**
	 * @return the exchange name.
	 */
	@AliasFor("name")
	String value() default "";

	/**
	 * @return the exchange name.
	 * @since 2.0
	 */
	@AliasFor("value")
	String name() default "";

	/**
	 * The exchange type, including custom.
	 * Defaults to {@link ExchangeTypes#DIRECT}.
	 * If a custom exchange type is used the corresponding plugin is required on the broker.
	 * @return the exchange type.
	 * @see ExchangeTypes
	 */
	String type() default ExchangeTypes.DIRECT;

	/**
	 * @return false if the exchange is to be declared as non-durable.
	 */
	String durable() default "true";

	/**
	 * @return true if the exchange is to be declared as auto-delete.
	 */
	String autoDelete() default "false";

	/**
	 * @return true if the exchange is to be declared as internal.
	 * @since 1.6
	 */
	String internal() default "false";

	/**
	 * @return true if the declaration exceptions should be ignored.
	 * @since 1.6
	 */
	String ignoreDeclarationExceptions() default "false";

	/**
	 * @return true if the exchange is to be declared as an
	 * 'x-delayed-message' exchange. Requires the delayed message exchange
	 * plugin on the broker.
	 * @since 1.6.4
	 */
	String delayed() default "false";

	/**
	 * @return the arguments to apply when declaring this exchange.
	 * @since 1.6
	 */
	Argument[] arguments() default {};

}
