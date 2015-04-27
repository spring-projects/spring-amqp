/*
 * Copyright 2015 the original author or authors.
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

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.springframework.amqp.core.ExchangeTypes;

/**
 * An exchange to which to bind a {@code RabbitListener} queue.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
@Target({})
@Retention(RUNTIME)
public @interface Exchange {

	/**
	 * @return the exchange name.
	 */
	String value();

	/**
	 * The exchange type - only DIRECT, FANOUT and TOPIC exchanges are supported.
	 * @return the exchange type.
	 */
	String type() default ExchangeTypes.DIRECT;

	/**
	 * @return true if the exchange is to be declared as durable.
	 */
	String durable() default "false";

	/**
	 * @return true if the exchange is to be declared as auto-delete.
	 */
	String autoDelete() default "false";

}
