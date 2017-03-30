/*
 * Copyright 2015-2016 the original author or authors.
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

import org.springframework.core.annotation.AliasFor;

/**
 * A queue definition used within the bindings attribute of a {@code QueueBinding}.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Queue {

	/**
	 * @return the queue name or "" for a generated queue name (default).
	 */
	@AliasFor("name")
	String value() default "";

	/**
	 * @return the queue name or "" for a generated queue name (default).
	 * @since 2.0
	 */
	@AliasFor("value")
	String name() default "";

	/**
	 * Specifies if this queue should be durable.
	 * By default if queue name is provided it is durable.
	 * @return true if the queue is to be declared as durable.
	 * @see org.springframework.amqp.core.Queue#isDurable()
	 */
	String durable() default "";

	/**
	 * Specifies if this queue should be exclusive.
	 * By default it queue name is provided it is not exclusive.
	 * @return true if the queue is to be declared as exclusive.
	 * @see org.springframework.amqp.core.Queue#isExclusive()
	 */
	String exclusive() default "";

	/**
	 * Specifies if this queue should be auto deleted when not used.
	 * By default it queue name is provided it is not auto-deleted.
	 * @return true if the queue is to be declared as auto-delete.
	 * @see org.springframework.amqp.core.Queue#isAutoDelete()
	 */
	String autoDelete() default "";

	/**
	 * @return true if the declaration exceptions should be ignored.
	 * @since 1.6
	 */
	String ignoreDeclarationExceptions() default "false";

	/**
	 * @return the arguments to apply when declaring this queue.
	 * @since 1.6
	 */
	Argument[] arguments() default {};

}
