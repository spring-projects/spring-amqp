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
	String value() default "";

	/**
	 * @return true if the queue is to be declared as durable.
	 */
	String durable() default "";

	/**
	 * @return true if the queue is to be declared as exclusive.
	 */
	String exclusive() default "";

	/**
	 * @return true if the queue is to be declared as auto-delete.
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
