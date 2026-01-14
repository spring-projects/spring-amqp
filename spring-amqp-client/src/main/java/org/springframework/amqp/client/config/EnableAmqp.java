/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.client.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.qpid.protonj2.client.ClientOptions;

import org.springframework.context.annotation.Import;

/**
 * Enable AMQP 1.0 infrastructure beans: {@link org.apache.qpid.protonj2.client.Client} etc.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see org.apache.qpid.protonj2.client.Client
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(AmqpDefaultConfiguration.class)
public @interface EnableAmqp {

	/**
	 * The value for the {@link org.apache.qpid.protonj2.client.ClientOptions#id()}
	 * of the {@link org.apache.qpid.protonj2.client.Client} to be created by the {@link AmqpDefaultConfiguration}.
	 * Can be specified as a property placeholder, e.g. {@code "${amqp.client-id}"}.
	 * @return the client id.
	 */
	String clientId() default "";

	/**
	 * The value for the {@link ClientOptions#futureType()}
	 * of the {@link org.apache.qpid.protonj2.client.Client} to be created by the {@link AmqpDefaultConfiguration}.
	 * Can be specified as a property placeholder, e.g. {@code "${amqp.future-type}"}.
	 * @return the future type for the client.
	 */
	String clientFutureType() default "";

}
