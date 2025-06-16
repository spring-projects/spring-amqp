/*
 * Copyright 2020-present the original author or authors.
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

package org.springframework.amqp.rabbit.test.context;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Adds infrastructure beans to a Spring test context; do not use with Spring Boot since
 * it has its own auto configuration mechanism.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface SpringRabbitTest {

	/**
	 * Container type.
	 */
	enum ContainerType {
		simple, direct
	}

	/**
	 * Set the host when not using
	 * {@link org.springframework.amqp.rabbit.junit.RabbitAvailable}.
	 * @return the host.
	 */
	String host() default "localhost";

	/**
	 * Set the port when not using
	 * {@link org.springframework.amqp.rabbit.junit.RabbitAvailable}.
	 * @return the port.
	 */
	int port() default 5672;

	/**
	 * Set the user when not using
	 * {@link org.springframework.amqp.rabbit.junit.RabbitAvailable}.
	 * @return the user.
	 */
	String user() default "guest";

	/**
	 * Set the password when not using
	 * {@link org.springframework.amqp.rabbit.junit.RabbitAvailable}.
	 * @return the password.
	 */
	String password() default "guest";

	/**
	 * Set the container type to determine which container factory to configure.
	 * @return the type.
	 */
	ContainerType containerType() default ContainerType.simple;

}
