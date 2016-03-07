/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.context.annotation.Import;

/**
 * Annotate a {@code @Configuration} class with this to enable proxying
 * {@code @RabbitListener} beans to capture arguments and result (if any).
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@EnableRabbit
@Import(RabbitListenerTestBootstrap.class)
public @interface RabbitListenerTest {

	/**
	 * Set to true to create a Mockito spy on the listener.
	 * @return true to create the spy; default true.
	 */
	boolean spy() default true;

	/**
	 * Set to true to advise the listener with a capture advice,
	 * @return true to advise the listener; default false.
	 */
	boolean capture() default false;

}
