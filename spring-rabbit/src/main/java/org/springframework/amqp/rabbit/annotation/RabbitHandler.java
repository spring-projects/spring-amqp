/*
 * Copyright 2015-present the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be the target of a Rabbit message
 * listener within a class that is annotated with {@link RabbitListener}.
 *
 * <p>See the {@link RabbitListener} for information about permitted method signatures
 * and available parameters.
 * <p><b>It is important to understand that when a message arrives, the method selection
 * depends on the payload type. The type is matched with a single non-annotated parameter,
 * or one that is annotated with {@code @Payload}.
 * There must be no ambiguity - the system
 * must be able to select exactly one method based on the payload type.</b>
 *
 * @author Gary Russell
 * @since 1.5
 * @see EnableRabbit
 * @see RabbitListener
 * @see RabbitListenerAnnotationBeanPostProcessor
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface RabbitHandler {

	/**
	 * When true, designate that this is the default fallback method if the payload type
	 * matches no other {@link RabbitHandler} method. Only one method can be so designated.
	 * @return true if this is the default method.
	 * @since 2.0.3
	 */
	boolean isDefault() default false;

}
