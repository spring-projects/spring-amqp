/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.support.converter;

import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;

import org.springframework.amqp.core.MessageProperties;

/**
 * Strategy for setting metadata on messages such that one can create the class that needs
 * to be instantiated when receiving a message.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
public interface JacksonJavaTypeMapper extends ClassMapper {

	/**
	 * The precedence for type conversion - inferred from the method parameter or message
	 * headers. Only applies if both exist.
	 */
	enum TypePrecedence {
		INFERRED, TYPE_ID
	}

	/**
	 * Set the message properties according to the type.
	 * @param javaType the type.
	 * @param properties the properties.
	 */
	void fromJavaType(JavaType javaType, MessageProperties properties);

	/**
	 * Determine the type from the message properties.
	 * @param properties the properties.
	 * @return the type.
	 */
	JavaType toJavaType(MessageProperties properties);

	/**
	 * Get the type precedence.
	 * @return the precedence.
	 */
	TypePrecedence getTypePrecedence();

	/**
	 * Add trusted packages.
	 * @param packages the packages.
	 */
	default void addTrustedPackages(String... packages) {
		// no op
	}

	/**
	 * Return the inferred type, if the type precedence is inferred and the
	 * header is present.
	 * @param properties the message properties.
	 * @return the type.
	 */
	@Nullable
	JavaType getInferredType(MessageProperties properties);

}
