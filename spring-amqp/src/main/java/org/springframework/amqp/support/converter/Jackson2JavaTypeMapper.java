/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.support.converter;

import com.fasterxml.jackson.databind.JavaType;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.MessageProperties;

/**
 * Strategy for setting metadata on messages such that one can create the class that needs
 * to be instantiated when receiving a message.
 *
 * @author Mark Pollack
 * @author James Carr
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Gary Russell
 *
 * @deprecated since 4.0 in favor of {@link JacksonJavaTypeMapper} for Jackson 3.
 */
@Deprecated(forRemoval = true, since = "4.0")
public interface Jackson2JavaTypeMapper extends ClassMapper {

	/**
	 * The precedence for type conversion - inferred from the method parameter or message
	 * headers. Only applies if both exist.
	 * @since 1.6
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
	 * @since 1.6
	 */
	TypePrecedence getTypePrecedence();

	/**
	 * Add trusted packages.
	 * @param packages the packages.
	 * @since 2.1
	 */
	default void addTrustedPackages(String... packages) {
		// no op
	}

	/**
	 * Return the inferred type, if the type precedence is inferred and the
	 * header is present.
	 * @param properties the message properties.
	 * @return the type.
	 * @since 2.2
	 */
	@Nullable
	JavaType getInferredType(MessageProperties properties);

}
