/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.support.converter;

import org.springframework.amqp.core.MessageProperties;

/**
 * Strategy for setting metadata on messages such that one can create the class
 * that needs to be instantiated when receiving a message.
 *
 * @author Mark Pollack
 * @author James Carr
 *
 */
public interface ClassMapper {

	void fromClass(Class<?> clazz, MessageProperties properties);

	Class<?> toClass(MessageProperties properties);
}
