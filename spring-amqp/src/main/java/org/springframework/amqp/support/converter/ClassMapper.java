/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
