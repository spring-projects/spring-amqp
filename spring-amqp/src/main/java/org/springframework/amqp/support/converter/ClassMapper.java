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
