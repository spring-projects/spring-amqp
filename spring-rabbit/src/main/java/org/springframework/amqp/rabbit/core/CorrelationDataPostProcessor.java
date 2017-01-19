/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.support.CorrelationData;

/**
 * A callback invoked immediately before publishing a message to update, replace, or
 * create correlation data for publisher confirms. Invoked after conversion and all
 * {@link org.springframework.amqp.core.MessagePostProcessor}s.
 *
 * @author Gary Russell
 * @since 1.6.7
 *
 */
@FunctionalInterface
public interface CorrelationDataPostProcessor {

	/**
	 * Update or replace the correlation data provided in the send method.
	 * @param message the message.
	 * @param correlationData the existing data (if present).
	 * @return the correlation data.
	 */
	CorrelationData postProcess(Message message, CorrelationData correlationData);

}
