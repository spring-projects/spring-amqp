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

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.AmqpException;

/**
 * Thrown when a blocking receive operation is performed but the consumeOk
 * was not received before the receive timeout. Consider increasing the
 * receive timeout.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@SuppressWarnings("serial")
public class ConsumeOkNotReceivedException extends AmqpException {

	public ConsumeOkNotReceivedException(String message) {
		super(message);
	}

}
