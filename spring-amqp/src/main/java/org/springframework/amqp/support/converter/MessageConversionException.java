/*
 * Copyright 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.support.converter;

import org.springframework.amqp.AmqpException;

/**
 * <p>
 * Exception to be thrown by message converters if they encounter a problem with converting a message or object.
 * </p>
 * <p>
 * N.B. this is <em>not</em> an {@link AmqpException} because it is a a client exception, not a protocol or broker
 * problem.
 * </p>
 * 
 * @author Mark Fisher
 * @author Dave Syer
 */
@SuppressWarnings("serial")
public class MessageConversionException extends RuntimeException {

	public MessageConversionException(String message, Throwable cause) {
		super(message, cause);
	}

	public MessageConversionException(String message) {
		super(message);
	}

}
