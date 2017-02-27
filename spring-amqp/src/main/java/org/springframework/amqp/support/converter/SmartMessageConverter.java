/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.amqp.support.converter;

import org.springframework.amqp.core.Message;

/**
 * An extended {@link MessageConverter} SPI with conversion hint support.
 *
 * <p>In case of a conversion hint being provided, the framework will call
 * these extended methods if a converter implements this interface, instead
 * of calling the regular {@code fromMessage} / {@code toMessage} variants.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public interface SmartMessageConverter extends MessageConverter {

	/**
	 * A variant of {@link #fromMessage(Message)} which takes an extra
	 * conversion context as an argument.
	 * @param message the input message.
	 * @param conversionHint an extra object passed to the {@link MessageConverter}.
	 * @return the result of the conversion, or {@code null} if the converter cannot
	 * perform the conversion.
	 * @throws MessageConversionException if the conversion fails.
	 * @see #fromMessage(Message)
	 */
	Object fromMessage(Message message, Object conversionHint) throws MessageConversionException;

}
