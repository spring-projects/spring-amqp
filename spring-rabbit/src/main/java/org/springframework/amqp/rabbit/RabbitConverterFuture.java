/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.amqp.rabbit;

import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.DirectReplyToMessageListenerContainer.ChannelHolder;
import org.springframework.core.ParameterizedTypeReference;

/**
 * A {@link RabbitFuture} with a return type of the template's
 * generic parameter.
 * @param <C> the type.
 *
 * @author Gary Russell
 * @since 2.4.7
 */
public class RabbitConverterFuture<C> extends RabbitFuture<C> {

	private volatile ParameterizedTypeReference<C> returnType;

	RabbitConverterFuture(String correlationId, Message requestMessage,
			BiConsumer<String, ChannelHolder> canceler,
			Function<RabbitFuture<?>, ScheduledFuture<?>> timeoutTaskFunction) {

		super(correlationId, requestMessage, canceler, timeoutTaskFunction);
	}

	public ParameterizedTypeReference<C> getReturnType() {
		return this.returnType;
	}

	public void setReturnType(ParameterizedTypeReference<C> returnType) {
		this.returnType = returnType;
	}

}
