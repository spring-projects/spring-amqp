/*
 * Copyright 2026-present the original author or authors.
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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.AmqpAcknowledgment;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.core.MethodParameter;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.TypeUtils;

/**
 * The {@link MessagingMessageConverter} extension for message listeners with flexible method signature.
 * <p>
 * If the inbound message has no type information and the configured message converter
 * supports it, this converter attempts to infer the conversion type from the method signature
 * and populates the {@link MessageProperties#setInferredArgumentType(Type)} before calling
 * {@link #extractPayload(org.springframework.amqp.core.Message)} of super class.
 * <p>
 * If an instance is created without the bean method to infer the type from, it acts as a super class.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class MessagingMessageConverterAdapter extends MessagingMessageConverter {

	private static final LogAccessor LOGGER = new LogAccessor(MessagingMessageConverterAdapter.class);

	private final List<Type> providedArgumentTypes = new ArrayList<>(4);

	{
		this.providedArgumentTypes.add(org.springframework.amqp.core.Message.class);
		this.providedArgumentTypes.add(MessageProperties.class);
		this.providedArgumentTypes.add(AmqpAcknowledgment.class);
	}

	private final @Nullable Object bean;

	private final @Nullable Method method;

	private final @Nullable Type inferredArgumentType;

	private final boolean isBatch;

	private boolean isMessageList;

	private boolean isAmqpMessageList;

	private boolean isCollection;

	/**
	 * Construct an instance based on the bean and its method to infer the payload type.
	 * @param bean the bean for method.
	 * @param method the method to infer the payload type.
	 * @param batch if the method is a batch listener.
	 * @param providedArgumentTypes additional argument types to consider as provided without conversion.
	 */
	@SuppressWarnings("this-escape")
	public MessagingMessageConverterAdapter(@Nullable Object bean, @Nullable Method method, boolean batch,
			Class<?>... providedArgumentTypes) {

		this.bean = bean;
		this.method = method;
		this.isBatch = batch;
		this.providedArgumentTypes.addAll(Arrays.asList(providedArgumentTypes));
		this.inferredArgumentType = determineInferredType();
		if (this.inferredArgumentType != null) {
			LOGGER.debug(() -> "Inferred argument type for " + method + " is " + this.inferredArgumentType);
		}
	}

	public boolean isMessageList() {
		return this.isMessageList;
	}

	public boolean isAmqpMessageList() {
		return this.isAmqpMessageList;
	}

	public @Nullable Method getMethod() {
		return this.method;
	}

	private @Nullable Type determineInferredType() {
		if (this.method == null) {
			return null;
		}

		Type genericParameterType = null;

		for (int i = 0; i < this.method.getParameterCount(); i++) {
			MethodParameter methodParameter = new MethodParameter(this.method, i);
			/*
			 * We're looking for a single parameter, or one annotated with @Payload.
			 * We ignore parameters with type Message because they are not involved with conversion.
			 */
			boolean isHeaderOrHeaders = methodParameter.hasParameterAnnotation(Header.class)
					|| methodParameter.hasParameterAnnotation(Headers.class)
					|| methodParameter.getParameterType().equals(MessageHeaders.class);
			boolean isPayload = methodParameter.hasParameterAnnotation(Payload.class);
			if (isHeaderOrHeaders && isPayload) {
				LOGGER.warn(() -> this.method.getName()
						+ ": Cannot annotate a parameter with both @Header and @Payload; "
						+ "ignored for payload conversion");
			}
			if (isEligibleParameter(methodParameter) && !isHeaderOrHeaders) {

				if (genericParameterType == null) {
					genericParameterType = extractGenericParameterTypeFromMethodParameter(methodParameter);
					if (this.isBatch && !this.isCollection) {
						throw new IllegalStateException(
								"Mis-configuration; a batch listener must consume a List<?> or "
										+ "Collection<?> for method: " + this.method);
					}

				}
				else {
					LOGGER.debug(() -> "Ambiguous parameters for target payload for method " + this.method
							+ "; no inferred type header added");
					return null;
				}
			}
		}
		return checkOptional(genericParameterType);
	}

	/*
	 * Don't consider parameter types that are available after conversion:
	 * Message, Message<?>, AmqpAcknowledgment etc.
	 */
	private boolean isEligibleParameter(MethodParameter methodParameter) {
		Type parameterType = methodParameter.getGenericParameterType();
		if (this.providedArgumentTypes.contains(parameterType)
				|| parameterType.getTypeName().startsWith("kotlin.coroutines.Continuation")) {

			return false;
		}

		if (parameterType instanceof ParameterizedType parameterizedType &&
				parameterizedType.getRawType().equals(Message.class)) {

			return !(parameterizedType.getActualTypeArguments()[0] instanceof WildcardType);
		}

		return !parameterType.equals(Message.class); // could be Message without a generic type
	}

	private Type extractGenericParameterTypeFromMethodParameter(MethodParameter methodParameter) {
		Type genericParameterType = methodParameter.getGenericParameterType();
		if (genericParameterType instanceof ParameterizedType parameterizedType) {
			if (parameterizedType.getRawType().equals(Message.class)) {

				genericParameterType = parameterizedType.getActualTypeArguments()[0];
			}
			else if (this.isBatch &&
					(parameterizedType.getRawType().equals(List.class) ||
							(parameterizedType.getRawType().equals(Collection.class) &&
									parameterizedType.getActualTypeArguments().length == 1))) {

				this.isCollection = true;
				Type paramType = parameterizedType.getActualTypeArguments()[0];
				boolean messageHasGeneric =
						paramType instanceof ParameterizedType pType
								&& pType.getRawType().equals(Message.class);
				this.isMessageList = TypeUtils.isAssignable(paramType, Message.class) || messageHasGeneric;
				this.isAmqpMessageList =
						TypeUtils.isAssignable(paramType, org.springframework.amqp.core.Message.class);
				if (messageHasGeneric) {
					genericParameterType = ((ParameterizedType) paramType).getActualTypeArguments()[0];
				}
				else {
					// when decoding batch messages, we convert to the List's generic type
					genericParameterType = paramType;
				}
			}
		}
		return genericParameterType;
	}

	@Override
	public Object extractPayload(org.springframework.amqp.core.Message message) {
		MessageProperties messageProperties = message.getMessageProperties();
		if (this.bean != null) {
			messageProperties.setTargetBean(this.bean);
		}
		if (this.method != null) {
			messageProperties.setTargetMethod(this.method);
			if (this.inferredArgumentType != null) {
				messageProperties.setInferredArgumentType(this.inferredArgumentType);
			}
		}
		return super.extractPayload(message);
	}

	private static @Nullable Type checkOptional(@Nullable Type genericParameterType) {
		if (genericParameterType instanceof ParameterizedType pType && pType.getRawType().equals(Optional.class)) {
			return pType.getActualTypeArguments()[0];
		}
		return genericParameterType;
	}

}
