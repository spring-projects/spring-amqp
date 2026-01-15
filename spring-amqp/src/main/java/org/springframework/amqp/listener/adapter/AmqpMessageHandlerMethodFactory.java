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

package org.springframework.amqp.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.core.KotlinDetector;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolverComposite;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.validation.BindingResult;
import org.springframework.validation.ObjectError;
import org.springframework.validation.Validator;

/**
 * Extension of the {@link DefaultMessageHandlerMethodFactory} for Spring AMQP requirements.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public class AmqpMessageHandlerMethodFactory extends DefaultMessageHandlerMethodFactory {

	private final HandlerMethodArgumentResolverComposite argumentResolvers =
			new HandlerMethodArgumentResolverComposite();

	@SuppressWarnings("NullAway.Init")
	private MessageConverter messageConverter;

	private @Nullable Validator validator;

	@Override
	public void setMessageConverter(MessageConverter messageConverter) {
		super.setMessageConverter(messageConverter);
		this.messageConverter = messageConverter;
	}

	@Override
	public void setValidator(Validator validator) {
		super.setValidator(validator);
		this.validator = validator;
	}

	@Override
	protected List<HandlerMethodArgumentResolver> initArgumentResolvers() {
		List<HandlerMethodArgumentResolver> resolvers = super.initArgumentResolvers();
		if (KotlinDetector.isKotlinPresent()) {
			// Insert before PayloadMethodArgumentResolver
			resolvers.add(resolvers.size() - 1, new ContinuationHandlerMethodArgumentResolver());
		}
		// Has to be at the end, but before PayloadMethodArgumentResolver
		resolvers.add(resolvers.size() - 1,
				new OptionalEmptyAwarePayloadArgumentResolver(this.messageConverter, this.validator));
		this.argumentResolvers.addResolvers(resolvers);
		return resolvers;
	}

	@Override
	public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
		InvocableHandlerMethod handlerMethod = new KotlinAwareInvocableHandlerMethod(bean, method);
		handlerMethod.setMessageMethodArgumentResolvers(this.argumentResolvers);
		return handlerMethod;
	}

	private static class OptionalEmptyAwarePayloadArgumentResolver extends PayloadMethodArgumentResolver {

		OptionalEmptyAwarePayloadArgumentResolver(MessageConverter messageConverter, @Nullable Validator validator) {
			super(messageConverter, validator);
		}

		@Override
		public @Nullable Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception {
			Object resolved;
			try {
				resolved = super.resolveArgument(parameter, message);
			}
			catch (MethodArgumentNotValidException ex) {
				Type type = parameter.getGenericParameterType();
				if (isOptional(message, type)) {
					BindingResult bindingResult = ex.getBindingResult();
					if (bindingResult != null) {
						List<ObjectError> allErrors = bindingResult.getAllErrors();
						if (allErrors.size() == 1) {
							String defaultMessage = allErrors.get(0).getDefaultMessage();
							if ("Payload value must not be empty".equals(defaultMessage)) {
								return Optional.empty();
							}
						}
					}
				}
				throw ex;
			}
			/*
			 * Replace Optional.empty() list elements with null.
			 */
			if (resolved instanceof List<?> list) {
				for (int i = 0; i < list.size(); i++) {
					if (list.get(i).equals(Optional.empty())) {
						list.set(i, null);
					}
				}
			}
			return resolved;
		}

		private boolean isOptional(Message<?> message, Type type) {
			return (Optional.class.equals(type) ||
					(type instanceof ParameterizedType pType && Optional.class.equals(pType.getRawType())))
					&& message.getPayload().equals(Optional.empty());
		}

		@Override
		protected boolean isEmptyPayload(@Nullable Object payload) {
			return payload == null || payload.equals(Optional.empty());
		}

	}

}
