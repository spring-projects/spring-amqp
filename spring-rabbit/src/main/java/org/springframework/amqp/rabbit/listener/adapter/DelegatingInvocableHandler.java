/*
 * Copyright 2015-2021 the original author or authors.
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

package org.springframework.amqp.rabbit.listener.adapter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.validation.Validator;


/**
 * Delegates to an {@link InvocableHandlerMethod} based on the message payload type.
 * Matches a single, non-annotated parameter or one that is annotated with
 * {@link org.springframework.messaging.handler.annotation.Payload}. Matches must be
 * unambiguous.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.5
 *
 */
public class DelegatingInvocableHandler {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private static final ParserContext PARSER_CONTEXT = new TemplateParserContext("!{", "}");

	private final List<InvocableHandlerMethod> handlers;

	private final ConcurrentMap<Class<?>, InvocableHandlerMethod> cachedHandlers = new ConcurrentHashMap<>();

	private final ConcurrentMap<InvocableHandlerMethod, MethodParameter> payloadMethodParameters =
			new ConcurrentHashMap<>();

	private final InvocableHandlerMethod defaultHandler;

	private final Map<InvocableHandlerMethod, Expression> handlerSendTo = new ConcurrentHashMap<>();

	private final Object bean;

	private final BeanExpressionResolver resolver;

	private final BeanExpressionContext beanExpressionContext;

	private final PayloadValidator validator;

	private final boolean asyncReplies;

	/**
	 * Construct an instance with the supplied handlers for the bean.
	 * @param handlers the handlers.
	 * @param bean the bean.
	 * @param beanExpressionResolver the resolver.
	 * @param beanExpressionContext the context.
	 */
	public DelegatingInvocableHandler(List<InvocableHandlerMethod> handlers, Object bean,
			BeanExpressionResolver beanExpressionResolver, BeanExpressionContext beanExpressionContext) {

		this(handlers, null, bean, beanExpressionResolver, beanExpressionContext, null);
	}

	/**
	 * Construct an instance with the supplied handlers for the bean.
	 * @param handlers the handlers.
	 * @param defaultHandler the default handler.
	 * @param bean the bean.
	 * @param beanExpressionResolver the resolver.
	 * @param beanExpressionContext the context.
	 * @since 2.0.3
	 */
	public DelegatingInvocableHandler(List<InvocableHandlerMethod> handlers,
			@Nullable InvocableHandlerMethod defaultHandler, Object bean, BeanExpressionResolver beanExpressionResolver,
			BeanExpressionContext beanExpressionContext) {
		this(handlers, defaultHandler, bean, beanExpressionResolver, beanExpressionContext, null);
	}

	/**
	 * Construct an instance with the supplied handlers for the bean.
	 * @param handlers the handlers.
	 * @param defaultHandler the default handler.
	 * @param bean the bean.
	 * @param beanExpressionResolver the resolver.
	 * @param beanExpressionContext the context.
	 * @param validator the validator.
	 * @since 2.0.3
	 */
	public DelegatingInvocableHandler(List<InvocableHandlerMethod> handlers,
			@Nullable InvocableHandlerMethod defaultHandler, Object bean, BeanExpressionResolver beanExpressionResolver,
			BeanExpressionContext beanExpressionContext, @Nullable Validator validator) {

		this.handlers = new ArrayList<>(handlers);
		this.defaultHandler = defaultHandler;
		this.bean = bean;
		this.resolver = beanExpressionResolver;
		this.beanExpressionContext = beanExpressionContext;
		this.validator = validator == null ? null : new PayloadValidator(validator);
		boolean asyncReplies;
		asyncReplies = defaultHandler != null && isAsyncReply(defaultHandler);
		Iterator<InvocableHandlerMethod> iterator = handlers.iterator();
		while (iterator.hasNext()) {
			asyncReplies |= isAsyncReply(iterator.next());
		}
		this.asyncReplies = asyncReplies;
	}

	private boolean isAsyncReply(InvocableHandlerMethod method) {
		return (AbstractAdaptableMessageListener.monoPresent && MonoHandler.isMono(method.getMethod().getReturnType()))
				|| ListenableFuture.class.isAssignableFrom(method.getMethod().getReturnType());
	}

	/**
	 * @return the bean
	 */
	public Object getBean() {
		return this.bean;
	}

	/**
	 * Return true if any handler method has an async reply type.
	 * @return the asyncReply.
	 * @since 2.2.21
	 */
	public boolean isAsyncReplies() {
		return this.asyncReplies;
	}

	/**
	 * Invoke the method with the given message.
	 * @param message the message.
	 * @param providedArgs additional arguments.
	 * @return the result of the invocation.
	 * @throws Exception raised if no suitable argument resolver can be found,
	 * or the method raised an exception.
	 */
	public InvocationResult invoke(Message<?> message, Object... providedArgs) throws Exception { // NOSONAR
		Class<? extends Object> payloadClass = message.getPayload().getClass();
		InvocableHandlerMethod handler = getHandlerForPayload(payloadClass);
		if (this.validator != null && this.defaultHandler != null) {
			MethodParameter parameter = this.payloadMethodParameters.get(handler);
			if (parameter != null && this.validator.supportsParameter(parameter)) {
				this.validator.validate(message, parameter, message.getPayload());
			}
		}
		Object result = handler.invoke(message, providedArgs);
		if (message.getHeaders().get(AmqpHeaders.REPLY_TO) == null) {
			Expression replyTo = this.handlerSendTo.get(handler);
			if (replyTo != null) {
				return new InvocationResult(result, replyTo, handler.getMethod().getGenericReturnType(),
						handler.getBean(), handler.getMethod());
			}
		}
		return new InvocationResult(result, null, handler.getMethod().getGenericReturnType(), handler.getBean(),
				handler.getMethod());
	}

	/**
	 * @param payloadClass the payload class.
	 * @return the handler.
	 */
	protected InvocableHandlerMethod getHandlerForPayload(Class<? extends Object> payloadClass) {
		InvocableHandlerMethod handler = this.cachedHandlers.get(payloadClass);
		if (handler == null) {
			handler = findHandlerForPayload(payloadClass);
			if (handler == null) {
				throw new AmqpException("No method found for " + payloadClass);
			}
			this.cachedHandlers.putIfAbsent(payloadClass, handler); //NOSONAR
			setupReplyTo(handler);
		}
		return handler;
	}

	private void setupReplyTo(InvocableHandlerMethod handler) {
		String replyTo = null;
		Method method = handler.getMethod();
		if (method != null) {
			SendTo ann = AnnotationUtils.getAnnotation(method, SendTo.class);
			replyTo = extractSendTo(method.toString(), ann);
		}
		if (replyTo == null) {
			Class<?> beanType = handler.getBeanType();
			SendTo ann = AnnotationUtils.getAnnotation(beanType, SendTo.class);
			replyTo = extractSendTo(beanType.getSimpleName(), ann);
		}
		if (replyTo != null) {
			this.handlerSendTo.put(handler, PARSER.parseExpression(replyTo, PARSER_CONTEXT));
		}
	}

	private String extractSendTo(String element, SendTo ann) {
		String replyTo = null;
		if (ann != null) {
			String[] destinations = ann.value();
			if (destinations.length > 1) {
				throw new IllegalStateException("Invalid @" + SendTo.class.getSimpleName() + " annotation on '"
						+ element + "' one destination must be set (got " + Arrays.toString(destinations) + ")");
			}
			replyTo = destinations.length == 1 ? resolve(destinations[0]) : null;
		}
		return replyTo;
	}

	private String resolve(String value) {
		if (this.resolver != null) {
			String resolvedValue = this.beanExpressionContext.getBeanFactory().resolveEmbeddedValue(value);
			Object newValue = this.resolver.evaluate(resolvedValue, this.beanExpressionContext);
			Assert.isInstanceOf(String.class, newValue, "Invalid @SendTo expression");
			return (String) newValue;
		}
		else {
			return value;
		}
	}

	protected InvocableHandlerMethod findHandlerForPayload(Class<? extends Object> payloadClass) {
		InvocableHandlerMethod result = null;
		for (InvocableHandlerMethod handler : this.handlers) {
			if (matchHandlerMethod(payloadClass, handler)) {
				if (result != null) {
					boolean resultIsDefault = result.equals(this.defaultHandler);
					if (!handler.equals(this.defaultHandler) && !resultIsDefault) {
						throw new AmqpException("Ambiguous methods for payload type: " + payloadClass + ": " +
								result.getMethod().getName() + " and " + handler.getMethod().getName());
					}
					if (!resultIsDefault) {
						continue; // otherwise replace the result with the actual match
					}
				}
				result = handler;
			}
		}
		return result != null ? result : this.defaultHandler;
	}

	protected boolean matchHandlerMethod(Class<? extends Object> payloadClass, InvocableHandlerMethod handler) {
		Method method = handler.getMethod();
		Annotation[][] parameterAnnotations = method.getParameterAnnotations();
		// Single param; no annotation or not @Header
		if (parameterAnnotations.length == 1) {
			MethodParameter methodParameter = new MethodParameter(method, 0);
			if ((methodParameter.getParameterAnnotations().length == 0
					|| !methodParameter.hasParameterAnnotation(Header.class))
						&& methodParameter.getParameterType().isAssignableFrom(payloadClass)) {
				if (this.validator != null) {
					this.payloadMethodParameters.put(handler, methodParameter);
				}
				return true;
			}
		}
		return findACandidate(payloadClass, handler, method, parameterAnnotations);
	}

	private boolean findACandidate(Class<? extends Object> payloadClass, InvocableHandlerMethod handler, Method method,
			Annotation[][] parameterAnnotations) {

		boolean foundCandidate = false;
		for (int i = 0; i < parameterAnnotations.length; i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			if ((methodParameter.getParameterAnnotations().length == 0
					|| !methodParameter.hasParameterAnnotation(Header.class))
						&& methodParameter.getParameterType().isAssignableFrom(payloadClass)) {
				if (foundCandidate) {
					throw new AmqpException("Ambiguous payload parameter for " + method.toGenericString());
				}
				if (this.validator != null) {
					this.payloadMethodParameters.put(handler, methodParameter);
				}
				foundCandidate = true;
			}
		}
		return foundCandidate;
	}

	/**
	 * Return a string representation of the method that will be invoked for this payload.
	 * @param payload the payload.
	 * @return the method name.
	 */
	public String getMethodNameFor(Object payload) {
		InvocableHandlerMethod handlerForPayload = null;
		try {
			handlerForPayload = getHandlerForPayload(payload.getClass());
		}
		catch (Exception e) {
			// NOSONAR
		}
		return handlerForPayload == null ? "no match" : handlerForPayload.getMethod().toGenericString();
	}

	/**
	 * Return the method that will be invoked for this payload.
	 * @param payload the payload.
	 * @return the method.
	 * @since 2.0
	 */
	public Method getMethodFor(Object payload) {
		return getHandlerForPayload(payload.getClass()).getMethod();
	}

	public boolean hasDefaultHandler() {
		return this.defaultHandler != null;
	}

	@Nullable
	public InvocationResult getInvocationResultFor(Object result, Object inboundPayload) {
		InvocableHandlerMethod handler = findHandlerForPayload(inboundPayload.getClass());
		if (handler != null) {
			return new InvocationResult(result, this.handlerSendTo.get(handler),
					handler.getMethod().getGenericReturnType(), handler.getBean(), handler.getMethod());
		}
		return null;
	}

	private static final class PayloadValidator extends PayloadMethodArgumentResolver {

		PayloadValidator(Validator validator) {
			super(new MessageConverter() { // Required but never used

				@Override
				@Nullable
				public Message<?> toMessage(Object payload, @Nullable
						MessageHeaders headers) {
					return null;
				}

				@Override
				@Nullable
				public Object fromMessage(Message<?> message, Class<?> targetClass) {
					return null;
				}

			}, validator);
		}

		@Override
		public void validate(Message<?> message, MethodParameter parameter, Object target) { // NOSONAR - public
			super.validate(message, parameter, target);
		}

	}
}
