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

package org.springframework.amqp.client.annotation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.aopalliance.aop.Advice;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.client.config.AmqpListenerEndpointRegistry;
import org.springframework.amqp.client.config.AmqpMessageListenerContainerFactory;
import org.springframework.amqp.client.config.MethodAmqpListenerEndpoint;
import org.springframework.amqp.client.listener.AmqpListenerErrorHandler;
import org.springframework.amqp.listener.AbstractListenerAnnotationBeanPostProcessor;
import org.springframework.amqp.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.support.AmqpHeaderMapper;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.utils.JavaUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.util.ObjectUtils;

/**
 * Bean post-processor that registers methods annotated with {@link AmqpListener}
 * to be invoked by an AMQP 1.0 message listener container created under the cover
 * by a {@link org.springframework.amqp.client.config.MethodAmqpMessageListenerContainerFactory}
 * according to the parameters of the annotation.
 * <p>
 * Annotated methods can use flexible arguments as defined by {@link AmqpListener}.
 * <p>
 * This post-processor is automatically registered by Spring's
 * by the {@link org.springframework.amqp.client.config.EnableAmqp} annotation.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see org.springframework.amqp.client.config.EnableAmqp
 */
public class AmqpListenerAnnotationBeanPostProcessor extends AbstractListenerAnnotationBeanPostProcessor<AmqpListener> {

	private final Map<MethodAmqpListenerEndpoint, @Nullable AmqpMessageListenerContainerFactory> endpoints =
			new HashMap<>();

	private @Nullable AmqpListenerEndpointRegistry amqpListenerEndpointRegistry;

	@Override
	public void afterSingletonsInstantiated() {
		super.afterSingletonsInstantiated();
		this.amqpListenerEndpointRegistry = getBeanFactory().getBean(AmqpListenerEndpointRegistry.class);
		this.endpoints.forEach(this::registerListenerEndpoint);
		this.endpoints.clear();
	}

	@Override
	protected TypeMetadata<AmqpListener> buildMetadata(Class<?> clazz) {
		return buildMetadata(clazz, AmqpListener.class, null);
	}

	@Override
	protected void processMultiMethodListeners(List<AmqpListener> classLevelListeners,
			List<Method> multiMethods, Object bean, String beanName) {

		throw new UnsupportedOperationException("The multi-method '@AmqpListener' is not supported.");
	}

	@Override
	protected void doProcessAmqpListener(AmqpListener listenerAnnotation, Method method, Object bean, String beanName) {
		Method methodToUse = checkProxy(method, bean);
		String[] addresses = resolveAddresses(listenerAnnotation);
		MethodAmqpListenerEndpoint endpoint = new MethodAmqpListenerEndpoint(bean, methodToUse, addresses);
		processListener(endpoint, listenerAnnotation, methodToUse, beanName);
	}

	private String[] resolveAddresses(AmqpListener listenerAnnotation) {
		List<String> addressesToReturn = new ArrayList<>();
		String[] addresses = listenerAnnotation.addresses();
		for (String address : addresses) {
			Object resolved = resolveExpression(address);
			processAddressValue(resolved, addressesToReturn);
		}
		return addressesToReturn.toArray(new String[0]);
	}

	private void processAddressValue(@Nullable Object value, List<String> addresses) {
		if (value instanceof String addressValue) {
			if (addressValue.contains(",")) {
				processAddressValue(addressValue.split(","), addresses);
			}
			else {
				addresses.add(addressValue);
			}
		}
		else if (value instanceof String[] addressValues) {
			addresses.addAll(List.of(addressValues));
		}
		else if (value instanceof Iterable<?> iterable) {
			for (Object item : iterable) {
				processAddressValue(item, addresses);
			}
		}
		else {
			throw new IllegalArgumentException("Unsupported address value: " + value);
		}
	}

	private void processListener(MethodAmqpListenerEndpoint endpoint, AmqpListener listener,
			Method method, String beanName) {

		JavaUtils.INSTANCE
				.acceptIfHasText(resolveExpressionAsString(listener.id(), "id"), endpoint::setId)
				.acceptIfHasText(listener.returnExceptions(),
						(value) -> endpoint.setReturnExceptions(resolveExpressionAsBoolean(value)))
				.acceptIfHasText(listener.defaultRequeueRejected(),
						(value) -> endpoint.setDefaultRequeueRejected(resolveExpressionAsBoolean(value)))
				.acceptIfHasText(listener.autoStartup(),
						(value) -> endpoint.setAutoStartup(resolveExpressionAsBoolean(value)))
				.acceptIfHasText(listener.autoAccept(),
						(value) -> endpoint.setAutoAccept(resolveExpressionAsBoolean(value)))
				.acceptIfNotNull(resolveExpressiontoInteger(listener.concurrency(), "concurrency"),
						endpoint::setConcurrency)
				.acceptIfNotNull(resolveExpressiontoInteger(listener.initialCredits(), "initialCredits"),
						endpoint::setInitialCredits)
				.acceptIfHasText(resolveExpressionAsString(listener.receiveTimeout(), "receiveTimeout"),
						(value) -> endpoint.setReceiveTimeout(toDuration(value, TimeUnit.MILLISECONDS)))
				.acceptIfHasText(resolveExpressionAsString(listener.gracefulShutdownPeriod(), "gracefulShutdownPeriod"),
						(value) -> endpoint.setGracefulShutdownPeriod(toDuration(value, TimeUnit.MILLISECONDS)))
				.acceptIfHasText(resolveExpressionAsString(listener.replyContentType(), "replyContentType"),
						endpoint::setReplyContentType)
				.acceptIfNotNull(resolveExpressionToBean(listener.errorHandler(), "errorHandler", method, beanName,
						AmqpListenerErrorHandler.class), endpoint::setErrorHandler)
				.acceptIfNotNull(resolveExpressionToBean(listener.executor(), "executor", method, beanName,
						Executor.class), endpoint::setTaskExecutor)
				.acceptIfNotNull(resolveExpressionToBean(listener.headerMapper(), "headerMapper", method, beanName,
						AmqpHeaderMapper.class), endpoint::setHeaderMapper)
				.acceptIfNotNull(resolveExpressionToBean(listener.replyPostProcessor(), "replyPostProcessor", method,
						beanName, ReplyPostProcessor.class), endpoint::setReplyPostProcessor)
				.acceptIfNotNull(resolveExpressionToBean(listener.messageConverter(), "messageConverter", method,
						beanName, MessageConverter.class), endpoint::setMessageConverter);

		BeanFactory beanFactory = getBeanFactory();
		String[] adviceNamesChain = listener.adviceChain();
		if (!ObjectUtils.isEmpty(adviceNamesChain)) {
			Advice[] adviceChain =
					Arrays.stream(adviceNamesChain)
							.map((name) -> beanFactory.getBean(name, Advice.class))
							.toArray(Advice[]::new);
			endpoint.setAdviceChain(adviceChain);
		}

		AmqpMessageListenerContainerFactory containerFactory =
				resolveExpressionToBean(listener.containerFactory(), "containerFactory", method, beanName,
						AmqpMessageListenerContainerFactory.class);

		if (this.amqpListenerEndpointRegistry == null) {
			this.endpoints.put(endpoint, containerFactory);
		}
		else {
			registerListenerEndpoint(endpoint, containerFactory);
		}
	}

	@SuppressWarnings("NullAway")
	private void registerListenerEndpoint(MethodAmqpListenerEndpoint endpoint,
			@Nullable AmqpMessageListenerContainerFactory containerFactory) {

		if (containerFactory != null) {
			this.amqpListenerEndpointRegistry.registerListenerEndpoint(containerFactory, endpoint);
		}
		else {
			this.amqpListenerEndpointRegistry.registerListenerEndpoint(endpoint);
		}
	}

}
