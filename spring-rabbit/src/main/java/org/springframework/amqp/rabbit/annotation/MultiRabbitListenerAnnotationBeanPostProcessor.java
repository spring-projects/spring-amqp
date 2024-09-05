/*
 * Copyright 2020-2024 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;

import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringUtils;

/**
 * An extension of {@link RabbitListenerAnnotationBeanPostProcessor} that indicates the proper
 * RabbitAdmin bean to be used when processing to the listeners, and also associates it to the
 * declarables (Exchanges, Queues, and Bindings) returned.
 * <p>
 * This processing restricts the {@link org.springframework.amqp.rabbit.core.RabbitAdmin} according to the related
 * configuration, preventing the server from automatic binding non-related structures.
 *
 * @author Wander Costa
 *
 * @since 2.3
 */
public class MultiRabbitListenerAnnotationBeanPostProcessor extends RabbitListenerAnnotationBeanPostProcessor {

	private ExpressionParser expressionParser = new SpelExpressionParser();

	private ParserContext templateParserContext = new TemplateParserContext();

	@Override
	protected Collection<Declarable> processAmqpListener(RabbitListener rabbitListener, Method method,
			Object bean, String beanName) {
		final String rabbitAdmin = resolveMultiRabbitAdminName(rabbitListener);
		final RabbitListener rabbitListenerRef = proxyIfAdminNotPresent(rabbitListener, rabbitAdmin);
		final Collection<Declarable> declarables = super.processAmqpListener(rabbitListenerRef, method, bean, beanName);
		for (final Declarable declarable : declarables) {
			if (declarable.getDeclaringAdmins().isEmpty()) {
				declarable.setAdminsThatShouldDeclare(rabbitAdmin);
			}
		}
		return declarables;
	}

	private RabbitListener proxyIfAdminNotPresent(final RabbitListener rabbitListener, final String rabbitAdmin) {
		if (StringUtils.hasText(rabbitListener.admin())) {
			return rabbitListener;
		}
		return (RabbitListener) Proxy.newProxyInstance(
				RabbitListener.class.getClassLoader(), new Class<?>[]{RabbitListener.class},
				new RabbitListenerAdminReplacementInvocationHandler(rabbitListener, rabbitAdmin));
	}

	/**
	 * Resolves the name of the RabbitAdmin bean based on the RabbitListener, or falls back to
	 * the default RabbitAdmin name provided by MultiRabbit.
	 * @param rabbitListener The RabbitListener to process the name from.
	 * @return The name of the RabbitAdmin bean.
	 */
	protected String resolveMultiRabbitAdminName(RabbitListener rabbitListener) {
		if (StringUtils.hasText(rabbitListener.admin())) {
			return rabbitListener.admin();
		}

		if (StringUtils.hasText(rabbitListener.containerFactory())) {
			var resolved = getContainerFactoryBeanName(rabbitListener.containerFactory());
			return resolved + RabbitListenerConfigUtils.MULTI_RABBIT_ADMIN_SUFFIX;
		}

		return RabbitListenerConfigUtils.RABBIT_ADMIN_BEAN_NAME;
	}

	/**
	 * Resolves the name of containerFactory bean based on the RabbitListener.
	 *
	 * @param containerFactory a SpEL expression is provided ({@code #{...}})
	 * @return The name of bean.
	 */
	private String getContainerFactoryBeanName(final String containerFactory) {

		var rawValue = this.expressionParser.parseExpression(containerFactory, this.templateParserContext).getExpressionString();
		if (containerFactory.startsWith(this.templateParserContext.getExpressionPrefix()) && rawValue.startsWith("@")) {

			return rawValue.substring(1);
		}

		return rawValue;
	}

	public void setExpressionParser(ExpressionParser expressionParser) {
		if (expressionParser != null) {
			this.expressionParser = expressionParser;
		}
	}

	public void setTemplateParserContext(ParserContext templateParserContext) {
		if (templateParserContext != null) {
			this.templateParserContext = templateParserContext;
		}
	}

	/**
	 * An {@link InvocationHandler} to provide a replacing admin() parameter of the listener.
	 */
	private static final class RabbitListenerAdminReplacementInvocationHandler implements InvocationHandler {

		private final RabbitListener target;
		private final String admin;

		private RabbitListenerAdminReplacementInvocationHandler(final RabbitListener target, final String admin) {
			this.target = target;
			this.admin = admin;
		}

		@Override
		public Object invoke(final Object proxy, final Method method, final Object[] args)
				throws InvocationTargetException, IllegalAccessException {
			if (method.getName().equals("admin")) {
				return this.admin;
			}
			return method.invoke(this.target, args);
		}
	}

}
