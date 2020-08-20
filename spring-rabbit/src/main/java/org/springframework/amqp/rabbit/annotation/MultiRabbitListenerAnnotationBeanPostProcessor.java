/*
 * Copyright 2020 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Collection;

import org.springframework.amqp.core.Declarable;
import org.springframework.util.StringUtils;

/**
 * An extension of {@link RabbitListenerAnnotationBeanPostProcessor} that associates the
 * proper RabbitAdmin to the beans of Exchanges, Queues, and Bindings after they are
 * created.
 * <p>
 * This processing restricts the {@link org.springframework.amqp.rabbit.core.RabbitAdmin} according to the related
 * configuration, preventing the server from automatic binding non-related structures.
 *
 * @author Wander Costa
 *
 * @since 2.3
 */
public class MultiRabbitListenerAnnotationBeanPostProcessor extends RabbitListenerAnnotationBeanPostProcessor {

	public static final String CONNECTION_FACTORY_BEAN_NAME = "multiRabbitConnectionFactory";

	public static final String CONNECTION_FACTORY_CREATOR_BEAN_NAME = "rabbitConnectionFactoryCreator";

	private static final String DEFAULT_RABBIT_ADMIN_BEAN_NAME = "defaultRabbitAdmin";

	private static final String RABBIT_ADMIN_SUFFIX = "-admin";

	@Override
	protected Collection<Declarable> processAmqpListener(RabbitListener rabbitListener, Method method,
			Object bean, String beanName) {
		final Collection<Declarable> declarables = super.processAmqpListener(rabbitListener, method, bean, beanName);
		final String rabbitAdmin = resolveMultiRabbitAdminName(rabbitListener);
		for (final Declarable declarable : declarables) {
			if (declarable.getDeclaringAdmins().isEmpty()) {
				declarable.setAdminsThatShouldDeclare(rabbitAdmin);
			}
		}
		return declarables;
	}

	/**
	 * Resolves the name of the RabbitAdmin bean based on the RabbitListener, or falls back to
	 * the default RabbitAdmin name provided by MultiRabbit.
	 * @param rabbitListener The RabbitListener to process the name from.
	 * @return The name of the RabbitAdmin bean.
	 */
	protected String resolveMultiRabbitAdminName(RabbitListener rabbitListener) {
		String admin = super.resolveExpressionAsString(rabbitListener.admin(), "admin");
		if (!StringUtils.hasText(admin) && StringUtils.hasText(rabbitListener.containerFactory())) {
			admin = rabbitListener.containerFactory()
					+ MultiRabbitListenerAnnotationBeanPostProcessor.RABBIT_ADMIN_SUFFIX;
		}
		if (!StringUtils.hasText(admin)) {
			admin = MultiRabbitListenerAnnotationBeanPostProcessor.DEFAULT_RABBIT_ADMIN_BEAN_NAME;
		}
		return admin;
	}

}
