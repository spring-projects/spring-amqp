/*
 * Copyright 2002-2019 the original author or authors.
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

import org.springframework.amqp.core.AbstractDeclarable;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.lang.NonNull;

/**
 * An extension of {@link RabbitListenerAnnotationBeanPostProcessor} that associates the
 * proper RabbitAdmin to the beans of Exchanges, Queues, and Bindings after they are
 * created.
 * <p>
 * This processing restricts the {@link RabbitAdmin} according to the related
 * configuration, preventing the server from automatic binding non-related structures.
 *
 * @author Wander Costa
 * @see RabbitListenerAnnotationBeanPostProcessor
 */
public class MultiRabbitListenerAnnotationBeanPostProcessor extends RabbitListenerAnnotationBeanPostProcessor {

	private BeanFactory beanFactory;

	@Override
	public void setBeanFactory(@NonNull BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		super.setBeanFactory(beanFactory);
	}

	@Override
	protected Collection<Declarable> processAmqpListener(RabbitListener rabbitListener, Method method,
			Object bean, String beanName) {
		final Collection<Declarable> declarables = super.processAmqpListener(rabbitListener, method, bean, beanName);
		final RabbitAdmin rabbitAdmin = resolveRabbitAdminBean(rabbitListener);
		declarables.stream()
				.filter(dec -> dec.getDeclaringAdmins().isEmpty())
				.filter(dec -> dec instanceof AbstractDeclarable)
				.map(dec -> (AbstractDeclarable) dec)
				.forEach(dec -> dec.setAdminsThatShouldDeclare(rabbitAdmin));
		return declarables;
	}

	/**
	 * Returns the RabbitAdmin bean of the requested name or the default one.
	 *
	 * @param rabbitListener the RabbitListener to retrieve its bean.
	 * @return the bean found.
	 */
	private RabbitAdmin resolveRabbitAdminBean(RabbitListener rabbitListener) {
		final String name = MultiRabbitAdminNameResolver.resolve(rabbitListener);
		return this.beanFactory.getBean(name, RabbitAdmin.class);
	}
}
