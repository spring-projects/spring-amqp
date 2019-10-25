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

import org.springframework.amqp.core.AbstractExchange;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.NonNull;

/**
 * An extension of {@link RabbitListenerAnnotationBeanPostProcessor} that associates the proper RabbitAdmin
 * to the beans of Exchanges, Queues, and Bindings after they are created.
 * <p>
 * This processing restricts the {@link RabbitAdmin} according to the related configuration, preventing the server
 * from automatic binding non-related structures.
 *
 * @author Wander Costa
 * @see RabbitListenerAnnotationBeanPostProcessor
 */
public class MultiRabbitListenerAnnotationBeanPostProcessor extends RabbitListenerAnnotationBeanPostProcessor
		implements ApplicationContextAware, BeanFactoryAware {

	private ApplicationContext applicationContext;
	private BeanFactory beanFactory;

	@Override
	protected void processAmqpListener(RabbitListener rabbitListener, Method method, Object bean, String beanName) {
		super.processAmqpListener(rabbitListener, method, bean, beanName);
		enhanceBeansWithReferenceToRabbitAdmin(rabbitListener);
	}

	/**
	 * Enhance beans with related RabbitAdmin, so as to be filtered when being processed by the RabbitAdmin.
	 *
	 * @param rabbitListener the RabbitListener to enhance its bean.
	 */
	private void enhanceBeansWithReferenceToRabbitAdmin(RabbitListener rabbitListener) {
		RabbitAdmin rabbitAdmin = getRabbitAdminBean(rabbitListener);
		// Enhance Exchanges
		this.applicationContext.getBeansOfType(AbstractExchange.class).values().stream().filter(this::isNotProcessed)
				.forEach(exchange -> exchange.setAdminsThatShouldDeclare(rabbitAdmin));

		// Enhance Queues
		this.applicationContext.getBeansOfType(Queue.class).values().stream().filter(this::isNotProcessed)
				.forEach(queue -> queue.setAdminsThatShouldDeclare(rabbitAdmin));

		// Enhance Bindings
		this.applicationContext.getBeansOfType(Binding.class).values().stream().filter(this::isNotProcessed)
				.forEach(binding -> binding.setAdminsThatShouldDeclare(rabbitAdmin));
	}

	/**
	 * Returns the RabbitAdmin bean of the requested name or the default one.
	 *
	 * @param rabbitListener the RabbitListener to retrieve its bean.
	 * @return the bean found.
	 */
	private RabbitAdmin getRabbitAdminBean(RabbitListener rabbitListener) {
		String name = MultiRabbitAdminNameResolver.resolve(rabbitListener);
		try {
			return this.beanFactory.getBean(name, RabbitAdmin.class);
		}
		catch (NoSuchBeanDefinitionException ex) {
			throw new IllegalStateException(String.format("Bean '%s' for RabbitAdmin not found.", name));
		}
	}

	/**
	 * Verifies the presence of an instance of RabbitAdmin or this object, as fallback.
	 *
	 * @param declarable the declarable to be verified.
	 * @return true is the declarable was not processed.
	 */
	private boolean isNotProcessed(Declarable declarable) {
		return declarable.getDeclaringAdmins() == null || (
				declarable.getDeclaringAdmins().stream().noneMatch(item -> item == this) && declarable
						.getDeclaringAdmins().stream().noneMatch(item -> item instanceof RabbitAdmin));
	}

	@Override
	public void setBeanFactory(@NonNull BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		super.setBeanFactory(beanFactory);
	}

	@Override
	public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

}
