/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.amqp.core;

import java.util.Map;

import org.springframework.amqp.core.AnonymousQueue.NamingStrategy;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Factory bean to create an {@link AnonymousQueue}.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class QueueFactoryBean extends AbstractFactoryBean<Queue> {

	private final String name;

	private final String namingStrategy;

	private final boolean durable;

	private final boolean exclusive;

	private final boolean autoDelete;

	private final Map<String, Object> arguments;

	private boolean shouldDeclare = true;

	private Object[] admins;

	public QueueFactoryBean() {
		this(null, null, false, true, true, null);
	}

	public QueueFactoryBean(String namingStrategy) {
		this(null, namingStrategy, false, true, true, null);
	}

	public QueueFactoryBean(String name, String namingStrategy, boolean durable, boolean exclusive, boolean autoDelete) {
		this(name, namingStrategy, durable, exclusive, autoDelete, null);
	}

	public QueueFactoryBean(String name, String namingStrategy, boolean durable, boolean exclusive, boolean autoDelete,
			Map<String, Object> arguments) {
		this.name = name;
		this.namingStrategy = namingStrategy;
		this.durable = durable;
		this.exclusive = exclusive;
		this.autoDelete = autoDelete;
		this.arguments = arguments;
	}

	public void setShouldDeclare(boolean shouldDeclare) {
		this.shouldDeclare = shouldDeclare;
	}

	public void setAdminsThatShouldDeclare(Object... admins) {
		this.admins = admins;
	}

	@Override
	public Class<?> getObjectType() {
		return AnonymousQueue.class;
	}

	@Override
	protected Queue createInstance() throws Exception {
		Queue queue;
		if (StringUtils.hasText(this.name)) {
			queue = new Queue(this.name, this.durable, this.exclusive, this.autoDelete, this.arguments);
		}
		else {
			Assert.state(!this.durable && this.exclusive && this.autoDelete, "If there is no 'name' attribute, "
					+ "'durable' must be false, 'exclusive' and 'auto-delete' must be 'true'");
			if (!StringUtils.hasText(this.namingStrategy) || "uuid".equals(this.namingStrategy)) {
				queue = new AnonymousQueue(this.arguments);
			}
			else if ("spring".equals(this.namingStrategy)) {
				queue = new AnonymousQueue(new AnonymousQueue.Base64UrlNamingStrategy(), this.arguments);
			}
			else {
				Assert.state(getBeanFactory().containsBean(this.namingStrategy), "'namingStrategy' ("
						+ this.namingStrategy + ") must be 'uuid', 'spring' or a "
						+ "reference to a bean that is an AnonymousQueue.NamingStrategy implementation");
				queue = new AnonymousQueue(getBeanFactory().getBean(this.namingStrategy, NamingStrategy.class),
						this.arguments);
			}
		}
		queue.setShouldDeclare(this.shouldDeclare);
		queue.setAdminsThatShouldDeclare(this.admins);
		return queue;
	}

}
