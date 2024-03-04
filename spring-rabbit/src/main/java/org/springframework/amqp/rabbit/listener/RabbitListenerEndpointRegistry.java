/*
 * Copyright 2014-2023 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Creates the necessary {@link MessageListenerContainer} instances for the
 * registered {@linkplain RabbitListenerEndpoint endpoints}. Also manages the
 * lifecycle of the listener containers, in particular within the lifecycle
 * of the application context.
 *
 * <p>Contrary to {@link MessageListenerContainer}s created manually, listener
 * containers managed by registry are not beans in the application context and
 * are not candidates for autowiring. Use {@link #getListenerContainers()} if
 * you need to access this registry's listener containers for management purposes.
 * If you need to access to a specific message listener container, use
 * {@link #getListenerContainer(String)} with the id of the endpoint.
 *
 * @author Stephane Nicoll
 * @author Juergen Hoeller
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.4
 *
 * @see RabbitListenerEndpoint
 * @see MessageListenerContainer
 * @see RabbitListenerContainerFactory
 */
public class RabbitListenerEndpointRegistry implements DisposableBean, SmartLifecycle, ApplicationContextAware,
		ApplicationListener<ContextRefreshedEvent> {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR protected

	private final Map<String, MessageListenerContainer> listenerContainers = new ConcurrentHashMap<>();

	private final Lock listenerContainersLock = new ReentrantLock();

	private int phase = Integer.MAX_VALUE;

	private ConfigurableApplicationContext applicationContext;

	private boolean contextRefreshed;


	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		if (applicationContext instanceof ConfigurableApplicationContext configurable) {
			this.applicationContext = configurable;
		}
	}

	/**
	 * Return the {@link MessageListenerContainer} with the specified id or
	 * {@code null} if no such container exists.
	 * @param id the id of the container
	 * @return the container or {@code null} if no container with that id exists
	 * @see RabbitListenerEndpoint#getId()
	 * @see #getListenerContainerIds()
	 */
	public MessageListenerContainer getListenerContainer(String id) {
		Assert.hasText(id, "Container identifier must not be empty");
		return this.listenerContainers.get(id);
	}

	/**
	 * Return the ids of the managed {@link MessageListenerContainer} instance(s).
	 * @return the ids.
	 * @since 1.5.2
	 * @see #getListenerContainer(String)
	 */
	public Set<String> getListenerContainerIds() {
		return Collections.unmodifiableSet(this.listenerContainers.keySet());
	}

	/**
	 * @return the managed {@link MessageListenerContainer} instance(s).
	 */
	public Collection<MessageListenerContainer> getListenerContainers() {
		return Collections.unmodifiableCollection(this.listenerContainers.values());
	}

	/**
	 * Create a message listener container for the given {@link RabbitListenerEndpoint}.
	 * <p>This create the necessary infrastructure to honor that endpoint in regard to its configuration.
	 * @param endpoint the endpoint to add
	 * @param factory the listener factory to use
	 * @see #registerListenerContainer(RabbitListenerEndpoint, RabbitListenerContainerFactory, boolean)
	 */
	public void registerListenerContainer(RabbitListenerEndpoint endpoint, RabbitListenerContainerFactory<?> factory) {
		registerListenerContainer(endpoint, factory, false);
	}

	/**
	 * Create a message listener container for the given {@link RabbitListenerEndpoint}.
	 * <p>This create the necessary infrastructure to honor that endpoint in regard to its configuration.
	 * <p>The {@code startImmediately} flag determines if the container should be
	 * started immediately.
	 * @param endpoint the endpoint to add.
	 * @param factory the {@link RabbitListenerContainerFactory} to use.
	 * @param startImmediately start the container immediately if necessary
	 * @see #getListenerContainers()
	 * @see #getListenerContainer(String)
	 */
	@SuppressWarnings("unchecked")
	public void registerListenerContainer(RabbitListenerEndpoint endpoint, RabbitListenerContainerFactory<?> factory,
				boolean startImmediately) {

		Assert.notNull(endpoint, "Endpoint must not be null");
		Assert.notNull(factory, "Factory must not be null");

		String id = endpoint.getId();
		Assert.hasText(id, "Endpoint id must not be empty");
		this.listenerContainersLock.lock();
		try {
			Assert.state(!this.listenerContainers.containsKey(id),
					"Another endpoint is already registered with id '" + id + "'");
			MessageListenerContainer container = createListenerContainer(endpoint, factory);
			this.listenerContainers.put(id, container);
			if (StringUtils.hasText(endpoint.getGroup()) && this.applicationContext != null) {
				List<MessageListenerContainer> containerGroup;
				if (this.applicationContext.containsBean(endpoint.getGroup())) {
					containerGroup = this.applicationContext.getBean(endpoint.getGroup(), List.class);
				}
				else {
					containerGroup = new ArrayList<>();
					this.applicationContext.getBeanFactory().registerSingleton(endpoint.getGroup(), containerGroup);
				}
				containerGroup.add(container);
			}
			if (this.contextRefreshed) {
				container.lazyLoad();
			}
			if (startImmediately) {
				startIfNecessary(container);
			}
		}
		finally {
			this.listenerContainersLock.unlock();
		}
	}

	/**
	 * Create and start a new {@link MessageListenerContainer} using the specified factory.
	 * @param endpoint the endpoint to create a {@link MessageListenerContainer}.
	 * @param factory the {@link RabbitListenerContainerFactory} to use.
	 * @return the {@link MessageListenerContainer}.
	 */
	protected MessageListenerContainer createListenerContainer(RabbitListenerEndpoint endpoint,
			RabbitListenerContainerFactory<?> factory) {

		MessageListenerContainer listenerContainer = factory.createListenerContainer(endpoint);
		listenerContainer.afterPropertiesSet();

		int containerPhase = listenerContainer.getPhase();
		if (containerPhase < Integer.MAX_VALUE) {  // a custom phase value
			if (this.phase < Integer.MAX_VALUE && this.phase != containerPhase) {
				throw new IllegalStateException("Encountered phase mismatch between container factory definitions: " +
						this.phase + " vs " + containerPhase);
			}
			this.phase = listenerContainer.getPhase();
		}

		return listenerContainer;
	}

	/**
	 * Remove a listener container from the registry.
	 * @param id the container id.
	 * @return the container, or null if there is no registration matching the id.
	 * @since 2.0.6
	 */
	@Nullable
	public MessageListenerContainer unregisterListenerContainer(String id) {
		return this.listenerContainers.remove(id);
	}

	@Override
	public void destroy() {
		for (MessageListenerContainer listenerContainer : getListenerContainers()) {
			if (listenerContainer instanceof DisposableBean disposable) {
				try {
					disposable.destroy();
				}
				catch (Exception ex) {
					this.logger.warn("Failed to destroy listener container [" + listenerContainer + "]", ex);
				}
			}
		}
	}


	// Delegating implementation of SmartLifecycle

	@Override
	public int getPhase() {
		return this.phase;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void start() {
		for (MessageListenerContainer listenerContainer : getListenerContainers()) {
			startIfNecessary(listenerContainer);
		}
	}

	@Override
	public void stop() {
		for (MessageListenerContainer listenerContainer : getListenerContainers()) {
			listenerContainer.stop();
		}
	}

	@Override
	public void stop(Runnable callback) {
		Collection<MessageListenerContainer> containers = getListenerContainers();
		if (!containers.isEmpty()) {
			AggregatingCallback aggregatingCallback = new AggregatingCallback(containers.size(), callback);
			for (MessageListenerContainer listenerContainer : containers) {
				try {
					listenerContainer.stop(aggregatingCallback);
				}
				catch (Exception e) {
					if (this.logger.isWarnEnabled()) {
						this.logger.warn("Failed to stop listener container [" + listenerContainer + "]", e);
					}
				}
			}
		}
		else {
			callback.run();
		}
	}

	@Override
	public boolean isRunning() {
		for (MessageListenerContainer listenerContainer : getListenerContainers()) {
			if (listenerContainer.isRunning()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Start the specified {@link MessageListenerContainer} if it should be started
	 * on startup or when start is called explicitly after startup.
	 * @param listenerContainer the container.
	 * @see MessageListenerContainer#isAutoStartup()
	 */
	private void startIfNecessary(MessageListenerContainer listenerContainer) {
		if (this.contextRefreshed || listenerContainer.isAutoStartup()) {
			listenerContainer.start();
		}
	}


	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (event.getApplicationContext().equals(this.applicationContext)) {
			this.contextRefreshed = true;
		}
	}


	private static final class AggregatingCallback implements Runnable {

		private final AtomicInteger count;

		private final Runnable finishCallback;

		AggregatingCallback(int count, Runnable finishCallback) {
			this.count = new AtomicInteger(count);
			this.finishCallback = finishCallback;
		}

		@Override
		public void run() {
			if (this.count.decrementAndGet() == 0) {
				this.finishCallback.run();
			}
		}

	}

}
