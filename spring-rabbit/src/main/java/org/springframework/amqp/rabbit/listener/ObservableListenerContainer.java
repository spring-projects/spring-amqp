/*
 * Copyright 2023-present the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.ClassUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 3.0.5
 *
 */
public abstract class ObservableListenerContainer extends RabbitAccessor
		implements MessageListenerContainer, ApplicationContextAware, BeanNameAware, DisposableBean {

	private static final boolean MICROMETER_PRESENT = ClassUtils.isPresent(
			"io.micrometer.core.instrument.MeterRegistry", AbstractMessageListenerContainer.class.getClassLoader());

	private @Nullable ApplicationContext applicationContext;

	private final Map<String, String> micrometerTags = new HashMap<>();

	private @Nullable MicrometerHolder micrometerHolder;

	private boolean micrometerEnabled = true;

	private boolean observationEnabled = false;

	private String beanName = "not.a.Spring.bean";

	private @Nullable String listenerId;

	protected final @Nullable ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	protected @Nullable MicrometerHolder getMicrometerHolder() {
		return this.micrometerHolder;
	}

	/**
	 * Set additional tags for the Micrometer listener timers.
	 * @param tags the tags.
	 * @since 2.2
	 */
	public void setMicrometerTags(@Nullable Map<String, String> tags) {
		if (tags != null) {
			this.micrometerTags.putAll(tags);
		}
	}

	/**
	 * Set to {@code false} to disable micrometer listener timers. When true, ignored
	 * if {@link #setObservationEnabled(boolean)} is set to true.
	 * @param micrometerEnabled false to disable.
	 * @since 2.2
	 * @see #setObservationEnabled(boolean)
	 */
	public void setMicrometerEnabled(boolean micrometerEnabled) {
		this.micrometerEnabled = micrometerEnabled;
	}

	/**
	 * Enable observation via micrometer; disables basic Micrometer timers enabled
	 * by {@link #setMicrometerEnabled(boolean)}.
	 * @param observationEnabled true to enable.
	 * @since 3.0
	 * @see #setMicrometerEnabled(boolean)
	 */
	public void setObservationEnabled(boolean observationEnabled) {
		this.observationEnabled = observationEnabled;
	}

	protected void checkMicrometer() {
		try {
			if (this.micrometerHolder == null && MICROMETER_PRESENT && this.micrometerEnabled
					&& !this.observationEnabled && this.applicationContext != null) {

				this.micrometerHolder = new MicrometerHolder(this.applicationContext, getListenerId(),
						this.micrometerTags);
			}
		}
		catch (IllegalStateException e) {
			this.logger.debug("Could not enable micrometer timers", e);
		}
	}

	protected void checkObservation() {
		if (this.observationEnabled) {
			obtainObservationRegistry(this.applicationContext);
		}
	}


	protected boolean isApplicationContextClosed() {
		return this.applicationContext instanceof ConfigurableApplicationContext configurableCtx
				&& configurableCtx.isClosed();
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	/**
	 * @return The bean name that this listener container has been assigned in its containing bean factory, if any.
	 */
	protected final String getBeanName() {
		return this.beanName;
	}

	/**
	 * The 'id' attribute of the listener.
	 * @return the id (or the container bean name if no id set).
	 */
	public String getListenerId() {
		return this.listenerId != null ? this.listenerId : this.beanName;
	}

	@Override
	public void setListenerId(String listenerId) {
		this.listenerId = listenerId;
	}

	@Override
	public void destroy() {
		if (this.micrometerHolder != null) {
			this.micrometerHolder.destroy();
		}
	}

}
