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
package org.springframework.amqp.rabbit.test;

import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.util.StringUtils;

/**
 * BeanPostProcessor extending {@link RabbitListenerAnnotationBeanPostProcessor}.
 * Wraps the listener bean in a Mockito spy.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class RabbitListenerAnnotationSpyBeanPostProcessor extends RabbitListenerAnnotationBeanPostProcessor {

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Map<String, Object> listeners = new HashMap<String, Object>();

	@Override
	protected void processListener(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener, Object bean,
			Object adminTarget, String beanName) {
		String id = rabbitListener.id();
		if (StringUtils.hasText(id)) {
			Object spy = spy(bean);
			this.listeners.put(id, spy);
			super.processListener(endpoint, rabbitListener, spy, adminTarget, beanName);
		}
		else {
			logger.info("The test harness can only proxy @RabbitListeners with an 'id' attribute");
			super.processListener(endpoint, rabbitListener, bean, adminTarget, beanName);
		}
	}

	@SuppressWarnings("unchecked")
	<T> T getSpy(String id) {
		return (T) this.listeners.get(id);
	}

}
