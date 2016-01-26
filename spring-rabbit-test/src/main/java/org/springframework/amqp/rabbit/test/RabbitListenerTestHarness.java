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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mockito.Mockito;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * BeanPostProcessor extending {@link RabbitListenerAnnotationBeanPostProcessor}.
 * Wraps the listener bean in a CGLIB proxy with an advice to capture the arguments
 * and result (if any) in a blocking queue. Test cases can access the results
 * by autowiring the test harness into test cases.
 *
 * @author Gary Russell
 * @since 1.6
 *
 */
public class RabbitListenerTestHarness extends RabbitListenerAnnotationBeanPostProcessor {

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Map<String, CaptureAdvice> listenerCapture = new HashMap<String, CaptureAdvice>();

	private final AnnotationAttributes attributes;

	private final Map<String, Object> listeners = new HashMap<String, Object>();

	public RabbitListenerTestHarness(AnnotationMetadata importMetadata) {
		Map<String, Object> map = importMetadata.getAnnotationAttributes(RabbitListenerTest.class.getName());
		this.attributes = AnnotationAttributes.fromMap(map);
		Assert.notNull(this.attributes,
				"@RabbitListenerTest is not present on importing class " + importMetadata.getClassName());
	}

	@Override
	protected void processListener(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener, Object bean,
			Object adminTarget, String beanName) {
		String id = rabbitListener.id();
		if (StringUtils.hasText(id)) {
			if (this.attributes.getBoolean("spy")) {
				Object spy = Mockito.spy(bean);
				this.listeners.put(id, spy);
				super.processListener(endpoint, rabbitListener, spy, adminTarget, beanName);
				return;
			}
			else {
				try {
					ProxyFactoryBean pfb = new ProxyFactoryBean();
					pfb.setProxyTargetClass(true);
					pfb.setTarget(bean);
					CaptureAdvice advice = new CaptureAdvice();
					pfb.addAdvice(advice);
					Object advised = pfb.getObject();
						this.listenerCapture.put(id, advice);
						super.processListener(endpoint, rabbitListener, advised, adminTarget, beanName);
						return;
				}
				catch (Exception e) {
					logger.error("Failed to proxy @RabbitListener with id: " + id);
				}
			}
		}
		else {
			logger.info("The test harness can only proxy @RabbitListeners with an 'id' attribute");
		}
		super.processListener(endpoint, rabbitListener, bean, adminTarget, beanName);
	}

	public BlockingQueue<Object[]> getArgumentQueueFor(String id) {
		CaptureAdvice advice = this.listenerCapture.get(id);
		if (advice != null) {
			return advice.arguments;
		}
		return null;
	}

	public BlockingQueue<Object> getResultQueueFor(String id) {
		CaptureAdvice advice = this.listenerCapture.get(id);
		if (advice != null) {
			return advice.returns;
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	<T> T getSpy(String id) {
		return (T) this.listeners.get(id);
	}

	private static final class CaptureAdvice implements MethodInterceptor {

		private final BlockingQueue<Object[]> arguments = new LinkedBlockingQueue<Object[]>();

		private final BlockingQueue<Object> returns = new LinkedBlockingQueue<Object>();

		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			Object[] args = invocation.getArguments();
			if (args.length > 0) {
				arguments.add(args);
			}
			Object result = invocation.proceed();
			if (args.length > 0 && result != null) {
				this.returns.put(result);
			}
			return result;
		}

	}

}
