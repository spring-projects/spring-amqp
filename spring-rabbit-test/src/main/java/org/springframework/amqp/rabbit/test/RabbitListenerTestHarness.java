/*
 * Copyright 2016 the original author or authors.
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
import java.util.concurrent.TimeUnit;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mockito.Mockito;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;
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
				() -> "@RabbitListenerTest is not present on importing class " + importMetadata.getClassName());
	}

	@Override
	protected void processListener(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener, Object bean,
			Object adminTarget, String beanName) {
		Object proxy = bean;
		String id = rabbitListener.id();
		if (StringUtils.hasText(id)) {
			if (this.attributes.getBoolean("spy")) {
				proxy = Mockito.spy(proxy);
				this.listeners.put(id, proxy);
			}
			if (this.attributes.getBoolean("capture")) {
				try {
					ProxyFactoryBean pfb = new ProxyFactoryBean();
					pfb.setProxyTargetClass(true);
					pfb.setTarget(proxy);
					CaptureAdvice advice = new CaptureAdvice();
					pfb.addAdvice(advice);
					proxy = pfb.getObject();
					this.listenerCapture.put(id, advice);
				}
				catch (Exception e) {
					logger.error("Failed to proxy @RabbitListener with id: " + id);
				}
			}
		}
		else {
			logger.info("The test harness can only proxy @RabbitListeners with an 'id' attribute");
		}
		super.processListener(endpoint, rabbitListener, proxy, adminTarget, beanName);
	}

	public InvocationData getNextInvocationDataFor(String id, long wait, TimeUnit unit) throws InterruptedException {
		CaptureAdvice advice = this.listenerCapture.get(id);
		if (advice != null) {
			return advice.invocationData.poll(wait, unit);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T> T getSpy(String id) {
		return (T) this.listeners.get(id);
	}

	private static final class CaptureAdvice implements MethodInterceptor {

		private final BlockingQueue<InvocationData> invocationData = new LinkedBlockingQueue<InvocationData>();

		CaptureAdvice() {
			super();
		}

		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			Object result = null;
			boolean isListenerMethod =
					AnnotationUtils.findAnnotation(invocation.getMethod(), RabbitListener.class) != null
					|| AnnotationUtils.findAnnotation(invocation.getMethod(), RabbitHandler.class) != null;
			try {
				result = invocation.proceed();
				if (isListenerMethod) {
					this.invocationData.put(new InvocationData(invocation, result));
				}
			}
			catch (Throwable t) { // NOSONAR - rethrown below
				if (isListenerMethod) {
					this.invocationData.put(new InvocationData(invocation, t));
				}
				throw t;
			}
			return result;
		}

	}

	public static class InvocationData {

		private final MethodInvocation invocation;

		private final Object result;

		private final Throwable throwable;

		public InvocationData(MethodInvocation invocation, Object result) {
			this.invocation = invocation;
			this.result = result;
			this.throwable = null;
		}

		public InvocationData(MethodInvocation invocation, Throwable throwable) {
			this.invocation = invocation;
			this.result = null;
			this.throwable = throwable;
		}

		public Object[] getArguments() {
			return this.invocation.getArguments();
		}

		public Object getResult() {
			return this.result;
		}

		public Throwable getThrowable() {
			return this.throwable;
		}

	}

}
