/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.amqp.rabbit.test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerAnnotationBeanPostProcessor;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.test.mockito.LambdaAnswer;
import org.springframework.amqp.rabbit.test.mockito.LambdaAnswer.ValueToReturn;
import org.springframework.amqp.rabbit.test.mockito.LatchCountDownAndCallRealMethodAnswer;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.test.util.AopTestUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * BeanPostProcessor extending {@link RabbitListenerAnnotationBeanPostProcessor}.
 * Wraps the listener bean in a CGLIB proxy with an advice to capture the arguments
 * and result (if any) in a blocking queue. Test cases can access the results
 * by autowiring the test harness into test cases.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Miguel Gross Valle
 *
 * @since 1.6
 *
 */
public class RabbitListenerTestHarness extends RabbitListenerAnnotationBeanPostProcessor {

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Map<String, CaptureAdvice> listenerCapture = new HashMap<>();

	private final Map<String, Object> listeners = new HashMap<>();

	private final Map<String, Object> delegates = new HashMap<>();

	private final AnnotationAttributes attributes;

	public RabbitListenerTestHarness(AnnotationMetadata importMetadata) {
		Map<String, @Nullable Object> map = importMetadata.getAnnotationAttributes(RabbitListenerTest.class.getName());
		AnnotationAttributes annotationAttributes = AnnotationAttributes.fromMap(map);
		Assert.notNull(annotationAttributes,
				() -> "@RabbitListenerTest is not present on importing class " + importMetadata.getClassName());
		this.attributes = annotationAttributes;
	}

	@Override
	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	protected Collection<Declarable> processListener(MethodRabbitListenerEndpoint endpoint,
			RabbitListener rabbitListener, Object bean, Object target, String beanName) {

		Object proxy = bean;
		String id = rabbitListener.id();
		if (StringUtils.hasText(id)) {
			if (this.attributes.getBoolean("spy")) {
				this.delegates.put(id, proxy);
				proxy = Mockito.mock(AopTestUtils.getUltimateTargetObject(proxy).getClass(),
						AdditionalAnswers.delegatesTo(proxy));
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
					throw new AmqpException("Failed to proxy @RabbitListener with id: " + id, e);
				}
			}
		}
		else {
			logger.info("The test harness can only proxy @RabbitListeners with an 'id' attribute");
		}
		return super.processListener(endpoint, rabbitListener, proxy, target, beanName);
	}

	/**
	 * Return a {@link LatchCountDownAndCallRealMethodAnswer} that is properly configured
	 * to invoke the listener.
	 * @param id the listener id.
	 * @param count the count.
	 * @return the answer.
	 * @since 2.1.16
	 */
	public LatchCountDownAndCallRealMethodAnswer getLatchAnswerFor(String id, int count) {
		return new LatchCountDownAndCallRealMethodAnswer(count, this.delegates.get(id));
	}

	/**
	 * Return a {@link LambdaAnswer} that is properly configured to invoke the listener.
	 * @param <T> the return type.
	 * @param id the listener id.
	 * @param callRealMethod true to call the real method.
	 * @param callback the callback.
	 * @return the answer.
	 * @since 2.1.16
	 */
	public <T> LambdaAnswer<T> getLambdaAnswerFor(String id, boolean callRealMethod, ValueToReturn<T> callback) {
		return new LambdaAnswer<>(callRealMethod, callback, this.delegates.get(id));
	}

	public @Nullable InvocationData getNextInvocationDataFor(String id, long wait, TimeUnit unit)
			throws InterruptedException {

		CaptureAdvice advice = this.listenerCapture.get(id);
		if (advice != null) {
			return advice.invocationData.poll(wait, unit);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T> @Nullable T getSpy(String id) {
		return (T) this.listeners.get(id);
	}

	/**
	 * Get the actual listener object (not the spy).
	 * @param <T> the type.
	 * @param id the id.
	 * @return the listener.
	 * @since 2.1.16
	 */
	@SuppressWarnings("unchecked")
	public <T> @Nullable T getDelegate(String id) {
		return (T) this.delegates.get(id);
	}

	private static final class CaptureAdvice implements MethodInterceptor {

		private final BlockingQueue<InvocationData> invocationData = new LinkedBlockingQueue<>();

		CaptureAdvice() {
		}

		@Override
		public @Nullable Object invoke(MethodInvocation invocation) throws Throwable {
			MergedAnnotations annotations = MergedAnnotations.from(invocation.getMethod());
			boolean isListenerMethod = annotations.isPresent(RabbitListener.class)
					|| annotations.isPresent(RabbitHandler.class);
			try {
				Object result = invocation.proceed();
				if (isListenerMethod) {
					this.invocationData.put(new InvocationData(invocation, result));
				}
				return result;
			}
			catch (Throwable t) { // NOSONAR - rethrown below
				if (isListenerMethod) {
					this.invocationData.put(new InvocationData(invocation, t));
				}
				throw t;
			}
		}

	}

	public static class InvocationData {

		private final MethodInvocation invocation;

		private final @Nullable Object result;

		private final @Nullable Throwable throwable;

		public InvocationData(MethodInvocation invocation, @Nullable Object result) {
			this.invocation = invocation;
			this.result = result;
			this.throwable = null;
		}

		public InvocationData(MethodInvocation invocation, Throwable throwable) {
			this.invocation = invocation;
			this.result = null;
			this.throwable = throwable;
		}

		public @Nullable Object[] getArguments() {
			return this.invocation.getArguments();
		}

		public @Nullable Object getResult() {
			return this.result;
		}

		public @Nullable Throwable getThrowable() {
			return this.throwable;
		}

	}

}
