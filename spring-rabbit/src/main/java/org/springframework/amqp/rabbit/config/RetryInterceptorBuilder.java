/*
 * Copyright 2014-present the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import org.aopalliance.intercept.MethodInterceptor;

import org.springframework.amqp.rabbit.retry.MessageKeyGenerator;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.NewMessageIdentifier;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.interceptor.StatefulRetryOperationsInterceptor;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * <p>Simplified facade to make it easier and simpler to build a
 * {@link StatefulRetryOperationsInterceptor} or
 * (stateless) {@link RetryOperationsInterceptor}
 * by providing a fluent interface to defining the behavior on error.
 * <p>
 * Typical example:
 * </p>
 *
 * <pre class="code">
 *	StatefulRetryOperationsInterceptor interceptor =
 *			RetryInterceptorBuilder.stateful()
 *				.maxAttempts(5)
 *				.backOffOptions(1, 2, 10) // initialInterval, multiplier, maxInterval
 *				.build();
 * </pre>
 * <p>
 * When building a stateful interceptor, a message identifier is required.
 * The default behavior determines message identity based on {@code messageId}.
 * This isn't a required field and may not be set by the sender.
 * If it is not, you can change the logic to determine message
 * identity using a custom generator:</p>
 * <pre class="code">
 * 		StatefulRetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateful()
 *				.messageKeyGenerator(new MyMessageKeyGenerator())
 *				.build();
 * </pre>
 *
 * @param <B> The target {@link RetryInterceptorBuilder} implementation type.
 * @param <T> The type of {@link MethodInterceptor} returned by the builder's {@link #build()} method.
 *
 * @author James Carr
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
public abstract class RetryInterceptorBuilder<B extends RetryInterceptorBuilder<B, T>, T extends MethodInterceptor> {

	private RetryOperations retryOperations;

	private final RetryTemplate retryTemplate = new RetryTemplate();

	private final SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();

	private MessageRecoverer messageRecoverer;

	private boolean templateAltered;

	private boolean backOffPolicySet;

	private boolean retryPolicySet;

	private boolean backOffOptionsSet;

	/**
	 * Create a builder for a stateful retry interceptor.
	 * @return The interceptor builder.
	 */
	public static StatefulRetryInterceptorBuilder stateful() {
		return new StatefulRetryInterceptorBuilder();
	}

	/**
	 * Create a builder for a stateless retry interceptor.
	 * @return The interceptor builder.
	 */
	public static StatelessRetryInterceptorBuilder stateless() {
		return new StatelessRetryInterceptorBuilder();
	}

	@SuppressWarnings("unchecked")
	protected final B _this() { // NOSONAR starts with _
		return (B) this;
	}

	/**
	 * Apply the retry operations - once this is set, other properties can no longer be set; can't
	 * be set if other properties have been applied.
	 * @param retryOperations The retry operations.
	 * @return this.
	 */
	public B retryOperations(RetryOperations retryOperations) {
		Assert.isTrue(!this.templateAltered, "Cannot set retryOperations when the default has been modified");
		this.retryOperations = retryOperations;
		return _this();
	}

	/**
	 * Apply the max attempts - a SimpleRetryPolicy will be used. Cannot be used if a custom retry operations
	 * or retry policy has been set.
	 * @param maxAttempts the max attempts.
	 * @return this.
	 */
	public B maxAttempts(int maxAttempts) {
		Assert.isNull(this.retryOperations, "cannot alter the retry policy when a custom retryOperations has been set");
		Assert.isTrue(!this.retryPolicySet, "cannot alter the retry policy when a custom retryPolicy has been set");
		this.simpleRetryPolicy.setMaxAttempts(maxAttempts);
		this.retryTemplate.setRetryPolicy(this.simpleRetryPolicy);
		this.templateAltered = true;
		return _this();
	}

	/**
	 * Apply the backoff options. Cannot be used if a custom retry operations, or back off policy has been set.
	 * @param initialInterval The initial interval.
	 * @param multiplier The multiplier.
	 * @param maxInterval The max interval.
	 * @return this.
	 */
	public B backOffOptions(long initialInterval, double multiplier, long maxInterval) {
		Assert.isNull(this.retryOperations, "cannot set the back off policy when a custom retryOperations has been set");
		Assert.isTrue(!this.backOffPolicySet, "cannot set the back off options when a back off policy has been set");
		ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
		policy.setInitialInterval(initialInterval);
		policy.setMultiplier(multiplier);
		policy.setMaxInterval(maxInterval);
		this.retryTemplate.setBackOffPolicy(policy);
		this.backOffOptionsSet = true;
		this.templateAltered = true;
		return _this();
	}

	/**
	 * Apply the retry policy - cannot be used if a custom retry template has been provided, or the max attempts or
	 * back off options or policy have been applied.
	 * @param policy The policy.
	 * @return this.
	 */
	public B retryPolicy(RetryPolicy policy) {
		Assert.isNull(this.retryOperations, "cannot set the retry policy when a custom retryOperations has been set");
		Assert.isTrue(!this.templateAltered, "cannot set the retry policy if max attempts or back off policy or options changed");
		this.retryTemplate.setRetryPolicy(policy);
		this.retryPolicySet = true;
		this.templateAltered = true;
		return _this();
	}

	/**
	 * Apply the back off policy. Cannot be used if a custom retry operations, or back off policy has been applied.
	 * @param policy The policy.
	 * @return this.
	 */
	public B backOffPolicy(BackOffPolicy policy) {
		Assert.isNull(this.retryOperations, "cannot set the back off policy when a custom retryOperations has been set");
		Assert.isTrue(!this.backOffOptionsSet, "cannot set the back off policy when the back off policy options have been set");
		this.retryTemplate.setBackOffPolicy(policy);
		this.templateAltered = true;
		this.backOffPolicySet = true;
		return _this();
	}

	/**
	 * Apply a Message recoverer - default is to log and discard after retry is exhausted.
	 * @param recoverer The recoverer.
	 * @return this.
	 */
	public B recoverer(MessageRecoverer recoverer) {
		this.messageRecoverer = recoverer;
		return _this();
	}

	protected void applyCommonSettings(AbstractRetryOperationsInterceptorFactoryBean factoryBean) {
		if (this.messageRecoverer != null) {
			factoryBean.setMessageRecoverer(this.messageRecoverer);
		}
		if (this.retryOperations != null) {
			factoryBean.setRetryOperations(this.retryOperations);
		}
		else {
			factoryBean.setRetryOperations(this.retryTemplate);
		}
	}

	public abstract T build();


	/**
	 * Builder for a stateful interceptor.
	 */
	public static final class StatefulRetryInterceptorBuilder
			extends RetryInterceptorBuilder<StatefulRetryInterceptorBuilder, StatefulRetryOperationsInterceptor> {

		private final StatefulRetryOperationsInterceptorFactoryBean factoryBean =
				new StatefulRetryOperationsInterceptorFactoryBean();

		private MessageKeyGenerator messageKeyGenerator;

		private NewMessageIdentifier newMessageIdentifier;

		StatefulRetryInterceptorBuilder() {
		}

		/**
		 * Stateful retry requires messages to be identifiable. Default is to use the message id header; use a custom
		 * implementation if the message id is not present or not reliable.
		 * @param messageKeyGenerator The key generator.
		 * @return this.
		 */
		public StatefulRetryInterceptorBuilder messageKeyGenerator(MessageKeyGenerator messageKeyGenerator) {
			this.messageKeyGenerator = messageKeyGenerator;
			return this;
		}

		/**
		 * Apply a custom new message identifier. Default is to use the redelivered header.
		 * @param newMessageIdentifier The new message identifier.
		 * @return this.
		 */
		public StatefulRetryInterceptorBuilder newMessageIdentifier(NewMessageIdentifier newMessageIdentifier) {
			this.newMessageIdentifier = newMessageIdentifier;
			return this;
		}

		@Override
		public StatefulRetryOperationsInterceptor build() {
			this.applyCommonSettings(this.factoryBean);
			if (this.messageKeyGenerator != null) {
				this.factoryBean.setMessageKeyGenerator(this.messageKeyGenerator);
			}
			if (this.newMessageIdentifier != null) {
				this.factoryBean.setNewMessageIdentifier(this.newMessageIdentifier);
			}
			return this.factoryBean.getObject();
		}

	}


	/**
	 * Builder for a stateless interceptor.
	 */
	public static final class StatelessRetryInterceptorBuilder
			extends RetryInterceptorBuilder<StatelessRetryInterceptorBuilder, RetryOperationsInterceptor> {

		private final StatelessRetryOperationsInterceptorFactoryBean factoryBean =
				new StatelessRetryOperationsInterceptorFactoryBean();

		StatelessRetryInterceptorBuilder() {
		}

		@Override
		public RetryOperationsInterceptor build() {
			this.applyCommonSettings(this.factoryBean);
			return this.factoryBean.getObject();
		}

	}

}
