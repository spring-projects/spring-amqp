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

package org.springframework.amqp.rabbit.annotation;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Binding.DestinationType;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.RabbitListenerConfigUtils;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.MethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.MultiMethodRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.adapter.AmqpMessageHandlerMethodFactory;
import org.springframework.amqp.rabbit.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.annotation.MergedAnnotations.SearchStrategy;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.env.Environment;
import org.springframework.core.task.TaskExecutor;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.Validator;

/**
 * Bean post-processor that registers methods annotated with {@link RabbitListener}
 * to be invoked by a AMQP message listener container created under the cover
 * by a {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory}
 * according to the parameters of the annotation.
 *
 * <p>Annotated methods can use flexible arguments as defined by {@link RabbitListener}.
 *
 * <p>This post-processor is automatically registered by Spring's
 * {@code <rabbit:annotation-driven>} XML element, and also by the {@link EnableRabbit}
 * annotation.
 *
 * <p>Auto-detect any {@link RabbitListenerConfigurer} instances in the container,
 * allowing for customization of the registry to be used, the default container
 * factory or for fine-grained control over endpoints registration. See
 * {@link EnableRabbit} Javadoc for complete usage details.
 *
 * @author Stephane Nicoll
 * @author Juergen Hoeller
 * @author Gary Russell
 * @author Alex Panchenko
 * @author Artem Bilan
 *
 * @since 1.4
 *
 * @see RabbitListener
 * @see EnableRabbit
 * @see RabbitListenerConfigurer
 * @see RabbitListenerEndpointRegistrar
 * @see RabbitListenerEndpointRegistry
 * @see org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint
 * @see MethodRabbitListenerEndpoint
 */
public class RabbitListenerAnnotationBeanPostProcessor
		implements BeanPostProcessor, Ordered, BeanFactoryAware, BeanClassLoaderAware, EnvironmentAware,
		SmartInitializingSingleton {

	/**
	 * The bean name of the default {@link org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory}.
	 */
	public static final String DEFAULT_RABBIT_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "rabbitListenerContainerFactory";

	public static final String RABBIT_EMPTY_STRING_ARGUMENTS_PROPERTY = "spring.rabbitmq.emptyStringArguments";

	private static final ConversionService CONVERSION_SERVICE = new DefaultConversionService();

	private final Log logger = LogFactory.getLog(this.getClass());

	private final Set<String> emptyStringArguments = new HashSet<>();

	private RabbitListenerEndpointRegistry endpointRegistry;

	private String defaultContainerFactoryBeanName = DEFAULT_RABBIT_LISTENER_CONTAINER_FACTORY_BEAN_NAME;

	private BeanFactory beanFactory;

	private ClassLoader beanClassLoader;

	private final RabbitHandlerMethodFactoryAdapter messageHandlerMethodFactory =
			new RabbitHandlerMethodFactoryAdapter();

	private final RabbitListenerEndpointRegistrar registrar = new RabbitListenerEndpointRegistrar();

	private final AtomicInteger counter = new AtomicInteger();

	private final ConcurrentMap<Class<?>, TypeMetadata> typeCache = new ConcurrentHashMap<>();

	private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

	private BeanExpressionContext expressionContext;

	private int increment;

	private Charset charset = StandardCharsets.UTF_8;

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}

	public RabbitListenerAnnotationBeanPostProcessor() {
		this.emptyStringArguments.add("x-dead-letter-exchange");
	}

	/**
	 * Set the {@link RabbitListenerEndpointRegistry} that will hold the created
	 * endpoint and manage the lifecycle of the related listener container.
	 * @param endpointRegistry the {@link RabbitListenerEndpointRegistry} to set.
	 */
	public void setEndpointRegistry(RabbitListenerEndpointRegistry endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	/**
	 * Set the name of the {@link RabbitListenerContainerFactory} to use by default.
	 * <p>If none is specified, "rabbitListenerContainerFactory" is assumed to be defined.
	 * @param containerFactoryBeanName the {@link RabbitListenerContainerFactory} bean name.
	 */
	public void setContainerFactoryBeanName(String containerFactoryBeanName) {
		this.defaultContainerFactoryBeanName = containerFactoryBeanName;
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to use to configure the message
	 * listener responsible to serve an endpoint detected by this processor.
	 * <p>By default, {@link DefaultMessageHandlerMethodFactory} is used and it
	 * can be configured further to support additional method arguments
	 * or to customize conversion and validation support. See
	 * {@link DefaultMessageHandlerMethodFactory} Javadoc for more details.
	 * @param messageHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
	 */
	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory.setMessageHandlerMethodFactory(messageHandlerMethodFactory);
	}

	/**
	 * Making a {@link BeanFactory} available is optional; if not set,
	 * {@link RabbitListenerConfigurer} beans won't get autodetected and an
	 * {@link #setEndpointRegistry endpoint registry} has to be explicitly configured.
	 * @param beanFactory the {@link BeanFactory} to be used.
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory clbf) {
			this.resolver = clbf.getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext(clbf, null);
		}
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	@Override
	public void setEnvironment(Environment environment) {
		String property = environment.getProperty(RABBIT_EMPTY_STRING_ARGUMENTS_PROPERTY, String.class);
		if (property != null) {
			this.emptyStringArguments.addAll(StringUtils.commaDelimitedListToSet(property));
		}
	}

	/**
	 * Set a charset for byte[] to String method argument conversion.
	 * @param charset the charset (default UTF-8).
	 * @since 2.2
	 */
	public void setCharset(Charset charset) {
		this.charset = charset;
	}

	MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
		return this.messageHandlerMethodFactory;
	}

	@Override
	public void afterSingletonsInstantiated() {
		this.registrar.setBeanFactory(this.beanFactory);

		if (this.beanFactory instanceof ListableBeanFactory lbf) {
			Map<String, RabbitListenerConfigurer> instances =
					lbf.getBeansOfType(RabbitListenerConfigurer.class);
			for (RabbitListenerConfigurer configurer : instances.values()) {
				configurer.configureRabbitListeners(this.registrar);
			}
		}

		if (this.registrar.getEndpointRegistry() == null) {
			if (this.endpointRegistry == null) {
				Assert.state(this.beanFactory != null,
						"BeanFactory must be set to find endpoint registry by bean name");
				this.endpointRegistry = this.beanFactory.getBean(
						RabbitListenerConfigUtils.RABBIT_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
						RabbitListenerEndpointRegistry.class);
			}
			this.registrar.setEndpointRegistry(this.endpointRegistry);
		}

		if (this.defaultContainerFactoryBeanName != null) {
			this.registrar.setContainerFactoryBeanName(this.defaultContainerFactoryBeanName);
		}

		// Set the custom handler method factory once resolved by the configurer
		MessageHandlerMethodFactory handlerMethodFactory = this.registrar.getMessageHandlerMethodFactory();
		if (handlerMethodFactory != null) {
			this.messageHandlerMethodFactory.setMessageHandlerMethodFactory(handlerMethodFactory);
		}

		// Actually register all listeners
		this.registrar.afterPropertiesSet();

		// clear the cache - prototype beans will be re-cached.
		this.typeCache.clear();
	}


	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
		Class<?> targetClass = AopUtils.getTargetClass(bean);
		final TypeMetadata metadata = this.typeCache.computeIfAbsent(targetClass, this::buildMetadata);
		for (ListenerMethod lm : metadata.listenerMethods) {
			for (RabbitListener rabbitListener : lm.annotations) {
				processAmqpListener(rabbitListener, lm.method, bean, beanName);
			}
		}
		if (metadata.handlerMethods.length > 0) {
			processMultiMethodListeners(metadata.classAnnotations, metadata.handlerMethods, bean, beanName);
		}
		return bean;
	}

	private TypeMetadata buildMetadata(Class<?> targetClass) {
		List<RabbitListener> classLevelListeners = findListenerAnnotations(targetClass);
		final boolean hasClassLevelListeners = classLevelListeners.size() > 0;
		final List<ListenerMethod> methods = new ArrayList<>();
		final List<Method> multiMethods = new ArrayList<>();
		ReflectionUtils.doWithMethods(targetClass, method -> {
			List<RabbitListener> listenerAnnotations = findListenerAnnotations(method);
			if (listenerAnnotations.size() > 0) {
				methods.add(new ListenerMethod(method,
						listenerAnnotations.toArray(new RabbitListener[listenerAnnotations.size()])));
			}
			if (hasClassLevelListeners) {
				RabbitHandler rabbitHandler = AnnotationUtils.findAnnotation(method, RabbitHandler.class);
				if (rabbitHandler != null) {
					multiMethods.add(method);
				}
			}
		}, ReflectionUtils.USER_DECLARED_METHODS
				.and(meth -> !meth.getDeclaringClass().getName().contains("$MockitoMock$")));
		if (methods.isEmpty() && multiMethods.isEmpty()) {
			return TypeMetadata.EMPTY;
		}
		return new TypeMetadata(
				methods.toArray(new ListenerMethod[methods.size()]),
				multiMethods.toArray(new Method[multiMethods.size()]),
				classLevelListeners.toArray(new RabbitListener[classLevelListeners.size()]));
	}

	private List<RabbitListener> findListenerAnnotations(AnnotatedElement element) {
		return MergedAnnotations.from(element, SearchStrategy.TYPE_HIERARCHY)
				.stream(RabbitListener.class)
				.filter(tma -> {
					Object source = tma.getSource();
					String name = "";
					if (source instanceof Class<?> clazz) {
						name = clazz.getName();
					}
					else if (source instanceof Method method) {
						name = method.getDeclaringClass().getName();
					}
					return !name.contains("$MockitoMock$");
				})
				.map(ann -> ann.synthesize())
				.collect(Collectors.toList());
	}

	private void processMultiMethodListeners(RabbitListener[] classLevelListeners, Method[] multiMethods,
			Object bean, String beanName) {

		List<Method> checkedMethods = new ArrayList<Method>();
		Method defaultMethod = null;
		for (Method method : multiMethods) {
			Method checked = checkProxy(method, bean);
			if (AnnotationUtils.findAnnotation(method, RabbitHandler.class).isDefault()) { // NOSONAR never null
				final Method toAssert = defaultMethod;
				Assert.state(toAssert == null, () -> "Only one @RabbitHandler can be marked 'isDefault', found: "
						+ toAssert.toString() + " and " + method.toString());
				defaultMethod = checked;
			}
			checkedMethods.add(checked);
		}
		for (RabbitListener classLevelListener : classLevelListeners) {
			MultiMethodRabbitListenerEndpoint endpoint =
					new MultiMethodRabbitListenerEndpoint(checkedMethods, defaultMethod, bean);
			processListener(endpoint, classLevelListener, bean, bean.getClass(), beanName);
		}
	}

	protected Collection<Declarable> processAmqpListener(RabbitListener rabbitListener, Method method, Object bean,
			String beanName) {
		Method methodToUse = checkProxy(method, bean);
		MethodRabbitListenerEndpoint endpoint = new MethodRabbitListenerEndpoint();
		endpoint.setMethod(methodToUse);
		return processListener(endpoint, rabbitListener, bean, methodToUse, beanName);
	}

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a @RabbitListener method on the target class for this JDK proxy ->
				// is it also present on the proxy itself?
				method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(), method.getParameterTypes());
						break;
					}
					catch (@SuppressWarnings("unused") NoSuchMethodException noMethod) {
					}
				}
			}
			catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			}
			catch (NoSuchMethodException ex) {
				throw new IllegalStateException(String.format(
						"@RabbitListener method '%s' found on bean target class '%s', " +
						"but not found in any interface(s) for a bean JDK proxy. Either " +
						"pull the method up to an interface or switch to subclass (CGLIB) " +
						"proxies by setting proxy-target-class/proxyTargetClass " +
						"attribute to 'true'", method.getName(), method.getDeclaringClass().getSimpleName()), ex);
			}
		}
		return method;
	}

	protected Collection<Declarable> processListener(MethodRabbitListenerEndpoint endpoint,
			RabbitListener rabbitListener, Object bean, Object target, String beanName) {

		final List<Declarable> declarables = new ArrayList<>();
		endpoint.setBean(bean);
		endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactory);
		endpoint.setId(getEndpointId(rabbitListener));
		List<Object> resolvedQueues = resolveQueues(rabbitListener, declarables);
		if (!resolvedQueues.isEmpty()) {
			if (resolvedQueues.get(0) instanceof String) {
				endpoint.setQueueNames(resolvedQueues.stream().map(o -> (String) o).toArray(String[]::new));
			}
			else {
				endpoint.setQueues(resolvedQueues.stream().map(o -> (Queue) o).toArray(Queue[]::new));
			}
		}
		endpoint.setConcurrency(resolveExpressionAsStringOrInteger(rabbitListener.concurrency(), "concurrency"));
		endpoint.setBeanFactory(this.beanFactory);
		endpoint.setReturnExceptions(resolveExpressionAsBoolean(rabbitListener.returnExceptions()));
		resolveErrorHandler(endpoint, rabbitListener);
		String group = rabbitListener.group();
		if (StringUtils.hasText(group)) {
			Object resolvedGroup = resolveExpression(group);
			if (resolvedGroup instanceof String str) {
				endpoint.setGroup(str);
			}
		}
		String autoStartup = rabbitListener.autoStartup();
		if (StringUtils.hasText(autoStartup)) {
			endpoint.setAutoStartup(resolveExpressionAsBoolean(autoStartup));
		}

		endpoint.setExclusive(rabbitListener.exclusive());
		String priority = resolveExpressionAsString(rabbitListener.priority(), "priority");
		if (StringUtils.hasText(priority)) {
			try {
				endpoint.setPriority(Integer.valueOf(priority));
			}
			catch (NumberFormatException ex) {
				throw new BeanInitializationException("Invalid priority value for " +
						rabbitListener + " (must be an integer)", ex);
			}
		}

		resolveExecutor(endpoint, rabbitListener, target, beanName);
		resolveAdmin(endpoint, rabbitListener, target);
		resolveAckMode(endpoint, rabbitListener);
		resolvePostProcessor(endpoint, rabbitListener, target, beanName);
		resolveMessageConverter(endpoint, rabbitListener, target, beanName);
		resolveReplyContentType(endpoint, rabbitListener);
		if (StringUtils.hasText(rabbitListener.batch())) {
			endpoint.setBatchListener(Boolean.parseBoolean(rabbitListener.batch()));
		}
		RabbitListenerContainerFactory<?> factory = resolveContainerFactory(rabbitListener, target, beanName);

		this.registrar.registerEndpoint(endpoint, factory);
		return declarables;
	}

	private void resolveErrorHandler(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener) {
		Object errorHandler = resolveExpression(rabbitListener.errorHandler());
		if (errorHandler instanceof RabbitListenerErrorHandler rleh) {
			endpoint.setErrorHandler(rleh);
		}
		else {
			String errorHandlerBeanName = resolveExpressionAsString(rabbitListener.errorHandler(), "errorHandler");
			if (StringUtils.hasText(errorHandlerBeanName)) {
				endpoint.setErrorHandler(
						this.beanFactory.getBean(errorHandlerBeanName, RabbitListenerErrorHandler.class));
			}
		}
	}

	private void resolveAckMode(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener) {
		String ackModeAttr = rabbitListener.ackMode();
		if (StringUtils.hasText(ackModeAttr)) {
			Object ackMode = resolveExpression(ackModeAttr);
			if (ackMode instanceof String str) {
				endpoint.setAckMode(AcknowledgeMode.valueOf(str));
			}
			else if (ackMode instanceof AcknowledgeMode mode) {
				endpoint.setAckMode(mode);
			}
			else {
				Assert.isNull(ackMode, "ackMode must resolve to a String or AcknowledgeMode");
			}
		}
	}

	private void resolveAdmin(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener, Object adminTarget) {
		Object resolved = resolveExpression(rabbitListener.admin());
		if (resolved instanceof AmqpAdmin admin) {
			endpoint.setAdmin(admin);
		}
		else {
			String rabbitAdmin = resolveExpressionAsString(rabbitListener.admin(), "admin");
			if (StringUtils.hasText(rabbitAdmin)) {
				Assert.state(this.beanFactory != null, "BeanFactory must be set to resolve RabbitAdmin by bean name");
				try {
					endpoint.setAdmin(this.beanFactory.getBean(rabbitAdmin, RabbitAdmin.class));
				}
				catch (NoSuchBeanDefinitionException ex) {
					throw new BeanInitializationException("Could not register rabbit listener endpoint on [" +
							adminTarget + "], no " + RabbitAdmin.class.getSimpleName() + " with id '" +
							rabbitAdmin + "' was found in the application context", ex);
				}
			}
		}
	}

	@Nullable
	private RabbitListenerContainerFactory<?> resolveContainerFactory(RabbitListener rabbitListener,
			Object factoryTarget, String beanName) {

		RabbitListenerContainerFactory<?> factory = null;
		Object resolved = resolveExpression(rabbitListener.containerFactory());
		if (resolved instanceof RabbitListenerContainerFactory<?> rlcf) {
			return rlcf;
		}
		String containerFactoryBeanName = resolveExpressionAsString(rabbitListener.containerFactory(),
				"containerFactory");
		if (StringUtils.hasText(containerFactoryBeanName)) {
			assertBeanFactory();
			try {
				factory = this.beanFactory.getBean(containerFactoryBeanName, RabbitListenerContainerFactory.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException(
						noBeanFoundMessage(factoryTarget, beanName, containerFactoryBeanName,
								RabbitListenerContainerFactory.class), ex);
			}
		}
		return factory;
	}

	private void resolveExecutor(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener,
			Object execTarget, String beanName) {

		Object resolved = resolveExpression(rabbitListener.executor());
		if (resolved instanceof TaskExecutor tex) {
			endpoint.setTaskExecutor(tex);
		}
		else {
			String execBeanName = resolveExpressionAsString(rabbitListener.executor(), "executor");
			if (StringUtils.hasText(execBeanName)) {
				assertBeanFactory();
				try {
					endpoint.setTaskExecutor(this.beanFactory.getBean(execBeanName, TaskExecutor.class));
				}
				catch (NoSuchBeanDefinitionException ex) {
					throw new BeanInitializationException(
							noBeanFoundMessage(execTarget, beanName, execBeanName, TaskExecutor.class), ex);
				}
			}
		}
	}

	private void resolvePostProcessor(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener,
			Object target, String beanName) {

		Object resolved = resolveExpression(rabbitListener.replyPostProcessor());
		if (resolved instanceof ReplyPostProcessor rpp) {
			endpoint.setReplyPostProcessor(rpp);
		}
		else {
			String ppBeanName = resolveExpressionAsString(rabbitListener.replyPostProcessor(), "replyPostProcessor");
			if (StringUtils.hasText(ppBeanName)) {
				assertBeanFactory();
				try {
					endpoint.setReplyPostProcessor(this.beanFactory.getBean(ppBeanName, ReplyPostProcessor.class));
				}
				catch (NoSuchBeanDefinitionException ex) {
					throw new BeanInitializationException(
							noBeanFoundMessage(target, beanName, ppBeanName, ReplyPostProcessor.class), ex);
				}
			}
		}
	}

	private void resolveMessageConverter(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener,
			Object target, String beanName) {

		Object resolved = resolveExpression(rabbitListener.messageConverter());
		if (resolved instanceof MessageConverter converter) {
			endpoint.setMessageConverter(converter);
		}
		else {
			String mcBeanName = resolveExpressionAsString(rabbitListener.messageConverter(), "messageConverter");
			if (StringUtils.hasText(mcBeanName)) {
				assertBeanFactory();
				try {
					endpoint.setMessageConverter(this.beanFactory.getBean(mcBeanName, MessageConverter.class));
				}
				catch (NoSuchBeanDefinitionException ex) {
					throw new BeanInitializationException(
							noBeanFoundMessage(target, beanName, mcBeanName, MessageConverter.class), ex);
				}
			}
		}
	}

	private void resolveReplyContentType(MethodRabbitListenerEndpoint endpoint, RabbitListener rabbitListener) {
		String contentType = resolveExpressionAsString(rabbitListener.replyContentType(), "replyContentType");
		if (StringUtils.hasText(contentType)) {
			endpoint.setReplyContentType(contentType);
			endpoint.setConverterWinsContentType(resolveExpressionAsBoolean(rabbitListener.converterWinsContentType()));
		}
	}

	protected void assertBeanFactory() {
		Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
	}

	protected String noBeanFoundMessage(Object target, String listenerBeanName, String requestedBeanName,
			Class<?> expectedClass) {
		return "Could not register rabbit listener endpoint on ["
				+ target + "] for bean " + listenerBeanName + ", no '" + expectedClass.getSimpleName() + "' with id '"
				+ requestedBeanName + "' was found in the application context";
	}

	private String getEndpointId(RabbitListener rabbitListener) {
		if (StringUtils.hasText(rabbitListener.id())) {
			return resolveExpressionAsString(rabbitListener.id(), "id");
		}
		else {
			return "org.springframework.amqp.rabbit.RabbitListenerEndpointContainer#" + this.counter.getAndIncrement();
		}
	}

	private List<Object> resolveQueues(RabbitListener rabbitListener, Collection<Declarable> declarables) {
		String[] queues = rabbitListener.queues();
		QueueBinding[] bindings = rabbitListener.bindings();
		org.springframework.amqp.rabbit.annotation.Queue[] queuesToDeclare = rabbitListener.queuesToDeclare();
		List<String> queueNames = new ArrayList<>();
		List<Queue> queueBeans = new ArrayList<>();
		for (String queue : queues) {
			resolveQueues(queue, queueNames, queueBeans);
		}
		if (!queueNames.isEmpty()) {
			// revert to the previous behavior of just using the name when there is mixture of String and Queue
			queueBeans.forEach(qb -> queueNames.add(qb.getName()));
			queueBeans.clear();
		}
		if (queuesToDeclare.length > 0) {
			if (queues.length > 0) {
				throw new BeanInitializationException(
						"@RabbitListener can have only one of 'queues', 'queuesToDeclare', or 'bindings'");
			}
			for (org.springframework.amqp.rabbit.annotation.Queue queue : queuesToDeclare) {
				queueNames.add(declareQueue(queue, declarables));
			}
		}
		if (bindings.length > 0) {
			if (queues.length > 0 || queuesToDeclare.length > 0) {
				throw new BeanInitializationException(
						"@RabbitListener can have only one of 'queues', 'queuesToDeclare', or 'bindings'");
			}
			return Arrays.stream(registerBeansForDeclaration(rabbitListener, declarables))
					.map(s -> (Object) s)
					.collect(Collectors.toList());
		}
		return queueNames.isEmpty()
				? queueBeans.stream()
						.map(s -> (Object) s)
						.collect(Collectors.toList())
				: queueNames.stream()
						.map(s -> (Object) s)
						.collect(Collectors.toList());

	}

	private void resolveQueues(String queue, List<String> result, List<Queue> queueBeans) {
		resolveAsStringOrQueue(resolveExpression(queue), result, queueBeans, "queues");
	}

	@SuppressWarnings("unchecked")
	private void resolveAsStringOrQueue(Object resolvedValue, List<String> names, @Nullable List<Queue> queues,
			String what) {

		Object resolvedValueToUse = resolvedValue;
		if (resolvedValue instanceof String[] strings) {
			resolvedValueToUse = Arrays.asList(strings);
		}
		if (queues != null && resolvedValueToUse instanceof Queue q) {
			if (!names.isEmpty()) {
				// revert to the previous behavior of just using the name when there is mixture of String and Queue
				names.add(q.getName());
			}
			else {
				queues.add(q);
			}
		}
		else if (resolvedValueToUse instanceof String str) {
			names.add(str);
		}
		else if (resolvedValueToUse instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValueToUse) {
				resolveAsStringOrQueue(object, names, queues, what);
			}
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@RabbitListener."
					+ what
					+ " can't resolve '%s' as a String[] or a String "
					+ (queues != null ? "or a Queue" : ""),
					resolvedValue));
		}
	}

	private String[] registerBeansForDeclaration(RabbitListener rabbitListener, Collection<Declarable> declarables) {
		List<String> queues = new ArrayList<String>();
		if (this.beanFactory instanceof ConfigurableBeanFactory) {
			for (QueueBinding binding : rabbitListener.bindings()) {
				String queueName = declareQueue(binding.value(), declarables);
				queues.add(queueName);
				declareExchangeAndBinding(binding, queueName, declarables);
			}
		}
		return queues.toArray(new String[0]);
	}

	private String declareQueue(org.springframework.amqp.rabbit.annotation.Queue bindingQueue,
			Collection<Declarable> declarables) {
		String queueName = (String) resolveExpression(bindingQueue.value());
		boolean isAnonymous = false;
		if (!StringUtils.hasText(queueName)) {
			queueName = Base64UrlNamingStrategy.DEFAULT.generateName();
			// default exclusive/autodelete and non-durable when anonymous
			isAnonymous = true;
		}
		Queue queue = new Queue(queueName,
				resolveExpressionAsBoolean(bindingQueue.durable(), !isAnonymous),
				resolveExpressionAsBoolean(bindingQueue.exclusive(), isAnonymous),
				resolveExpressionAsBoolean(bindingQueue.autoDelete(), isAnonymous),
				resolveArguments(bindingQueue.arguments()));
		queue.setIgnoreDeclarationExceptions(resolveExpressionAsBoolean(bindingQueue.ignoreDeclarationExceptions()));
		((ConfigurableBeanFactory) this.beanFactory).registerSingleton(queueName + ++this.increment, queue);
		if (bindingQueue.admins().length > 0) {
			queue.setAdminsThatShouldDeclare((Object[]) bindingQueue.admins());
		}
		queue.setShouldDeclare(resolveExpressionAsBoolean(bindingQueue.declare()));
		declarables.add(queue);
		return queueName;
	}

	private void declareExchangeAndBinding(QueueBinding binding, String queueName, Collection<Declarable> declarables) {
		org.springframework.amqp.rabbit.annotation.Exchange bindingExchange = binding.exchange();
		String exchangeName = resolveExpressionAsString(bindingExchange.value(), "@Exchange.exchange");
		Assert.isTrue(StringUtils.hasText(exchangeName), () -> "Exchange name required; binding queue " + queueName);
		String exchangeType = resolveExpressionAsString(bindingExchange.type(), "@Exchange.type");

		ExchangeBuilder exchangeBuilder = new ExchangeBuilder(exchangeName, exchangeType);

		if (resolveExpressionAsBoolean(bindingExchange.autoDelete())) {
			exchangeBuilder.autoDelete();
		}

		if (resolveExpressionAsBoolean(bindingExchange.internal())) {
			exchangeBuilder.internal();
		}

		if (resolveExpressionAsBoolean(bindingExchange.delayed())) {
			exchangeBuilder.delayed();
		}

		if (resolveExpressionAsBoolean(bindingExchange.ignoreDeclarationExceptions())) {
			exchangeBuilder.ignoreDeclarationExceptions();
		}

		if (!resolveExpressionAsBoolean(bindingExchange.declare())) {
			exchangeBuilder.suppressDeclaration();
		}

		if (bindingExchange.admins().length > 0) {
			exchangeBuilder.admins((Object[]) bindingExchange.admins());
		}

		Map<String, Object> arguments = resolveArguments(bindingExchange.arguments());

		if (!CollectionUtils.isEmpty(arguments)) {
			exchangeBuilder.withArguments(arguments);
		}


		org.springframework.amqp.core.Exchange exchange =
				exchangeBuilder.durable(resolveExpressionAsBoolean(bindingExchange.durable()))
						.build();

		((ConfigurableBeanFactory) this.beanFactory)
				.registerSingleton(exchangeName + ++this.increment, exchange);
		registerBindings(binding, queueName, exchangeName, exchangeType, declarables);
		declarables.add(exchange);
	}

	private void registerBindings(QueueBinding binding, String queueName, String exchangeName, String exchangeType,
			Collection<Declarable> declarables) {
		final List<String> routingKeys;
		if (exchangeType.equals(ExchangeTypes.FANOUT) || binding.key().length == 0) {
			routingKeys = Collections.singletonList("");
		}
		else {
			final int length = binding.key().length;
			routingKeys = new ArrayList<>();
			for (int i = 0; i < length; ++i) {
				resolveAsStringOrQueue(resolveExpression(binding.key()[i]), routingKeys, null, "@QueueBinding.key");
			}
		}
		final Map<String, Object> bindingArguments = resolveArguments(binding.arguments());
		final boolean bindingIgnoreExceptions = resolveExpressionAsBoolean(binding.ignoreDeclarationExceptions());
		boolean declare = resolveExpressionAsBoolean(binding.declare());
		for (String routingKey : routingKeys) {
			final Binding actualBinding = new Binding(queueName, DestinationType.QUEUE, exchangeName, routingKey,
					bindingArguments);
			actualBinding.setIgnoreDeclarationExceptions(bindingIgnoreExceptions);
			actualBinding.setShouldDeclare(declare);
			if (binding.admins().length > 0) {
				actualBinding.setAdminsThatShouldDeclare((Object[]) binding.admins());
			}
			((ConfigurableBeanFactory) this.beanFactory)
					.registerSingleton(exchangeName + "." + queueName + ++this.increment, actualBinding);
			declarables.add(actualBinding);
		}
	}

	private Map<String, Object> resolveArguments(Argument[] arguments) {
		Map<String, Object> map = new HashMap<>();
		for (Argument arg : arguments) {
			String key = resolveExpressionAsString(arg.name(), "@Argument.name");
			if (StringUtils.hasText(key)) {
				Object value = resolveExpression(arg.value());
				Object type = resolveExpression(arg.type());
				Class<?> typeClass;
				String typeName;
				if (type instanceof Class<?> clazz) {
					typeClass = clazz;
					typeName = typeClass.getName();
				}
				else {
					Assert.isTrue(type instanceof String, () -> "Type must resolve to a Class or String, but resolved to ["
							+ type.getClass().getName() + "]");
					typeName = (String) type;
					try {
						typeClass = ClassUtils.forName(typeName, this.beanClassLoader);
					}
					catch (Exception e) {
						throw new IllegalStateException("Could not load class", e);
					}
				}
				addToMap(map, key, value, typeClass, typeName);
			}
			else {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("@Argument ignored because the name resolved to an empty String");
				}
			}
		}
		return map.size() < 1 ? null : map;
	}

	private void addToMap(Map<String, Object> map, String key, Object value, Class<?> typeClass, String typeName) {
		if (value.getClass().getName().equals(typeName)) {
			if (typeClass.equals(String.class) && !StringUtils.hasText((String) value)) {
				putEmpty(map, key);
			}
			else {
				map.put(key, value);
			}
		}
		else {
			if (value instanceof String && !StringUtils.hasText((String) value)) {
				putEmpty(map, key);
			}
			else {
				if (CONVERSION_SERVICE.canConvert(value.getClass(), typeClass)) {
					map.put(key, CONVERSION_SERVICE.convert(value, typeClass));
				}
				else {
					throw new IllegalStateException("Cannot convert from " + value.getClass().getName()
							+ " to " + typeName);
				}
			}
		}
	}

	private void putEmpty(Map<String, Object> map, String key) {
		if (this.emptyStringArguments.contains(key)) {
			map.put(key, "");
		}
		else {
			map.put(key, null);
		}
	}

	private boolean resolveExpressionAsBoolean(String value) {
		return resolveExpressionAsBoolean(value, false);
	}

	private boolean resolveExpressionAsBoolean(String value, boolean defaultValue) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof Boolean bool) {
			return bool;
		}
		else if (resolved instanceof String str) {
			return StringUtils.hasText(str) ? Boolean.parseBoolean(str) : defaultValue;
		}
		else {
			return defaultValue;
		}
	}

	protected String resolveExpressionAsString(String value, String attribute) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof String str) {
			return str;
		}
		else {
			throw new IllegalStateException("The [" + attribute + "] must resolve to a String. "
					+ "Resolved to [" + resolved.getClass() + "] for [" + value + "]");
		}
	}

	private String resolveExpressionAsStringOrInteger(String value, String attribute) {
		if (!StringUtils.hasLength(value)) {
			return null;
		}
		Object resolved = resolveExpression(value);
		if (resolved instanceof String str) {
			return str;
		}
		else if (resolved instanceof Integer) {
			return resolved.toString();
		}
		else {
			throw new IllegalStateException("The [" + attribute + "] must resolve to a String. "
					+ "Resolved to [" + resolved.getClass() + "] for [" + value + "]");
		}
	}

	private Object resolveExpression(String value) {
		String resolvedValue = resolve(value);

		return this.resolver.evaluate(resolvedValue, this.expressionContext);
	}

	/**
	 * Resolve the specified value if possible.
	 * @param value the value to resolve.
	 * @return the resolved value.
	 * @see ConfigurableBeanFactory#resolveEmbeddedValue
	 */
	private String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory cbf) {
			return cbf.resolveEmbeddedValue(value);
		}
		return value;
	}

	/**
	 * An {@link MessageHandlerMethodFactory} adapter that offers a configurable underlying
	 * instance to use. Useful if the factory to use is determined once the endpoints
	 * have been registered but not created yet.
	 * @see RabbitListenerEndpointRegistrar#setMessageHandlerMethodFactory
	 */
	private class RabbitHandlerMethodFactoryAdapter implements MessageHandlerMethodFactory {

		private final DefaultFormattingConversionService defaultFormattingConversionService =
				new DefaultFormattingConversionService();

		private MessageHandlerMethodFactory factory;

		RabbitHandlerMethodFactoryAdapter() {
		}

		public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory rabbitHandlerMethodFactory1) {
			this.factory = rabbitHandlerMethodFactory1;
		}

		@Override
		public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
			return getFactory().createInvocableHandlerMethod(bean, method);
		}

		private MessageHandlerMethodFactory getFactory() {
			if (this.factory == null) {
				this.factory = createDefaultMessageHandlerMethodFactory();
			}
			return this.factory;
		}

		private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
			DefaultMessageHandlerMethodFactory defaultFactory = new AmqpMessageHandlerMethodFactory();
			Validator validator = RabbitListenerAnnotationBeanPostProcessor.this.registrar.getValidator();
			if (validator != null) {
				defaultFactory.setValidator(validator);
			}
			defaultFactory.setBeanFactory(RabbitListenerAnnotationBeanPostProcessor.this.beanFactory);
			this.defaultFormattingConversionService.addConverter(
					new BytesToStringConverter(RabbitListenerAnnotationBeanPostProcessor.this.charset));
			defaultFactory.setConversionService(this.defaultFormattingConversionService);

			List<HandlerMethodArgumentResolver> customArgumentsResolver = new ArrayList<>(
					RabbitListenerAnnotationBeanPostProcessor.this.registrar.getCustomMethodArgumentResolvers());
			defaultFactory.setCustomArgumentResolvers(customArgumentsResolver);
			defaultFactory.setMessageConverter(new GenericMessageConverter(this.defaultFormattingConversionService));

			defaultFactory.afterPropertiesSet();
			return defaultFactory;
		}

	}

	/**
	 * The metadata holder of the class with {@link RabbitListener}
	 * and {@link RabbitHandler} annotations.
	 */
	private static class TypeMetadata {

		/**
		 * Methods annotated with {@link RabbitListener}.
		 */
		final ListenerMethod[] listenerMethods; // NOSONAR

		/**
		 * Methods annotated with {@link RabbitHandler}.
		 */
		final Method[] handlerMethods; // NOSONAR

		/**
		 * Class level {@link RabbitListener} annotations.
		 */
		final RabbitListener[] classAnnotations; // NOSONAR

		static final TypeMetadata EMPTY = new TypeMetadata();

		private TypeMetadata() {
			this.listenerMethods = new ListenerMethod[0];
			this.handlerMethods = new Method[0];
			this.classAnnotations = new RabbitListener[0];
		}

		TypeMetadata(ListenerMethod[] methods, Method[] multiMethods, RabbitListener[] classLevelListeners) { // NOSONAR
			this.listenerMethods = methods; // NOSONAR
			this.handlerMethods = multiMethods; // NOSONAR
			this.classAnnotations = classLevelListeners; // NOSONAR
		}

	}

	/**
	 * A method annotated with {@link RabbitListener}, together with the annotations.
	 *
	 * @param method      the method with annotations
	 * @param annotations on the method
	 */
	private record ListenerMethod(Method method, RabbitListener[] annotations) {
	}

	private record BytesToStringConverter(Charset charset) implements Converter<byte[], String> {

		@Override
		public String convert(byte[] source) {
			return new String(source, this.charset);
		}

	}

}
