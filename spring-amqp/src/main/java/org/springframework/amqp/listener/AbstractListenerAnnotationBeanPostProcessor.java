/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.listener;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
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
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.env.Environment;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * The {@link BeanPostProcessor} base for listener annotation parsers.
 *
 * @param <A> the listener annotation type.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 */
public abstract class AbstractListenerAnnotationBeanPostProcessor<A extends Annotation>
		implements BeanPostProcessor, Ordered, BeanFactoryAware, BeanClassLoaderAware, EnvironmentAware,
		SmartInitializingSingleton {

	protected static final ConversionService CONVERSION_SERVICE = new DefaultConversionService();

	protected final Log logger = LogFactory.getLog(getClass());

	protected final ConcurrentMap<Class<?>, TypeMetadata<A>> typeCache = new ConcurrentHashMap<>();

	@SuppressWarnings("NullAway.Init")
	private BeanFactory beanFactory;

	@SuppressWarnings("NullAway.Init")
	private Environment environment;

	@SuppressWarnings("NullAway.Init")
	private ClassLoader beanClassLoader;

	private BeanExpressionResolver resolver = new StandardBeanExpressionResolver();

	@SuppressWarnings("NullAway.Init")
	private BeanExpressionContext expressionContext;

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory clbf) {
			BeanExpressionResolver beanExpressionResolver = clbf.getBeanExpressionResolver();
			if (beanExpressionResolver != null) {
				this.resolver = beanExpressionResolver;
			}
			this.expressionContext = new BeanExpressionContext(clbf, null);
		}
	}

	protected BeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	protected ClassLoader getBeanClassLoader() {
		return this.beanClassLoader;
	}

	@Override
	public void afterSingletonsInstantiated() {
		// clear the cache - prototype beans will be re-cached.
		this.typeCache.clear();
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
		Class<?> targetClass = AopUtils.getTargetClass(bean);
		TypeMetadata<A> metadata = this.typeCache.computeIfAbsent(targetClass, this::buildMetadata);
		for (ListenerMethod<A> lm : metadata.listenerMethods()) {
			for (A listenerAnnotation : lm.annotations()) {
				doProcessAmqpListener(listenerAnnotation, lm.method(), bean, beanName);
			}
		}
		if (!metadata.handlerMethods().isEmpty()) {
			processMultiMethodListeners(metadata.classAnnotations(), metadata.handlerMethods(), bean, beanName);
		}
		return bean;
	}

	protected abstract TypeMetadata<A> buildMetadata(Class<?> clazz);

	protected abstract void doProcessAmqpListener(A listenerAnnotation, Method method, Object bean, String beanName);

	protected abstract void processMultiMethodListeners(List<A> classLevelListeners, List<Method> multiMethods,
			Object bean, String beanName);

	protected static Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a listener annotated method on the target class for this JDK proxy ->
				// is it also present on the proxy itself?
				method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(), method.getParameterTypes());
						break;
					}
					catch (@SuppressWarnings("unused") NoSuchMethodException noMethod) {
						// Ignore
					}
				}
			}
			catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			}
			catch (NoSuchMethodException ex) {
				throw new IllegalStateException(String.format(
						"The annotated listener method '%s' found on bean target class '%s', " +
								"but not found in any interface(s) for a bean JDK proxy. Either " +
								"pull the method up to an interface or switch to subclass (CGLIB) " +
								"proxies by setting proxy-target-class/proxyTargetClass " +
								"attribute to 'true'", method.getName(), method.getDeclaringClass().getSimpleName()), ex);
			}
		}
		return method;
	}

	protected static String noBeanFoundMessage(Object target, String listenerBeanName, String requestedBeanName,
			Class<?> expectedClass) {

		return "Could not register listener endpoint on ["
				+ target + "] for bean " + listenerBeanName + ", no '" + expectedClass.getSimpleName() + "' with id '"
				+ requestedBeanName + "' was found in the application context";
	}

	protected boolean resolveExpressionAsBoolean(String value) {
		return resolveExpressionAsBoolean(value, false);
	}

	protected boolean resolveExpressionAsBoolean(String value, boolean defaultValue) {
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

	protected @Nullable String resolveExpressionAsString(String value, String attribute) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof String str) {
			return str;
		}
		else {
			throw new IllegalStateException("The [" + attribute + "] must resolve to a String. "
					+ "Resolved to [" + resolved + "] for [" + value + "]");
		}
	}

	protected @Nullable String resolveExpressionAsStringOrInteger(String value, String attribute) {
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
					+ "Resolved to [" + resolved + "] for [" + value + "]");
		}
	}

	protected @Nullable Integer resolveExpressionToInteger(String value, String attribute) {
		if (!StringUtils.hasLength(value)) {
			return null;
		}
		Object resolved = resolveExpression(value);
		if (resolved instanceof String str) {
			return Integer.parseInt(str);
		}
		else if (resolved instanceof Number number) {
			return number.intValue();
		}
		else {
			throw new IllegalStateException("The [" + attribute + "] must resolve to a Integer. "
					+ "Resolved to [" + resolved + "] for [" + value + "]");
		}
	}

	@SuppressWarnings("unchecked")
	protected <B> @Nullable B resolveExpressionToBean(String attributeValue, String attributeName,
			Object annotationTarget, String beanName, Class<B> beanType) {

		Object resolved = resolveExpression(attributeValue);
		if (resolved != null && beanType.isAssignableFrom(resolved.getClass())) {
			return (B) resolved;
		}
		else if (resolved instanceof String factoryBeanName && StringUtils.hasText(factoryBeanName)) {
			try {
				return getBeanFactory().getBean(factoryBeanName, beanType);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException(
						noBeanFoundMessage(annotationTarget, beanName, factoryBeanName, beanType), ex);
			}
		}
		else if (resolved != null && StringUtils.hasText(resolved.toString())) {
			throw new IllegalStateException("The '" + attributeName + "()' attribute must resolve to a 'String' " +
					"or '" + beanType + "'. Resolved to '" + resolved + "' for '" + attributeValue + "'.");
		}
		return null;
	}

	protected @Nullable Object resolveExpression(String value) {
		return this.resolver.evaluate(resolve(value), this.expressionContext);
	}

	/**
	 * Resolve the specified value if possible.
	 * @param value the value to resolve.
	 * @return the resolved value.
	 * @see ConfigurableBeanFactory#resolveEmbeddedValue
	 */
	private @Nullable String resolve(String value) {
		if (this.beanFactory instanceof ConfigurableBeanFactory cbf) {
			return cbf.resolveEmbeddedValue(value);
		}
		return value;
	}

	protected static Duration toDuration(String value, TimeUnit timeUnit) {
		if (isDurationString(value)) {
			return Duration.parse(value);
		}
		return toDuration(Long.parseLong(value), timeUnit);
	}

	protected static boolean isDurationString(String value) {
		return (value.length() > 1 && (isP(value.charAt(0)) || isP(value.charAt(1))));
	}

	protected static boolean isP(char ch) {
		return (ch == 'P' || ch == 'p');
	}

	private static Duration toDuration(long value, TimeUnit timeUnit) {
		return Duration.of(value, timeUnit.toChronoUnit());
	}

	@SuppressWarnings("unchecked")
	protected static <A extends Annotation> TypeMetadata<A> buildMetadata(Class<?> targetClass,
			Class<A> listenerAnnotation, @Nullable Class<? extends Annotation> handlerAnnotation) {

		List<A> classLevelListeners = findListenerAnnotations(targetClass, listenerAnnotation);
		boolean hasClassLevelListeners = handlerAnnotation != null && !classLevelListeners.isEmpty();
		List<ListenerMethod<A>> methods = new ArrayList<>();
		List<Method> multiMethods = new ArrayList<>();
		ReflectionUtils.doWithMethods(targetClass,
				method -> {
					List<A> listenerAnnotations = findListenerAnnotations(method, listenerAnnotation);
					if (!listenerAnnotations.isEmpty()) {
						methods.add(new ListenerMethod<>(method, listenerAnnotations));
					}
					if (hasClassLevelListeners) {
						Annotation handler = AnnotationUtils.findAnnotation(method, handlerAnnotation);
						if (handler != null) {
							multiMethods.add(method);
						}
					}
				},
				ReflectionUtils.USER_DECLARED_METHODS
						.and(meth -> !meth.getDeclaringClass().getName().contains("$MockitoMock$")));

		if (methods.isEmpty() && multiMethods.isEmpty()) {
			return (TypeMetadata<A>) TypeMetadata.EMPTY;
		}
		return new TypeMetadata<>(methods, multiMethods, classLevelListeners);
	}

	private static <A extends Annotation> List<A> findListenerAnnotations(AnnotatedElement element,
			Class<A> annotationType) {

		return MergedAnnotations.from(element, MergedAnnotations.SearchStrategy.TYPE_HIERARCHY)
				.stream(annotationType)
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
				.map(MergedAnnotation::synthesize)
				.collect(Collectors.toList());
	}

	/**
	 * The metadata holder of the class with {@link A}  and {@code handler} annotations.
	 *
	 * @param listenerMethods methods annotated with {@link A}.
	 * @param handlerMethods methods annotated with {@code handler} annotation.
	 * @param classAnnotations class level {@link A} annotations.
	 * @param <A> the annotation type.
	 */
	protected record TypeMetadata<A extends Annotation>(
			List<ListenerMethod<A>> listenerMethods,
			List<Method> handlerMethods,
			List<A> classAnnotations) {

		private static final TypeMetadata<?> EMPTY = new TypeMetadata<>(List.of(), List.of(), List.of());

	}

	/**
	 * A method annotated with {@link A}, together with the annotations.
	 *
	 * @param method the method with annotations
	 * @param annotations on the method
	 * @param <A> the annotation type.
	 */
	protected record ListenerMethod<A extends Annotation>(Method method, List<A> annotations) {

	}

}
