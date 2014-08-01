/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

/**
 * Enable Rabbit listener annotated endpoints that are created under the cover
 * by a {@link org.springframework.amqp.rabbit.config.RabbitListenerContainerFactory
 * RabbitListenerContainerFactory}. To be used on
 * {@link org.springframework.context.annotation.Configuration Configuration}
 * classes as follows:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRabbit
 * public class AppConfig {
 *     &#064;Bean
 *     public SimpleRabbitListenerContainerFactory myRabbitListenerContainerFactory() {
 *       SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
 *       factory.setConnectionFactory(connectionFactory());
 *       factory.setMaxConcurrentConsumers(5);
 *       return factory;
 *     }
 *     // other &#064;Bean definitions
 * }</pre>
 *
 * The {@code RabbitListenerContainerFactory} is responsible to create the listener container
 * responsible for a particular endpoint. Typical implementations, as the
 * {@link org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory SimpleRabbitListenerContainerFactory}
 * used in the sample above, provides the necessary configuration options that are supported by
 * the underlying {@link org.springframework.amqp.rabbit.listener.MessageListenerContainer MessageListenerContainer}.
 *
 * <p>{@code @EnableRabbit} enables detection of {@link RabbitListener} annotations on any
 * Spring-managed bean in the container. For example, given a class {@code MyService}:
 *
 * <pre class="code">
 * package com.acme.foo;
 *
 * public class MyService {
 *     &#064;RabbitListener(containerFactory="myRabbitListenerContainerFactory", queues="myQueue")
 *     public void process(String msg) {
 *         // process incoming message
 *     }
 * }</pre>
 *
 * The container factory to use is identified by the {@link RabbitListener#containerFactory() containerFactory}
 * attribute defining the name of the {@code RabbitListenerContainerFactory} bean to use.  When none
 * is set a {@code RabbitListenerContainerFactory} bean with name {@code rabbitListenerContainerFactory} is
 * assumed to be present.
 *
 * <p>the following configuration would ensure that every time a {@link org.springframework.amqp.core.Message}
 * is received on the {@link org.springframework.amqp.core.Queue} named "myQueue", {@code MyService.process()}
 * is called with the content of the message:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRabbit
 * public class AppConfig {
 *     &#064;Bean
 *     public MyService myService() {
 *         return new MyService();
 *     }
 *
 *     // Rabbit infrastructure setup
 * }</pre>
 *
 * Alternatively, if {@code MyService} were annotated with {@code @Component}, the
 * following configuration would ensure that its {@code @RabbitListener} annotated
 * method is invoked with a matching incoming message:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRabbit
 * &#064;ComponentScan(basePackages="com.acme.foo")
 * public class AppConfig {
 * }</pre>
 *
 * Note that the created containers are not registered against the application context
 * but can be easily located for management purposes using the
 * {@link org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistry RabbitListenerEndpointRegistry}.
 *
 * <p>Annotated methods can use flexible signature; in particular, it is possible to use
 * the {@link org.springframework.messaging.Message Message} abstraction and related annotations,
 * see {@link RabbitListener} Javadoc for more details. For instance, the following would
 * inject the content of the message and a a custom "myCounter" AMQP header:
 *
 * <pre class="code">
 * &#064;RabbitListener(containerFactory = "myRabbitListenerContainerFactory", queues = "myQueue")
 * public void process(String msg, @Header("myCounter") int counter) {
 *     // process incoming message
 * }</pre>
 *
 * These features are abstracted by the {@link org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory
 * MessageHandlerMethodFactory} that is responsible to build the necessary invoker to process
 * the annotated method. By default, {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory
 * DefaultMessageHandlerMethodFactory} is used.
 *
 * <p>When more control is desired, a {@code @Configuration} class may implement
 * {@link RabbitListenerConfigurer}. This allows access to the underlying
 * {@link org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistrar RabbitListenerEndpointRegistrar}
 * instance. The following example demonstrates how to specify an explicit default
 * {@code RabbitListenerContainerFactory}
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRabbit
 * public class AppConfig implements RabbitListenerConfigurer {
 *     &#064;Override
 *     public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
 *         registrar.setContainerFactory(myRabbitListenerContainerFactory());
 *     }
 *
 *     &#064;Bean
 *     public RabbitListenerContainerFactory<?> myRabbitListenerContainerFactory() {
 *         // factory settings
 *     }
 *
 *     &#064;Bean
 *     public MyService myService() {
 *         return new MyService();
 *     }
 * }</pre>
 *
 * For reference, the example above can be compared to the following Spring XML
 * configuration:
 * <pre class="code">
 * {@code <beans>
 *     <rabbit:annotation-driven container-factory="myRabbitListenerContainerFactory"/>
 *
 *     <bean id="myRabbitListenerContainerFactory"
 *           class="org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory">
 *           // factory settings
 *     </bean>
 *
 *     <bean id="myService" class="com.acme.foo.MyService"/>
 * </beans>
 * }</pre>
 *
 * It is also possible to specify a custom {@link org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistry
 * RabbitListenerEndpointRegistry} in case you need more control on the way the containers
 * are created and managed. The example below also demonstrates how to customize the
 * {@code RabbitHandlerMethodFactory} to use with a custom {@link org.springframework.validation.Validator
 * Validator} so that payloads annotated with {@link org.springframework.validation.annotation.Validated
 * Validated} are first validated against a custom {@code Validator}.
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRabbit
 * public class AppConfig implements RabbitListenerConfigurer {
 *     &#064;Override
 *     public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
 *         registrar.setEndpointRegistry(myRabbitListenerEndpointRegistry());
 *         registrar.setMessageHandlerMethodFactory(myMessageHandlerMethodFactory);
 *     }
 *
 *     &#064;Bean
 *     public RabbitListenerEndpointRegistry<?> myRabbitListenerEndpointRegistry() {
 *         // registry configuration
 *     }
 *
 *     &#064;Bean
 *     public RabbitHandlerMethodFactory myMessageHandlerMethodFactory() {
 *        DefaultRabbitHandlerMethodFactory factory = new DefaultRabbitHandlerMethodFactory();
 *        factory.setValidator(new MyValidator());
 *        return factory;
 *     }
 *
 *     &#064;Bean
 *     public MyService myService() {
 *         return new MyService();
 *     }
 * }</pre>
 *
 * For reference, the example above can be compared to the following Spring XML
 * configuration:
 * <pre class="code">
 * {@code <beans>
 *     <rabbit:annotation-driven registry="myRabbitListenerEndpointRegistry"
 *         handler-method-factory="myRabbitHandlerMethodFactory"/&gt;
 *
 *     <bean id="myRabbitListenerEndpointRegistry"
 *           class="org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistry">
 *           // registry configuration
 *     </bean>
 *
 *     <bean id="myRabbitHandlerMethodFactory"
 *           class="org.springframework.amqp.rabbit.config.DefaultRabbitHandlerMethodFactory">
 *         <property name="validator" ref="myValidator"/>
 *     </bean>
 *
 *     <bean id="myService" class="com.acme.foo.MyService"/>
 * </beans>
 * }</pre>
 *
 * Implementing {@code RabbitListenerConfigurer} also allows for fine-grained
 * control over endpoints registration via the {@code RabbitListenerEndpointRegistrar}.
 * For example, the following configures an extra endpoint:
 *
 * <pre class="code">
 * &#064;Configuration
 * &#064;EnableRabbit
 * public class AppConfig implements RabbitListenerConfigurer {
 *     &#064;Override
 *     public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
 *         SimpleRabbitListenerEndpoint myEndpoint = new SimpleRabbitListenerEndpoint();
 *         // ... configure the endpoint
 *         registrar.registerEndpoint(endpoint, anotherRabbitListenerContainerFactory());
 *     }
 *
 *     &#064;Bean
 *     public MyService myService() {
 *         return new MyService();
 *     }
 *
 *     &#064;Bean
 *     public RabbitListenerContainerFactory<?> anotherRabbitListenerContainerFactory() {
 *         // ...
 *     }
 *
 *     // Rabbit infrastructure setup
 * }</pre>
 *
 * Note that all beans implementing {@code RabbitListenerConfigurer} will be detected and
 * invoked in a similar fashion. The example above can be translated in a regular bean
 * definition registered in the context in case you use the XML configuration.
 *
 * @author Stephane Nicoll
 * @since 2.0
 * @see RabbitListener
 * @see RabbitListenerAnnotationBeanPostProcessor
 * @see org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistrar
 * @see org.springframework.amqp.rabbit.config.RabbitListenerEndpointRegistry
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(RabbitBootstrapConfiguration.class)
public @interface EnableRabbit {
}
