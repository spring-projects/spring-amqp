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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.Valid;

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.connection.SimplePropertyValueConnectionNameStrategy;
import org.springframework.amqp.rabbit.core.NeedsManagementTests;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunningSupport;
import org.springframework.amqp.rabbit.junit.LogLevels;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.Jackson2XmlMessageConverter;
import org.springframework.amqp.support.converter.RemoteInvocationAwareMessageConverterAdapter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.MethodParameter;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.web.JsonPath;
import org.springframework.lang.NonNull;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ErrorHandler;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;

import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 *
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 * @author Mohammad Hewedy
 *
 * @since 1.4
 */
@SpringJUnitConfig(EnableRabbitIntegrationTests.EnableRabbitConfig.class)
@DirtiesContext
@TestPropertySource(properties = "spring.application.name=testConnectionName")
@RabbitAvailable(queues = { "test.manual.container", "test.no.listener.yet",
		"test.simple", "test.header", "test.message", "test.reply", "test.sendTo", "test.sendTo.reply",
		"test.sendTo.spel", "test.sendTo.reply.spel", "test.sendTo.runtimespel", "test.sendTo.reply.runtimespel",
		"test.sendTo.runtimespelsource", "test.sendTo.runtimespelsource.reply",
		"test.intercepted", "test.intercepted.withReply",
		"test.invalidPojo", "differentTypes", "differentTypes2", "differentTypes3",
		"test.inheritance", "test.inheritance.class",
		"test.comma.1", "test.comma.2", "test.comma.3", "test.comma.4", "test,with,commas",
		"test.converted", "test.converted.list", "test.converted.array", "test.converted.args1",
		"test.converted.args2", "test.converted.message", "test.notconverted.message",
		"test.notconverted.channel", "test.notconverted.messagechannel", "test.notconverted.messagingmessage",
		"test.converted.foomessage", "test.notconverted.messagingmessagenotgeneric", "test.simple.direct",
		"test.simple.direct2", "test.generic.list", "test.generic.map",
		"amqp656dlq", "test.simple.declare", "test.return.exceptions", "test.pojo.errors", "test.pojo.errors2",
		"test.messaging.message", "test.amqp.message", "test.bytes.to.string", "test.projection",
		"test.custom.argument", "test.arg.validation",
		"manual.acks.1", "manual.acks.2", "erit.batch.1", "erit.batch.2", "erit.batch.3", "erit.mp.arg" },
		purgeAfterEach = false)
public class EnableRabbitIntegrationTests extends NeedsManagementTests {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private RabbitTemplate jsonRabbitTemplate;

	@Autowired
	private RabbitAdmin rabbitAdmin;

	@Autowired
	private CountDownLatch errorHandlerLatch1;

	@Autowired
	private CountDownLatch errorHandlerLatch2;

	@Autowired
	private AtomicReference<Throwable> errorHandlerError;

	@Autowired
	private String tagPrefix;

	@Autowired
	private ApplicationContext context;

	@Autowired
	private TxService txService;

	@Autowired
	private TxClassLevel txClassLevel;

	@Autowired
	private MyService service;

	@Autowired
	private ListenerInterceptor interceptor;

	@Autowired
	private RabbitListenerEndpointRegistry registry;

	@Autowired
	private MetaListener metaListener;

	@Autowired
	private CachingConnectionFactory connectionFactory;

	@Autowired
	private MyService myService;

	@Autowired
	private MeterRegistry meterRegistry;

	@Autowired
	private MultiListenerBean multi;

	@Autowired
	private MultiListenerValidatedJsonBean multiValidated;

	@BeforeAll
	public static void setUp() {
		System.setProperty(RabbitListenerAnnotationBeanPostProcessor.RABBIT_EMPTY_STRING_ARGUMENTS_PROPERTY,
				"test-empty");
		RabbitAvailableCondition.getBrokerRunning().removeExchanges("auto.exch.tx",
				"auto.exch",
				"auto.exch.fanout",
				"auto.exch",
				"auto.exch",
				"auto.start",
				"auto.headers",
				"auto.headers",
				"auto.internal",
				"multi.exch",
				"multi.json.exch",
				"multi.exch.tx",
				"test.metaFanout");
	}

	@AfterAll
	public static void tearDown() {
		System.getProperties().remove(RabbitListenerAnnotationBeanPostProcessor.RABBIT_EMPTY_STRING_ARGUMENTS_PROPERTY);
		RabbitAvailableCondition.getBrokerRunning().removeTestQueues("sendTo.replies", "sendTo.replies.spel");
	}

	@Test
	public void autoDeclare() {
		assertThat(rabbitTemplate.convertSendAndReceive("auto.exch", "auto.rk", "foo")).isEqualTo("FOOthreadNamer-1");
		assertThat(this.myService.channelBoundOk).isTrue();
	}

	@Test
	public void autoSimpleDeclare() {
		assertThat(rabbitTemplate.convertSendAndReceive("test.simple.declare", "foo")).isEqualTo("FOOexec1-1");
	}

	@Test
	public void autoSimpleDeclareAnonymousQueue() {
		final SimpleMessageListenerContainer container = (SimpleMessageListenerContainer) registry
				.getListenerContainer("anonymousQueue575");
		assertThat(container.getQueueNames()).hasSize(1);
		assertThat(rabbitTemplate.convertSendAndReceive(container.getQueueNames()[0], "foo")).isEqualTo("viaAnonymous:foo");
		Object messageListener = container.getMessageListener();
		assertThat(TestUtils.getPropertyValue(messageListener, "retryTemplate")).isNotNull();
		assertThat(TestUtils.getPropertyValue(messageListener, "recoveryCallback")).isNotNull();
	}

	@Test
	public void tx() {
		assertThat(AopUtils.isJdkDynamicProxy(this.txService)).isTrue();
		Baz baz = new Baz();
		baz.field = "baz";
		assertThat(rabbitTemplate.convertSendAndReceive("auto.exch.tx", "auto.rk.tx", baz)).isEqualTo("BAZ: baz: auto.rk.tx");
	}

	@Test
	public void autoDeclareFanout() {
		assertThat(rabbitTemplate.convertSendAndReceive("auto.exch.fanout", "", "foo")).isEqualTo("FOOFOO");
	}

	@Test
	public void autoDeclareAnon() {
		assertThat(rabbitTemplate.convertSendAndReceive("auto.exch", "auto.anon.rk", "foo")).isEqualTo("FOO");
	}

	@Test
	public void autoStart() {
		MessageListenerContainer listenerContainer = this.registry.getListenerContainer("notStarted");
		assertThat(listenerContainer).isNotNull();
		assertThat(listenerContainer.isRunning()).isFalse();
		this.registry.start();
		assertThat(listenerContainer.isRunning()).isTrue();
		listenerContainer.stop();
	}

	@Test
	public void autoDeclareAnonWitAtts() {
		String received = (String) rabbitTemplate.convertSendAndReceive("auto.exch", "auto.anon.atts.rk", "foo");
		assertThat(received).startsWith("foo:");
		org.springframework.amqp.core.Queue anonQueueWithAttributes
				= new org.springframework.amqp.core.Queue(received.substring(4), true, true, true);
		this.rabbitAdmin.declareQueue(anonQueueWithAttributes); // will fail if atts not correctly set
	}

	@SuppressWarnings("unchecked")
	@Test
	public void simpleEndpoint() {
		assertThat(rabbitTemplate.convertSendAndReceive("test.simple", "foo")).isEqualTo("FOO");
		assertThat(this.context.getBean("testGroup", List.class)).hasSize(2);
	}

	@Test
	public void simpleDirectEndpoint() {
		MessageListenerContainer container = this.registry.getListenerContainer("direct");
		assertThat(container.isRunning()).isFalse();
		container.start();
		String reply = (String) rabbitTemplate.convertSendAndReceive("test.simple.direct", "foo");
		assertThat(reply).startsWith("FOOfoo");
		assertThat(reply).contains("rabbitClientThread-"); // container runs on client thread
		assertThat(TestUtils.getPropertyValue(container, "consumersPerQueue")).isEqualTo(2);
	}

	@Test
	@LogLevels(classes = { DirectMessageListenerContainer.class, RabbitTemplate.class })
	public void simpleDirectEndpointWithConcurrency() {
		String reply = (String) rabbitTemplate.convertSendAndReceive("test.simple.direct2", "foo");
		assertThat(reply).startsWith("FOOfoo");
		assertThat(reply).contains("rabbitClientThread-"); // container runs on client thread
		assertThat(TestUtils.getPropertyValue(this.registry.getListenerContainer("directWithConcurrency"),
				"consumersPerQueue")).isEqualTo(3);
	}

	@Test
	public void simpleInheritanceMethod() {
		assertThat(rabbitTemplate.convertSendAndReceive("test.inheritance", "foo")).isEqualTo("FOO");
	}

	@Test
	public void simpleInheritanceClass() {
		assertThat(rabbitTemplate.convertSendAndReceive("test.inheritance.class", "foo")).isEqualTo("FOOBAR");
	}

	@Test
	public void commas() {
		assertThat(rabbitTemplate.convertSendAndReceive("test,with,commas", "foo")).isEqualTo("FOOfoo");
		List<?> commaContainers = this.context.getBean("commas", List.class);
		assertThat(commaContainers).hasSize(1);
		SimpleMessageListenerContainer container = (SimpleMessageListenerContainer) commaContainers.get(0);
		List<String> queueNames = Arrays.asList(container.getQueueNames());
		assertThat(queueNames).containsExactly("test.comma.1", "test.comma.2", "test,with,commas", "test.comma.3", "test.comma.4");
	}

	@Test
	public void multiListener() {
		Foo foo = new Foo();
		foo.field = "foo";
		assertThat(rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", foo))
				.isEqualTo("FOO: foo handled by default handler");
		Bar bar = new Bar();
		bar.field = "bar";
		rabbitTemplate.convertAndSend("multi.exch", "multi.rk", bar);
		rabbitTemplate.setReceiveTimeout(10000);
		assertThat(this.rabbitTemplate.receiveAndConvert("sendTo.replies")).isEqualTo("BAR: bar");
		bar.field = "crash";
		rabbitTemplate.convertAndSend("multi.exch", "multi.rk", bar);
		assertThat(this.rabbitTemplate.receiveAndConvert("sendTo.replies"))
				.isEqualTo("CRASHCRASH Test reply from error handler");
		verify(this.multi, times(2)).bar(any());
		bar.field = "bar";
		Baz baz = new Baz();
		baz.field = "baz";
		assertThat(rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", baz)).isEqualTo("BAZ: baz");
		Qux qux = new Qux();
		qux.field = "qux";
		List<String> beanMethodHeaders = new ArrayList<>();
		MessagePostProcessor mpp = msg -> {
			beanMethodHeaders.add(msg.getMessageProperties().getHeader("bean"));
			beanMethodHeaders.add(msg.getMessageProperties().getHeader("method"));
			return msg;
		};
		this.rabbitTemplate.setAfterReceivePostProcessors(mpp);
		assertThat(rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", qux)).isEqualTo("QUX: qux: multi.rk");
		assertThat(beanMethodHeaders).hasSize(2);
		assertThat(beanMethodHeaders.get(0)).contains("$MultiListenerBean");
		assertThat(beanMethodHeaders.get(1)).isEqualTo("qux");
		this.rabbitTemplate.removeAfterReceivePostProcessor(mpp);
		assertThat(rabbitTemplate.convertSendAndReceive("multi.exch.tx", "multi.rk.tx", bar)).isEqualTo("BAR: barbar");
		assertThat(rabbitTemplate.convertSendAndReceive("multi.exch.tx", "multi.rk.tx", baz))
				.isEqualTo("BAZ: bazbaz: multi.rk.tx");
		assertThat(this.multi.bean).isInstanceOf(MultiListenerBean.class);
		assertThat(this.multi.method).isNotNull();
		assertThat(this.multi.method.getName()).isEqualTo("baz");
		assertThat(AopUtils.isJdkDynamicProxy(this.txClassLevel)).isTrue();
	}

	@Test
	public void multiListenerJson() {
		Bar bar = new Bar();
		bar.field = "bar";
		String exchange = "multi.json.exch";
		String routingKey = "multi.json.rk";
		assertThat(this.jsonRabbitTemplate.convertSendAndReceive(exchange, routingKey, bar)).isEqualTo("BAR: barMultiListenerJsonBean");
		Baz baz = new Baz();
		baz.field = "baz";
		assertThat(this.jsonRabbitTemplate.convertSendAndReceive(exchange, routingKey, baz)).isEqualTo("BAZ: baz");
		Qux qux = new Qux();
		qux.field = "qux";
		assertThat(this.jsonRabbitTemplate.convertSendAndReceive(exchange, routingKey, qux)).isEqualTo("QUX: qux: multi.json.rk");

		// SpEL replyTo
		this.jsonRabbitTemplate.convertAndSend(exchange, routingKey, bar);
		this.jsonRabbitTemplate.setReceiveTimeout(10000);
		assertThat(this.jsonRabbitTemplate.receiveAndConvert("sendTo.replies.spel")).isEqualTo("BAR: barMultiListenerJsonBean");
		MessageListenerContainer container = this.registry.getListenerContainer("multi");
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers")).isEqualTo(1);
		assertThat(TestUtils.getPropertyValue(container, "messageListener.errorHandler")).isNotNull();
		assertThat(TestUtils.getPropertyValue(container, "messageListener.returnExceptions", Boolean.class)).isTrue();
	}

	@Test
	public void endpointWithHeader() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("prefix", "prefix-");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.header", request);
		assertThat(MessageTestUtils.extractText(reply)).isEqualTo("prefix-FOO");
		assertThat(reply.getMessageProperties().getHeaders().get("replyMPPApplied")).isEqualTo(Boolean.TRUE);
		assertThat((String) reply.getMessageProperties().getHeader("bean"))
				.isEqualTo("MyService");
		assertThat((String) reply.getMessageProperties().getHeader("method"))
				.isEqualTo("capitalizeWithHeader");
		assertThat((String) reply.getMessageProperties().getHeader("prefix")).isEqualTo("prefix-");
	}

	@Test
	public void endpointWithMessage() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("prefix", "prefix-");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.message", request);
		assertThat(MessageTestUtils.extractText(reply)).isEqualTo("prefix-FOO");
	}

	@Test
	public void endpointWithComplexReply() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("foo", "fooValue");
		Message request = MessageTestUtils.createTextMessage("content", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.reply", request);
		assertThat(MessageTestUtils.extractText(reply)).as("Wrong reply").isEqualTo("content");
		assertThat(reply.getMessageProperties().getHeaders().get("foo")).as("Wrong foo header").isEqualTo("fooValue");
		assertThat((String) reply.getMessageProperties().getHeaders().get("bar")).startsWith(tagPrefix);
	}

	@Test
	@DirtiesContext
	public void simpleEndpointWithSendTo() {
		rabbitTemplate.convertAndSend("test.sendTo", "bar");
		rabbitTemplate.setReceiveTimeout(10000);
		Object result = rabbitTemplate.receiveAndConvert("test.sendTo.reply");
		assertThat(result).isNotNull();
		assertThat(result).isEqualTo("BAR");
	}

	@Test
	@DirtiesContext
	public void simpleEndpointWithSendToSpel() {
		rabbitTemplate.convertAndSend("test.sendTo.spel", "bar");
		rabbitTemplate.setReceiveTimeout(10000);
		Object result = rabbitTemplate.receiveAndConvert("test.sendTo.reply.spel");
		assertThat(result).isNotNull();
		assertThat(result).isEqualTo("BARbar");
	}

	@Test
	public void simpleEndpointWithSendToSpelRuntime() {
		rabbitTemplate.convertAndSend("test.sendTo.runtimespel", "spel");
		rabbitTemplate.setReceiveTimeout(10000);
		Object result = rabbitTemplate.receiveAndConvert("test.sendTo.reply.runtimespel");
		assertThat(result).isNotNull();
		assertThat(result).isEqualTo("runtimespel");
	}

	@Test
	public void simpleEndpointWithSendToSpelRuntimeMessagingMessage() {
		rabbitTemplate.convertAndSend("test.sendTo.runtimespelsource", "spel");
		rabbitTemplate.setReceiveTimeout(10000);
		Object result = rabbitTemplate.receiveAndConvert("test.sendTo.runtimespelsource.reply");
		assertThat(result).isNotNull();
		assertThat(result).isEqualTo("sourceEval");
	}

	@Test
	public void testInvalidPojoConversion() throws InterruptedException {
		this.rabbitTemplate.convertAndSend("test.invalidPojo", "bar");

		assertThat(this.errorHandlerLatch1.await(10, TimeUnit.SECONDS)).isTrue();
		Throwable throwable = this.errorHandlerError.get();
		assertThat(throwable).isNotNull();
		assertThat(throwable).isInstanceOf(AmqpRejectAndDontRequeueException.class);
		assertThat(throwable.getCause()).isInstanceOf(ListenerExecutionFailedException.class);
		assertThat(throwable.getCause().getCause()).isInstanceOf(org.springframework.messaging.converter.MessageConversionException.class);
		assertThat(throwable.getCause().getCause().getMessage()).contains("Failed to convert message payload 'bar' to 'java.util.Date'");
	}

	@Test
	public void testRabbitListenerValidation() throws InterruptedException {
		ValidatedClass validatedObject = new ValidatedClass();
		validatedObject.setBar(5);
		this.jsonRabbitTemplate.convertAndSend("test.arg.validation", validatedObject);
		assertThat(this.service.validationLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.service.validatedObject.validated).isTrue();
		assertThat(this.service.validatedObject.valCount).isEqualTo(1);

	}

	@Test
	public void testRabbitHandlerValidationFailed() throws InterruptedException {
		ValidatedClass validatedObject = new ValidatedClass();
		validatedObject.setBar(42);
		this.jsonRabbitTemplate.convertAndSend("multi.json.exch", "multi.json.valid.rk", validatedObject);
		assertThat(this.errorHandlerLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		Throwable throwable = this.errorHandlerError.get();
		assertThat(throwable).isNotNull();
		assertThat(throwable).isInstanceOf(AmqpRejectAndDontRequeueException.class);
		assertThat(throwable.getCause()).isInstanceOf(ListenerExecutionFailedException.class);
		assertThat(throwable.getCause().getCause()).isInstanceOf(MethodArgumentNotValidException.class);
	}

	@Test
	public void testRabbitHandlerNoDefaultValidationCount() throws InterruptedException {
		ValidatedClass validatedObject = new ValidatedClass();
		validatedObject.setBar(8);
		this.jsonRabbitTemplate.convertAndSend("multi.json.exch", "multi.json.valid.rk", validatedObject);
		assertThat(this.multiValidated.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiValidated.validatedObject.validated).isTrue();
		assertThat(this.multiValidated.validatedObject.valCount).isEqualTo(1);
	}

	@Test
	public void testDifferentTypes() throws InterruptedException {
		Foo1 foo = new Foo1();
		foo.setBar("bar");
		this.service.foos.clear();
		this.jsonRabbitTemplate.convertAndSend("differentTypes", foo);
		assertThat(this.service.dtLatch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.service.foos.get(0)).isInstanceOf(Foo2.class);
		assertThat(((Foo2) this.service.foos.get(0)).getBar()).isEqualTo("bar");
		assertThat(TestUtils.getPropertyValue(this.registry.getListenerContainer("different"), "concurrentConsumers")).isEqualTo(2);
	}

	@Test
	public void testDifferentTypesWithConcurrency() throws InterruptedException {
		Foo1 foo = new Foo1();
		foo.setBar("bar");
		this.service.foos.clear();
		this.jsonRabbitTemplate.convertAndSend("differentTypes2", foo);
		assertThat(this.service.dtLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.service.foos.get(0)).isInstanceOf(Foo2.class);
		assertThat(((Foo2) this.service.foos.get(0)).getBar()).isEqualTo("bar");
		MessageListenerContainer container = this.registry.getListenerContainer("differentWithConcurrency");
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers")).isEqualTo(3);
		assertThat(TestUtils.getPropertyValue(container, "maxConcurrentConsumers")).isNull();
	}

	@Test
	public void testDifferentTypesWithVariableConcurrency() throws InterruptedException {
		Foo1 foo = new Foo1();
		foo.setBar("bar");
		this.service.foos.clear();
		this.jsonRabbitTemplate.convertAndSend("differentTypes3", foo);
		assertThat(this.service.dtLatch3.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.service.foos.get(0)).isInstanceOf(Foo2.class);
		assertThat(((Foo2) this.service.foos.get(0)).getBar()).isEqualTo("bar");
		MessageListenerContainer container = this.registry.getListenerContainer("differentWithVariableConcurrency");
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers")).isEqualTo(3);
		assertThat(TestUtils.getPropertyValue(container, "maxConcurrentConsumers")).isEqualTo(4);
	}

	@Test
	public void testInterceptor() throws InterruptedException {
		this.rabbitTemplate.convertAndSend("test.intercepted", "intercept this");
		assertThat(this.interceptor.oneWayLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.rabbitTemplate.convertSendAndReceive("test.intercepted.withReply", "intercept this")).isEqualTo("INTERCEPT THIS");
		assertThat(this.interceptor.twoWayLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testConverted() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(
				EnableRabbitConfigWithCustomConversion.class);
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		Foo1 foo1 = new Foo1();
		foo1.setBar("bar");
		Jackson2JsonMessageConverter converter = ctx.getBean(Jackson2JsonMessageConverter.class);
		converter.setTypePrecedence(TypePrecedence.TYPE_ID);
		Object returned = template.convertSendAndReceive("test.converted", foo1);
		assertThat(returned).isInstanceOf(Foo2.class);
		assertThat(((Foo2) returned).getBar()).isEqualTo("bar");
		assertThat(TestUtils.getPropertyValue(ctx.getBean("foo1To2Converter"), "converted", Boolean.class)).isTrue();
		converter.setTypePrecedence(TypePrecedence.INFERRED);

		// No type info in message
		template.setMessageConverter(new SimpleMessageConverter());
		@SuppressWarnings("resource")
		MessagePostProcessor messagePostProcessor = message -> {
			message.getMessageProperties().setContentType("application/json");
			message.getMessageProperties().setUserId("guest");
			message.getMessageProperties().setHeader("stringHeader", "string");
			message.getMessageProperties().setHeader("intHeader", 42);
			return message;
		};
		returned = template.convertSendAndReceive("", "test.converted", "{ \"bar\" : \"baz\" }", messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("{\"bar\":\"baz\"}");

		returned = template.convertSendAndReceive("", "test.converted.list", "[ { \"bar\" : \"baz\" } ]",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("{\"bar\":\"BAZZZZ\"}");

		returned = template.convertSendAndReceive("", "test.converted.array", "[ { \"bar\" : \"baz\" } ]",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("{\"bar\":\"BAZZxx\"}");

		returned = template.convertSendAndReceive("", "test.converted.args1", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"bar=baztest.converted.args1\"");

		returned = template.convertSendAndReceive("", "test.converted.args2", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"bar=baztest.converted.args2\"");

		List<String> beanMethodHeaders = new ArrayList<>();
		MessagePostProcessor mpp = msg -> {
			beanMethodHeaders.add(msg.getMessageProperties().getHeader("bean"));
			beanMethodHeaders.add(msg.getMessageProperties().getHeader("method"));
			return msg;
		};
		template.setAfterReceivePostProcessors(mpp);
		returned = template.convertSendAndReceive("", "test.converted.message", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"bar=bazfoo2MessageFoo2Service\"");
		assertThat(beanMethodHeaders).hasSize(2);
		assertThat(beanMethodHeaders.get(0)).isEqualTo("Foo2Service");
		assertThat(beanMethodHeaders.get(1)).isEqualTo("foo2Message");
		template.removeAfterReceivePostProcessor(mpp);
		Foo2Service foo2Service = ctx.getBean(Foo2Service.class);
		assertThat(foo2Service.bean).isInstanceOf(Foo2Service.class);
		assertThat(foo2Service.method).isNotNull();
		assertThat(foo2Service.method.getName()).isEqualTo("foo2Message");

		returned = template.convertSendAndReceive("", "test.notconverted.message", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"fooMessage\"");
		assertThat(foo2Service.stringHeader).isEqualTo("string");
		assertThat(foo2Service.intHeader).isEqualTo(42);

		returned = template.convertSendAndReceive("", "test.notconverted.channel", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"barAndChannel\"");

		returned = template.convertSendAndReceive("", "test.notconverted.messagechannel", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"bar=bazMessageAndChannel\"");

		returned = template.convertSendAndReceive("", "test.notconverted.messagingmessage", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"GenericMessageLinkedHashMap\"");

		returned = template.convertSendAndReceive("", "test.converted.foomessage", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"GenericMessageFoo2guest\"");

		returned = template.convertSendAndReceive("", "test.notconverted.messagingmessagenotgeneric",
				"{ \"bar\" : \"baz\" }", messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"GenericMessageLinkedHashMap\"");

		returned = template.convertSendAndReceive("", "test.projection",
				"{ \"username\" : \"SomeUsername\", \"user\" : { \"name\" : \"SomeName\"}}", messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("\"SomeUsernameSomeName\"");

		Jackson2JsonMessageConverter jsonConverter = ctx.getBean(Jackson2JsonMessageConverter.class);

		DefaultJackson2JavaTypeMapper mapper = TestUtils.getPropertyValue(jsonConverter, "javaTypeMapper",
				DefaultJackson2JavaTypeMapper.class);
		Mockito.verify(mapper).setBeanClassLoader(ctx.getClassLoader());

		ctx.close();
	}

	@Test
	public void testXmlConverted() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(
				EnableRabbitConfigWithCustomXmlConversion.class);
		RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
		Foo1 foo1 = new Foo1();
		foo1.setBar("bar");
		Jackson2XmlMessageConverter converter = ctx.getBean(Jackson2XmlMessageConverter.class);
		converter.setTypePrecedence(TypePrecedence.TYPE_ID);
		Object returned = template.convertSendAndReceive("test.converted", foo1);
		assertThat(returned).isInstanceOf(Foo2.class);
		assertThat(((Foo2) returned).getBar()).isEqualTo("bar");
		assertThat(TestUtils.getPropertyValue(ctx.getBean("foo1To2Converter"), "converted", Boolean.class)).isTrue();
		converter.setTypePrecedence(TypePrecedence.INFERRED);

		// No type info in message
		template.setMessageConverter(new SimpleMessageConverter());
		@SuppressWarnings("resource")
		MessagePostProcessor messagePostProcessor = message -> {
			message.getMessageProperties().setContentType("application/xml");
			message.getMessageProperties().setUserId("guest");
			return message;
		};
		returned = template.convertSendAndReceive("", "test.converted", "<Foo2><bar>baz</bar></Foo2>", messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<Foo2><bar>baz</bar></Foo2>");

		returned = template.convertSendAndReceive("", "test.converted.list", "<Foo2><list><bar>baz</bar></list></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<Foo2><bar>BAZZZZ</bar></Foo2>");

		returned = template.convertSendAndReceive("", "test.converted.array", "<Foo2><list><bar>baz</bar></list></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<Foo2><bar>BAZZxx</bar></Foo2>");

		returned = template.convertSendAndReceive("", "test.converted.args1", "<Foo2><bar>baz</bar></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>bar=baztest.converted.args1</String>");

		returned = template.convertSendAndReceive("", "test.converted.args2", "<Foo2><bar>baz</bar></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>bar=baztest.converted.args2</String>");

		returned = template.convertSendAndReceive("", "test.converted.message", "<Foo2><bar>baz</bar></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>bar=bazfoo2MessageFoo2Service</String>");

		returned = template.convertSendAndReceive("", "test.notconverted.message", "<Foo2><bar>baz</bar></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>fooMessage</String>");

		returned = template.convertSendAndReceive("", "test.notconverted.channel", "<Foo2><bar>baz</bar></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>barAndChannel</String>");

		returned = template.convertSendAndReceive("", "test.notconverted.messagechannel", "<Foo2><bar>baz</bar></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>bar=bazMessageAndChannel</String>");

		returned = template.convertSendAndReceive("", "test.notconverted.messagingmessage", "<Foo2><bar>baz</bar></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>GenericMessageLinkedHashMap</String>");

		returned = template.convertSendAndReceive("", "test.converted.foomessage", "<Foo2><bar>baz</bar></Foo2>",
				messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>GenericMessageFoo2guest</String>");

		returned = template.convertSendAndReceive("", "test.notconverted.messagingmessagenotgeneric",
				"<Foo2><bar>baz</bar></Foo2>", messagePostProcessor);
		assertThat(returned).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) returned)).isEqualTo("<String>GenericMessageLinkedHashMap</String>");

		Jackson2XmlMessageConverter xmlConverter = ctx.getBean(Jackson2XmlMessageConverter.class);

		DefaultJackson2JavaTypeMapper mapper = TestUtils.getPropertyValue(xmlConverter, "javaTypeMapper",
				DefaultJackson2JavaTypeMapper.class);
		Mockito.verify(mapper).setBeanClassLoader(ctx.getClassLoader());

		ctx.close();
	}

	@Test
	public void testMeta() throws Exception {
		this.rabbitTemplate.convertAndSend("test.metaFanout", "", "foo");
		assertThat(this.metaListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testHeadersExchange() throws Exception {
		assertThat(rabbitTemplate.convertSendAndReceive("auto.headers", "", "foo",
				message -> {
					message.getMessageProperties().getHeaders().put("foo", "bar");
					message.getMessageProperties().getHeaders().put("baz", "qux");
					return message;
				})).isEqualTo("FOO");
		assertThat(rabbitTemplate.convertSendAndReceive("auto.headers", "", "bar",
				message -> {
					message.getMessageProperties().getHeaders().put("baz", "fiz");
					return message;
				})).isEqualTo("BAR");
	}

	@Test
	public void deadLetterOnDefaultExchange() {
		this.rabbitTemplate.convertAndSend("amqp656", "foo");
		assertThat(this.rabbitTemplate.receiveAndConvert("amqp656dlq", 10000)).isEqualTo("foo");
		try {
			Map<String, Object> amqp656 = await().until(() -> queueInfo("amqp656"), q -> q != null);
			if (amqp656 != null) {
				assertThat(arguments(amqp656).get("test-empty")).isEqualTo("");
				assertThat(arguments(amqp656).get("test-null")).isEqualTo("undefined");
			}
		}
		catch (Exception e) {
			// empty
		}
	}

	@Test
	@DirtiesContext
	public void returnExceptionWithRethrowAdapter() {
		this.rabbitTemplate.setMessageConverter(new RemoteInvocationAwareMessageConverterAdapter());
		try {
			this.rabbitTemplate.convertSendAndReceive("test.return.exceptions", "foo");
			fail("ExpectedException");
		}
		catch (Exception e) {
			assertThat(e.getCause().getMessage()).isEqualTo("return this");
		}
	}

	@Test
	public void listenerErrorHandler() {
		assertThat(this.rabbitTemplate.convertSendAndReceive("test.pojo.errors", "foo")).isEqualTo("BAR");
	}

	@Test
	@DirtiesContext
	public void listenerErrorHandlerException() {
		this.rabbitTemplate.setMessageConverter(new RemoteInvocationAwareMessageConverterAdapter());
		try {
			this.rabbitTemplate.convertSendAndReceive("test.pojo.errors2", "foo");
			fail("ExpectedException");
		}
		catch (Exception e) {
			assertThat(e.getCause().getMessage()).isEqualTo("from error handler");
			assertThat(e.getCause().getCause().getMessage()).isEqualTo("return this");
			EnableRabbitConfig config = this.context.getBean(EnableRabbitConfig.class);
			assertThat(config.errorHandlerChannel).isNotNull();
		}
	}

	@Test
	public void testPrototypeCache() {
		RabbitListenerAnnotationBeanPostProcessor bpp =
				this.context.getBean(RabbitListenerAnnotationBeanPostProcessor.class);
		@SuppressWarnings("unchecked")
		Map<Class<?>, ?> typeCache = TestUtils.getPropertyValue(bpp, "typeCache", Map.class);
		assertThat(typeCache.containsKey(Foo1.class)).isFalse();
		this.context.getBean("foo1Prototype");
		assertThat(typeCache.containsKey(Foo1.class)).isTrue();
		Object value = typeCache.get(Foo1.class);
		this.context.getBean("foo1Prototype");
		assertThat(typeCache.containsKey(Foo1.class)).isTrue();
		assertThat(typeCache.get(Foo1.class)).isSameAs(value);
	}

	@Test
	public void testGenericReturnTypes() {
		Object returned = this.jsonRabbitTemplate.convertSendAndReceive("", "test.generic.list", new JsonObject("baz"));
		assertThat(returned).isInstanceOf(List.class);
		assertThat(((List<?>) returned).get(0)).isInstanceOf(JsonObject.class);

		returned = this.jsonRabbitTemplate.convertSendAndReceive("", "test.generic.map", new JsonObject("baz"));
		assertThat(returned).isInstanceOf(Map.class);
		assertThat(((Map<?, ?>) returned).get("key")).isInstanceOf(JsonObject.class);
	}

	@Test
	public void testManualContainer() throws Exception {
		this.rabbitTemplate.convertAndSend("test.manual.container", "foo");
		EnableRabbitConfig config = this.context.getBean(EnableRabbitConfig.class);
		assertThat(config.manualContainerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(new String(config.message.getBody())).isEqualTo("foo");
	}

	@Test
	public void testNoListenerYet() throws Exception {
		this.rabbitTemplate.convertAndSend("test.no.listener.yet", "bar");
		EnableRabbitConfig config = this.context.getBean(EnableRabbitConfig.class);
		assertThat(config.noListenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(new String(config.message.getBody())).isEqualTo("bar");
	}

	@Test
	public void connectionName() {
		Connection conn = this.connectionFactory.createConnection();
		conn.close();
		assertThat(conn.getDelegate().getClientProvidedName()).isEqualTo("testConnectionName");
	}

	@Test
	public void messagingMessageReturned() throws InterruptedException {
		Message message = org.springframework.amqp.core.MessageBuilder.withBody("\"messaging\"".getBytes())
			.andProperties(MessagePropertiesBuilder.newInstance().setContentType("application/json").build()).build();
		message = this.rabbitTemplate.sendAndReceive("test.messaging.message", message);
		assertThat(message).isNotNull();
		assertThat(new String(message.getBody())).isEqualTo("{\"field\":\"MESSAGING\"}");
		assertThat(message.getMessageProperties().getHeaders().get("foo")).isEqualTo("bar");
		Timer timer = await().until(() -> {
			try {
				return this.meterRegistry.get("spring.rabbitmq.listener")
						.tag("listener.id", "list.of.messages")
						.tag("queue", "test.messaging.message")
						.tag("result", "success")
						.tag("exception", "none")
						.tag("extraTag", "foo")
						.timer();
			}
			catch (@SuppressWarnings("unused") Exception e) {
				return null;
			}
		}, tim -> tim != null);
		assertThat(timer.count()).isEqualTo(1L);
	}

	@Test
	public void amqpMessageReturned() {
		Message message = org.springframework.amqp.core.MessageBuilder.withBody("amqp".getBytes())
				.andProperties(MessagePropertiesBuilder.newInstance().setContentType("text/plain").build()).build();
		message = this.rabbitTemplate.sendAndReceive("test.amqp.message", message);
		assertThat(message).isNotNull();
		assertThat(new String(message.getBody())).isEqualTo("AMQP");
		assertThat(message.getMessageProperties().getHeaders().get("foo")).isEqualTo("bar");
	}

	@Test
	public void bytesToString() {
		Message message = new Message("bytes".getBytes(), new MessageProperties());
		message = this.rabbitTemplate.sendAndReceive("test.bytes.to.string", message);
		assertThat(message).isNotNull();
		assertThat(message.getBody()).isEqualTo("BYTES".getBytes());
	}

	@Test
	public void testManualOverride() {
		assertThat(TestUtils.getPropertyValue(this.registry.getListenerContainer("manual.acks.1"), "acknowledgeMode"))
			.isEqualTo(AcknowledgeMode.MANUAL);
		assertThat(TestUtils.getPropertyValue(this.registry.getListenerContainer("manual.acks.2"), "acknowledgeMode"))
			.isEqualTo(AcknowledgeMode.MANUAL);
	}

	@Test
	public void testConsumerBatchEnabled() throws InterruptedException {
		this.rabbitTemplate.convertAndSend("erit.batch.1", "foo");
		this.rabbitTemplate.convertAndSend("erit.batch.1", "bar");
		this.rabbitTemplate.convertAndSend("erit.batch.2", "foo");
		this.rabbitTemplate.convertAndSend("erit.batch.2", "bar");
		this.rabbitTemplate.convertAndSend("erit.batch.3", "foo");
		this.rabbitTemplate.convertAndSend("erit.batch.3", "bar");
		assertThat(this.myService.batch1Latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myService.batch2Latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myService.batch3Latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myService.amqpMessagesReceived).hasSize(2);
		assertThat(this.myService.amqpMessagesReceived.get(0)).isInstanceOf(Message.class);
		assertThat(this.myService.messagingMessagesReceived).hasSize(2);
		assertThat(this.myService.messagingMessagesReceived.get(0))
				.isInstanceOf(org.springframework.messaging.Message.class);
		assertThat(this.myService.batch3Strings).hasSize(2);
		assertThat(this.myService.batch3Strings.get(0)).isInstanceOf(String.class);
	}

	@Test
	public void testCustomMethodArgumentResolverListener() throws InterruptedException {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("customHeader", "fiz");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		this.rabbitTemplate.convertAndSend("test.custom.argument", request);
		assertThat(this.myService.customMethodArgumentResolverLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.myService.customMethodArgument.body).isEqualTo("foo");
		assertThat(this.myService.customMethodArgument.topic).isEqualTo("fiz");

	}

	@Test
	void messagePropertiesParam() {
		assertThat(this.rabbitTemplate.convertSendAndReceive("erit.mp.arg", "foo", msg -> {
			msg.getMessageProperties().setHeader("myProp", "bar");
			return msg;
		})).isEqualTo("foo, myProp=bar");
	}

	@Test
	void listenerWithBrokerNamedQueue() {
		AbstractMessageListenerContainer container =
				(AbstractMessageListenerContainer) this.registry.getListenerContainer("brokerNamed");
		assertThat(container.getQueueNames()[0]).startsWith("amq.gen");
	}

	interface TxService {

		@Transactional
		String baz(@Payload Baz baz, @Header("amqp_receivedRoutingKey") String rk);

	}

	static class TxServiceImpl implements TxService {

		@Override
		@RabbitListener(bindings = @QueueBinding(
				value = @Queue,
				exchange = @Exchange(value = "auto.exch.tx", autoDelete = "true"),
				key = "auto.rk.tx")
		)
		public String baz(Baz baz, String rk) {
			return "BAZ: " + baz.field + ": " + rk;
		}

	}

	public interface MyServiceInterface {

		@RabbitListener(queues = "test.inheritance")
		String testAnnotationInheritance(String foo);

	}

	public static class MyServiceInterfaceImpl implements MyServiceInterface {

		@Override
		public String testAnnotationInheritance(String foo) {
			return foo.toUpperCase();
		}

	}

	@RabbitListener(queues = "test.inheritance.class")
	public interface MyServiceInterface2 {

		@RabbitHandler
		String testAnnotationInheritance(String foo);

	}

	public static class MyServiceInterfaceImpl2 implements MyServiceInterface2 {

		@Override
		public String testAnnotationInheritance(String foo) {
			return foo.toUpperCase() + "BAR";
		}

	}

	public static class MyService {

		final RabbitTemplate txRabbitTemplate;

		final List<Object> foos = new ArrayList<>();

		final CountDownLatch dtLatch1 = new CountDownLatch(1);

		final CountDownLatch dtLatch2 = new CountDownLatch(1);

		final CountDownLatch dtLatch3 = new CountDownLatch(1);

		final CountDownLatch validationLatch = new CountDownLatch(1);

		final CountDownLatch batch1Latch = new CountDownLatch(1);

		final CountDownLatch batch2Latch = new CountDownLatch(1);

		final CountDownLatch batch3Latch = new CountDownLatch(1);

		final CountDownLatch customMethodArgumentResolverLatch = new CountDownLatch(1);

		volatile Boolean channelBoundOk;

		volatile List<Message> amqpMessagesReceived;

		volatile List<org.springframework.messaging.Message<?>> messagingMessagesReceived;

		volatile List<String> batch3Strings;

		volatile CustomMethodArgument customMethodArgument;

		volatile ValidatedClass validatedObject;

		public MyService(RabbitTemplate txRabbitTemplate) {
			this.txRabbitTemplate = txRabbitTemplate;
		}

		@RabbitListener(id = "threadNamer", bindings = @QueueBinding(
				value = @Queue(value = "auto.declare", autoDelete = "true", admins = "rabbitAdmin"),
				exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
				key = "auto.rk"), containerFactory = "txListenerContainerFactory"
		)
		public String handleWithDeclare(String foo, Channel channel) {
			this.channelBoundOk = this.txRabbitTemplate.execute(c -> c.equals(channel));
			return foo.toUpperCase() + Thread.currentThread().getName();
		}

		@RabbitListener(queuesToDeclare = @Queue(name = "${jjjj:test.simple.declare}", durable = "true"),
				executor = "exec1")
		public String handleWithSimpleDeclare(String foo) {
			return foo.toUpperCase() + Thread.currentThread().getName();
		}

		@RabbitListener(queuesToDeclare = @Queue, id = "anonymousQueue575")
		public String handleWithAnonymousQueueToDeclare(String data) {
			return "viaAnonymous:" + data;
		}

		@RabbitListener(bindings = @QueueBinding(
				value = @Queue(value = "auto.declare.fanout", autoDelete = "true"),
				exchange = @Exchange(value = "auto.exch.fanout", autoDelete = "true", type = "fanout"))
		)
		public String handleWithFanout(String foo) {
			return foo.toUpperCase() + foo.toUpperCase();
		}

		@RabbitListener(bindings = {
				@QueueBinding(
						value = @Queue,
						exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
						key = "auto.anon.rk") }
		)
		public String handleWithDeclareAnon(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(bindings = @QueueBinding(
				value = @Queue(autoDelete = "true", exclusive = "true", durable = "true"),
				exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
				key = "auto.anon.atts.rk")
		)
		public String handleWithDeclareAnonQueueWithAtts(String foo, @Header(AmqpHeaders.CONSUMER_QUEUE) String queue) {
			return foo + ":" + queue;
		}

		@RabbitListener(queues = "test.#{'${my.queue.suffix:SIMPLE}'.toLowerCase()}", group = "testGroup")
		public String capitalize(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(id = "direct", queues = "test.simple.direct", autoStartup = "${no.property.here:false}",
				containerFactory = "directListenerContainerFactory")
		public String capitalizeDirect1(String foo) {
			return foo.toUpperCase() + foo + Thread.currentThread().getName();
		}

		@RabbitListener(id = "directWithConcurrency", queues = "test.simple.direct2", concurrency = "${ffffx:3}",
				containerFactory = "directListenerContainerFactory")
		public String capitalizeDirect2(String foo) {
			return foo.toUpperCase() + foo + Thread.currentThread().getName();
		}

		@RabbitListener(queues = { "#{'test.comma.1,test.comma.2'.split(',')}",
				"test,with,commas",
				"#{commaQueues}" },
				group = "commas")
		public String multiQueuesConfig(String foo) {
			return foo.toUpperCase() + foo;
		}

		@RabbitListener(queues = "test.header", group = "testGroup", replyPostProcessor = "#{'echoPrefixHeader'}")
		public String capitalizeWithHeader(@Payload String content, @Header String prefix) {
			return prefix + content.toUpperCase();
		}

		@RabbitListener(queues = "test.message")
		public String capitalizeWithMessage(org.springframework.messaging.Message<String> message) {
			return message.getHeaders().get("prefix") + message.getPayload().toUpperCase();
		}

		@RabbitListener(queues = "test.reply")
		public org.springframework.messaging.Message<?> reply(String payload, @Header String foo,
				@Header(AmqpHeaders.CONSUMER_TAG) String tag) {
			return MessageBuilder.withPayload(payload)
					.setHeader("foo", foo).setHeader("bar", tag).build();
		}

		@RabbitListener(queues = "test.sendTo")
		@SendTo("${foo.bar:test.sendTo.reply}")
		public String capitalizeAndSendTo(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(queues = "test.sendTo.spel")
		@SendTo("#{spelReplyTo}")
		public String capitalizeAndSendToSpel(String foo) {
			return foo.toUpperCase() + foo;
		}

		@RabbitListener(queues = "test.sendTo.runtimespel")
		@SendTo("!{'test.sendTo.reply.' + result}")
		public String capitalizeAndSendToSpelRuntime(String foo) {
			return "runtime" + foo;
		}

		@RabbitListener(queues = "test.sendTo.runtimespelsource")
		@SendTo("!{source.headers['amqp_consumerQueue'] + '.reply'}")
		public String capitalizeAndSendToSpelRuntimeSource(String foo) {
			return "sourceEval";
		}

		@RabbitListener(queues = "test.invalidPojo")
		public void handleIt(Date body) {

		}

		@RabbitListener(id = "different", queues = "differentTypes",
				containerFactory = "jsonListenerContainerFactoryNoClassMapper")
		public void handleDifferent(@Validated Foo2 foo) {
			foos.add(foo);
			dtLatch1.countDown();
		}

		@RabbitListener(id = "differentWithConcurrency", queues = "differentTypes2",
				containerFactory = "jsonListenerContainerFactoryNoClassMapper", concurrency = "#{3}")
		public void handleDifferentWithConcurrency(Foo2 foo, MessageHeaders headers) {
			foos.add(foo);
			dtLatch2.countDown();
		}

		@RabbitListener(id = "differentWithVariableConcurrency", queues = "differentTypes3",
				containerFactory = "jsonListenerContainerFactory", concurrency = "3-4")
		public void handleDifferentWithVariableConcurrency(Foo2 foo) {
			foos.add(foo);
			dtLatch3.countDown();
		}

		@RabbitListener(id = "notStarted", containerFactory = "rabbitAutoStartFalseListenerContainerFactory",
				bindings = @QueueBinding(
						value = @Queue(autoDelete = "true", exclusive = "true", durable = "true"),
						exchange = @Exchange(value = "auto.start", autoDelete = "true", delayed = "${no.prop:false}"),
						key = "auto.start")
		)
		public void handleWithAutoStartFalse(String foo) {
		}

		@RabbitListener(id = "headersId", bindings = {
				@QueueBinding(
						value = @Queue(name = "auto.headers1", autoDelete = "true",
								arguments = @Argument(name = "x-message-ttl", value = "10000",
										type = "java.lang.Integer")),
						exchange = @Exchange(name = "auto.headers", type = ExchangeTypes.HEADERS, autoDelete = "true"),
						arguments = {
								@Argument(name = "x-match", value = "all"),
								@Argument(name = "foo", value = "bar"),
								@Argument(name = "baz")
						}
				),
				@QueueBinding(
						value = @Queue(value = "auto.headers2", autoDelete = "true",
								arguments = @Argument(name = "x-message-ttl", value = "10000",
										type = "#{T(java.lang.Integer)}")),
						exchange = @Exchange(value = "auto.headers", type = ExchangeTypes.HEADERS, autoDelete = "true"),
						arguments = {
								@Argument(name = "x-match", value = "any"),
								@Argument(name = "foo", value = "bax"),
								@Argument(name = "#{'baz'}", value = "#{'fiz'}")
						}
				)
		}
		)
		public String handleWithHeadersExchange(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(bindings = {
				@QueueBinding(
						value = @Queue,
						exchange = @Exchange(value = "auto.internal", autoDelete = "true", internal = "true"),
						key = "auto.internal.rk") }
		)
		public String handleWithInternalExchange(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(bindings = {
				@QueueBinding(
						value = @Queue,
						exchange = @Exchange(value = "auto.internal", autoDelete = "true",
								ignoreDeclarationExceptions = "true"),
						key = "auto.internal.rk") }
		)
		public String handleWithInternalExchangeIgnore(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(id = "defaultDLX",
				bindings = @QueueBinding(
						value = @Queue(value = "amqp656",
								autoDelete = "true",
								arguments = {
										@Argument(name = "x-dead-letter-exchange", value = ""),
										@Argument(name = "x-dead-letter-routing-key", value = "amqp656dlq"),
										@Argument(name = "test-empty", value = ""),
										@Argument(name = "test-null", value = "") }),
						exchange = @Exchange(value = "amq.topic", type = "topic"),
						key = "foo"))
		public String handleWithDeadLetterDefaultExchange(String foo) {
			throw new AmqpRejectAndDontRequeueException("dlq");
		}

		@RabbitListener(queues = "test.return.exceptions", returnExceptions = "${some.prop:true}")
		public String alwaysFails(String data) throws Exception {
			throw new Exception("return this");
		}

		@RabbitListener(queues = "test.pojo.errors", errorHandler = "#{alwaysBARHandler}")
		public String alwaysFailsWithErrorHandler(String data) throws Exception {
			throw new Exception("return this");
		}

		@RabbitListener(queues = "test.pojo.errors2", errorHandler = "throwANewException", returnExceptions = "true")
		public String alwaysFailsWithErrorHandlerThrowAnother(String data) throws Exception {
			throw new Exception("return this");
		}

		@RabbitListener(queues = "test.generic.list", containerFactory = "simpleJsonListenerContainerFactory")
		public List<JsonObject> genericList(JsonObject in) {
			return Collections.singletonList(in);
		}

		@RabbitListener(queues = "test.generic.map", containerFactory = "simpleJsonListenerContainerFactory")
		public Map<String, JsonObject> genericMap(JsonObject in) {
			return Collections.singletonMap("key", in);
		}

		@RabbitListener(id = "list.of.messages",
				queues = "test.messaging.message", containerFactory = "simpleJsonListenerContainerFactory")
		public org.springframework.messaging.Message<Bar> messagingMessage(String in) {
			Bar bar = new Bar();
			bar.field = in.toUpperCase();
			return new GenericMessage<>(bar, Collections.singletonMap("foo", "bar"));
		}

		@RabbitListener(queues = "test.amqp.message")
		public Message amqpMessage(String in) {
			return org.springframework.amqp.core.MessageBuilder.withBody(in.toUpperCase().getBytes())
					.andProperties(MessagePropertiesBuilder.newInstance().setContentType("text/plain")
							.setHeader("foo", "bar")
							.build())
					.build();
		}

		@RabbitListener(queues = "test.bytes.to.string")
		public String bytesToString(String in) {
			return in.toUpperCase();
		}

		@RabbitListener(id = "manual.acks.1", queues = "manual.acks.1", ackMode = "MANUAL")
		public String manual1(String in, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag)
				throws IOException {

			channel.basicAck(tag, false);
			return in.toUpperCase();
		}

		@RabbitListener(id = "manual.acks.2", queues = "manual.acks.2",
				ackMode = "#{T(org.springframework.amqp.core.AcknowledgeMode).MANUAL}")

		public String manual2(String in, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag)
				throws IOException {

			channel.basicAck(tag, false);
			return in.toUpperCase();
		}

		@RabbitListener(queues = "erit.batch.1", containerFactory = "consumerBatchContainerFactory")
		public void consumerBatch1(List<Message> amqpMessages) {
			this.amqpMessagesReceived = amqpMessages;
			this.batch1Latch.countDown();
		}

		@RabbitListener(queues = "erit.batch.2", containerFactory = "consumerBatchContainerFactory")
		public void consumerBatch2(List<org.springframework.messaging.Message<?>> messages) {
			this.messagingMessagesReceived = messages;
			this.batch2Latch.countDown();
		}

		@RabbitListener(queues = "erit.batch.3", containerFactory = "consumerBatchContainerFactory")
		public void consumerBatch3(List<String> strings) {
			this.batch3Strings = strings;
			this.batch3Latch.countDown();
		}

		@RabbitListener(queues = "test.custom.argument")
		public void customMethodArgumentResolverListener(String data, CustomMethodArgument customMethodArgument) {
			this.customMethodArgument = customMethodArgument;
			this.customMethodArgumentResolverLatch.countDown();
		}

		@RabbitListener(queues = "test.arg.validation", containerFactory = "simpleJsonListenerContainerFactory")
		public void handleValidArg(@Valid ValidatedClass validatedObject) {
			this.validatedObject = validatedObject;
			this.validationLatch.countDown();
		}

		@RabbitListener(queues = "erit.mp.arg")
		public String mpArgument(String payload, MessageProperties props) {
			return payload + ", myProp=" + props.getHeader("myProp");
		}

		@RabbitListener(id = "brokerNamed", queues = "#{@brokerNamed}")
		void brokerNamed(String in) {
		}

	}

	public static class JsonObject {

		private String bar;

		public JsonObject() {
		}

		public JsonObject(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "JsonObject [bar=" + this.bar + "]";
		}

	}

	public static class Foo1 {

		private String bar;

		public String getBar() {
			return bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class Foo2 {

		private String bar;

		public String getBar() {
			return bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "bar=" + this.bar;
		}


	}

	public static class CustomMethodArgument {

		final String body;

		final String topic;

		CustomMethodArgument(String body, String topic) {
			this.body = body;
			this.topic = topic;
		}

	}

	public static class ProxiedListener {

		@RabbitListener(queues = "test.intercepted")
		public void listen(String foo) {
		}

		@RabbitListener(queues = "test.intercepted.withReply")
		public String listenAndReply(String foo) {
			return foo.toUpperCase();
		}

	}

	public static class ListenerInterceptor implements MethodInterceptor {

		private final CountDownLatch oneWayLatch = new CountDownLatch(1);

		private final CountDownLatch twoWayLatch = new CountDownLatch(1);

		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			String methodName = invocation.getMethod().getName();
			if (methodName.equals("listen") && invocation.getArguments().length == 1 &&
					invocation.getArguments()[0].equals("intercept this")) {
				this.oneWayLatch.countDown();
				return invocation.proceed();
			}
			else if (methodName.equals("listenAndReply") && invocation.getArguments().length == 1 &&
					invocation.getArguments()[0].equals("intercept this")) {
				Object result = invocation.proceed();
				if (result.equals("INTERCEPT THIS")) {
					this.twoWayLatch.countDown();
				}
				return result;
			}
			return invocation.proceed();
		}

	}

	public static class ProxyListenerBPP implements BeanPostProcessor, BeanFactoryAware, PriorityOrdered {

		private BeanFactory beanFactory;

		@Override
		public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
			this.beanFactory = beanFactory;
		}

		@Override
		public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
			return bean;
		}

		@Override
		public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
			if (bean instanceof ProxiedListener) {
				ProxyFactoryBean pfb = new ProxyFactoryBean();
				pfb.setProxyTargetClass(true); // CGLIB, false for JDK proxy (interface needed)
				pfb.setTarget(bean);
				pfb.addAdvice(this.beanFactory.getBean("wasCalled", Advice.class));
				return pfb.getObject();
			}
			else {
				return bean;
			}
		}

		@Override
		public int getOrder() {
			return Ordered.LOWEST_PRECEDENCE - 1000; // Just before @RabbitListener post processor
		}

	}

	@Configuration
	@EnableRabbit
	@EnableTransactionManagement
	public static class EnableRabbitConfig implements RabbitListenerConfigurer {

		private int increment;

		private Message message;

		private final CountDownLatch manualContainerLatch = new CountDownLatch(1);

		private final CountDownLatch noListenerLatch = new CountDownLatch(1);

		private volatile Channel errorHandlerChannel;

		@Bean
		public ConnectionNameStrategy cns() {
			return new SimplePropertyValueConnectionNameStrategy("spring.application.name");
		}

		@Bean
		public static ProxyListenerBPP listenerProxier() { // note static
			return new ProxyListenerBPP();
		}

		@Bean
		public ProxiedListener proxy() {
			return new ProxiedListener();
		}

		@Bean
		public static Advice wasCalled() {
			return new ListenerInterceptor();
		}

		@Bean
		public String spelReplyTo() {
			return "test.sendTo.reply.spel";
		}

		@Bean
		@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
		public Foo1 foo1Prototype() {
			return new Foo1();
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setErrorHandler(errorHandler());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			factory.setReceiveTimeout(10L);
			factory.setBeforeSendReplyPostProcessors(m -> {
				m.getMessageProperties().getHeaders().put("replyMPPApplied", true);
				m.getMessageProperties().setHeader("bean",
						m.getMessageProperties().getTargetBean().getClass().getSimpleName());
				m.getMessageProperties().setHeader("method", m.getMessageProperties().getTargetMethod().getName());
				return m;
			});
			factory.setRetryTemplate(new RetryTemplate());
			factory.setReplyRecoveryCallback(c -> null);
			return factory;
		}

		@Bean
		public SimpleRabbitListenerContainerFactory txListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setErrorHandler(errorHandler());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			factory.setReceiveTimeout(10L);
			factory.setRetryTemplate(new RetryTemplate());
			factory.setReplyRecoveryCallback(c -> null);
			factory.setChannelTransacted(true);
			return factory;
		}

		@Bean
		public SimpleMessageListenerContainer factoryCreatedContainerSimpleListener(
				SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory) {
			SimpleRabbitListenerEndpoint listener = new SimpleRabbitListenerEndpoint();
			listener.setQueueNames("test.manual.container");
			listener.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
				this.message = message;
				this.manualContainerLatch.countDown();
			});
			return rabbitListenerContainerFactory.createListenerContainer(listener);
		}

		@Bean
		public SimpleMessageListenerContainer factoryCreatedContainerNoListener(
				SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory) {
			SimpleMessageListenerContainer container = rabbitListenerContainerFactory.createListenerContainer();
			container.setMessageListener(message -> {
				this.message = message;
				this.noListenerLatch.countDown();
			});
			container.setQueueNames("test.no.listener.yet");
			return container;
		}

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitAutoStartFalseListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setReceiveTimeout(10L);
			factory.setAutoStartup(false);
			return factory;
		}

		@Bean
		public SimpleRabbitListenerContainerFactory jsonListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setErrorHandler(errorHandler());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			Jackson2JsonMessageConverter messageConverter = new Jackson2JsonMessageConverter();
			DefaultClassMapper classMapper = new DefaultClassMapper();
			Map<String, Class<?>> idClassMapping = new HashMap<>();
			idClassMapping.put(
					"org.springframework.amqp.rabbit.annotation.EnableRabbitIntegrationTests$Foo1", Foo2.class);
			classMapper.setIdClassMapping(idClassMapping);
			messageConverter.setClassMapper(classMapper);
			factory.setMessageConverter(messageConverter);
			factory.setReceiveTimeout(10L);
			factory.setConcurrentConsumers(2);
			return factory;
		}

		@Bean
		public SimpleRabbitListenerContainerFactory jsonListenerContainerFactoryNoClassMapper() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setErrorHandler(errorHandler());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			Jackson2JsonMessageConverter messageConverter = new Jackson2JsonMessageConverter();
			factory.setMessageConverter(messageConverter);
			factory.setReceiveTimeout(10L);
			factory.setConcurrentConsumers(2);
			return factory;
		}

		@Bean
		public MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		public SimpleRabbitListenerContainerFactory simpleJsonListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setErrorHandler(errorHandler());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			Jackson2JsonMessageConverter messageConverter = new Jackson2JsonMessageConverter();
			messageConverter.getJavaTypeMapper().addTrustedPackages("*");
			factory.setMessageConverter(messageConverter);
			factory.setReceiveTimeout(10L);
			factory.setContainerCustomizer(
					container -> container.setMicrometerTags(Collections.singletonMap("extraTag", "foo")));
			return factory;
		}

		@Bean
		public DirectRabbitListenerContainerFactory directListenerContainerFactory() {
			DirectRabbitListenerContainerFactory factory = new DirectRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setErrorHandler(errorHandler());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			factory.setConsumersPerQueue(2);
			return factory;
		}

		@Bean
		public SimpleRabbitListenerContainerFactory consumerBatchContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			factory.setBatchListener(true);
			factory.setBatchSize(2);
			factory.setConsumerBatchEnabled(true);
			return factory;
		}

		@Override
		public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
			registrar.setValidator(new Validator() {
				@Override
				public boolean supports(Class<?> clazz) {
					return ValidatedClass.class.isAssignableFrom(clazz);
				}

				@Override
				public void validate(Object target, Errors errors) {
					if (target instanceof ValidatedClass) {
						if (((ValidatedClass) target).getBar() > 10) {
							errors.reject("bar too large");
						}
						else {
							((ValidatedClass) target).setValidated(true);
						}
					}
				}
			});
			registrar.setCustomMethodArgumentResolvers(
				new HandlerMethodArgumentResolver() {

					@Override
					public boolean supportsParameter(MethodParameter parameter) {
						return CustomMethodArgument.class.isAssignableFrom(parameter.getParameterType());
					}

					@Override
					public Object resolveArgument(MethodParameter parameter, org.springframework.messaging.Message<?> message) {
						return new CustomMethodArgument(
								(String) message.getPayload(),
								message.getHeaders().get("customHeader", String.class)
						);
					}

				}
			);
		}

		@Bean
		public String tagPrefix() {
			return UUID.randomUUID().toString();
		}

		@Bean
		public Collection<org.springframework.amqp.core.Queue> commaQueues() {
			org.springframework.amqp.core.Queue comma3 = new org.springframework.amqp.core.Queue("test.comma.3");
			org.springframework.amqp.core.Queue comma4 = new org.springframework.amqp.core.Queue("test.comma.4");
			List<org.springframework.amqp.core.Queue> list = new ArrayList<>();
			list.add(comma3);
			list.add(comma4);
			return list;
		}

		@Bean
		public ConsumerTagStrategy consumerTagStrategy() {
			return queue -> tagPrefix() + this.increment++;
		}

		@Bean
		public CountDownLatch errorHandlerLatch1() {
			return new CountDownLatch(1);
		}

		@Bean
		public CountDownLatch errorHandlerLatch2() {
			return new CountDownLatch(1);
		}

		@Bean
		public AtomicReference<Throwable> errorHandlerError() {
			return new AtomicReference<Throwable>();
		}

		@Bean
		public ErrorHandler errorHandler() {
			ErrorHandler handler = Mockito.spy(new ConditionalRejectingErrorHandler());
			willAnswer(invocation -> {
				try {
					return invocation.callRealMethod();
				}
				catch (Throwable e) {
					errorHandlerError().set(e);
					Throwable cause = e.getCause().getCause();
					if (cause instanceof org.springframework.messaging.converter.MessageConversionException) {
						errorHandlerLatch1().countDown();
					}
					else if (cause instanceof MethodArgumentNotValidException) {
						errorHandlerLatch2().countDown();
					}
					throw e;
				}
			}).given(handler).handleError(Mockito.any(Throwable.class));
			return handler;
		}

		@Bean
		public RabbitTemplate txRabbitTemplate() {
			RabbitTemplate template = new RabbitTemplate(rabbitConnectionFactory());
			template.setChannelTransacted(true);
			return template;
		}

		@Bean
		public MyService myService() {
			return new MyService(txRabbitTemplate());
		}

		@Bean
		public RabbitListenerErrorHandler alwaysBARHandler() {
			return (msg, springMsg, ex) -> "BAR";
		}

		@Bean
		public RabbitListenerErrorHandler upcaseAndRepeatErrorHandler() {
			return (msg, springMsg, ex) -> {
				String payload = ((Bar) springMsg.getPayload()).field.toUpperCase();
				return payload + payload + " " + ex.getCause().getMessage();
 			};
		}

		@Bean
		public RabbitListenerErrorHandler throwANewException() {
			return (msg, springMsg, ex) -> {
				this.errorHandlerChannel = springMsg.getHeaders().get(AmqpHeaders.CHANNEL, Channel.class);
				throw new RuntimeException("from error handler", ex.getCause());
			};
		}

		@Bean
		public RabbitListenerErrorHandler throwWrappedValidationException() {
			return (msg, springMsg, ex) -> {
				throw new RuntimeException("argument validation failed", ex);
			};
		}

		@Bean
		public MyServiceInterface myInheritanceService() {
			return new MyServiceInterfaceImpl();
		}

		@Bean
		public MyServiceInterface2 myInheritanceService2() {
			return new MyServiceInterfaceImpl2();
		}

		@Bean
		public TxService txService() {
			return new TxServiceImpl();
		}

		@Bean
		public TaskExecutor exec1() {
			return new ThreadPoolTaskExecutor();
		}

		// Rabbit infrastructure setup

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			BrokerRunningSupport brokerRunning = RabbitAvailableCondition.getBrokerRunning();
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost(brokerRunning.getHostName());
			connectionFactory.setPort(brokerRunning.getPort());
			connectionFactory.setUsername(brokerRunning.getUser());
			connectionFactory.setPassword(brokerRunning.getPassword());
			ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
			executor.setThreadNamePrefix("rabbitClientThread-");
			executor.afterPropertiesSet();
			connectionFactory.setExecutor(executor);
			connectionFactory.setConnectionNameStrategy(cns());
			return connectionFactory;
		}

		@Bean
		public RabbitTemplate rabbitTemplate() {
			return new RabbitTemplate(rabbitConnectionFactory());
		}

		@Bean
		public RabbitTemplate jsonRabbitTemplate() {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory());
			rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
			return rabbitTemplate;
		}

		@Bean
		public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
			return new RabbitAdmin(connectionFactory);
		}

		@Bean
		public MultiListenerBean multiListener() {
			return spy(new MultiListenerBean());
		}

		@Bean
		public MultiListenerJsonBean multiListenerJson() {
			return new MultiListenerJsonBean();
		}

		@Bean
		public MultiListenerValidatedJsonBean multiListenerValidatedJsonBean() {
			return new MultiListenerValidatedJsonBean();
		}

		@Bean
		public PlatformTransactionManager transactionManager() {
			return mock(PlatformTransactionManager.class);
		}

		@Bean
		public TxClassLevel txClassLevel() {
			return new TxClassLevelImpl();
		}

		@Bean
		public org.springframework.amqp.core.Queue sendToReplies() {
			return new org.springframework.amqp.core.Queue(sendToRepliesBean(), false, false, false);
		}

		@Bean
		public org.springframework.amqp.core.Queue sendToRepliesSpEL() {
			return new org.springframework.amqp.core.Queue(sendToRepliesSpELBean(), false, false, true);
		}

		@Bean
		public String sendToRepliesSpELBean() {
			return "sendTo.replies.spel";
		}

		@Bean
		public String sendToRepliesBean() {
			return "sendTo.replies";
		}

		@Bean
		public MetaListener meta() {
			return new MetaListener();
		}

		@Bean
		public DirectExchange internal() {
			DirectExchange directExchange = new DirectExchange("auto.internal", true, true);
			directExchange.setInternal(true);
			return directExchange;
		}

		@Bean
		public ReplyPostProcessor echoPrefixHeader() {
			return (req, resp) -> {
				resp.getMessageProperties().setHeader("prefix", req.getMessageProperties().getHeader("prefix"));
				return resp;
			};
		}

		@Bean
		org.springframework.amqp.core.Queue brokerNamed() {
			return QueueBuilder.nonDurable("").autoDelete().exclusive().build();
		}

	}

	@RabbitListener(bindings = @QueueBinding
			(value = @Queue,
					exchange = @Exchange(value = "multi.exch", autoDelete = "true"),
					key = "multi.rk"), errorHandler = "upcaseAndRepeatErrorHandler")
	static class MultiListenerBean {

		volatile Object bean;

		volatile Method method;

		@RabbitHandler
		@SendTo("${foo.bar:#{sendToRepliesBean}}")
		public String bar(@NonNull Bar bar) {
			if (bar.field.equals("crash")) {
				throw new RuntimeException("Test reply from error handler");
			}
			return "BAR: " + bar.field;
		}

		@RabbitHandler
		public String baz(Baz baz, Message message) {
			this.bean = message.getMessageProperties().getTargetBean();
			this.method = message.getMessageProperties().getTargetMethod();
			return "BAZ: " + baz.field;
		}

		@RabbitHandler
		public String qux(@Header("amqp_receivedRoutingKey") String rk, @NonNull @Payload Qux qux) {
			return "QUX: " + qux.field + ": " + rk;
		}

		@RabbitHandler(isDefault = true)
		public String defaultHandler(@Payload Object payload) {
			if (payload instanceof Foo) {
				return "FOO: " + ((Foo) payload).field + " handled by default handler";
			}
			return payload.toString() + " handled by default handler";
		}

	}

	@RabbitListener(id = "multi", bindings = @QueueBinding
			(value = @Queue,
					exchange = @Exchange(value = "multi.json.exch", autoDelete = "true"),
					key = "multi.json.rk"), containerFactory = "simpleJsonListenerContainerFactory",
			errorHandler = "alwaysBARHandler", returnExceptions = "true")
	static class MultiListenerJsonBean {

		@RabbitHandler
		@SendTo("!{@sendToRepliesSpELBean}")
		public String bar(Bar bar, Message message) {
			return "BAR: " + bar.field + message.getMessageProperties().getTargetBean().getClass().getSimpleName();
		}

		@RabbitHandler
		public String baz(Baz baz) {
			return "BAZ: " + baz.field;
		}

		@RabbitHandler
		public String qux(@Header("amqp_receivedRoutingKey") String rk, @Payload Qux qux) {
			return "QUX: " + qux.field + ": " + rk;
		}

	}

	@RabbitListener(bindings = @QueueBinding
			(value = @Queue,
			 exchange = @Exchange(value = "multi.json.exch", autoDelete = "true"),
			 key = "multi.json.valid.rk"), containerFactory = "simpleJsonListenerContainerFactory",
			 returnExceptions = "true")
	static class MultiListenerValidatedJsonBean {

		final CountDownLatch latch = new CountDownLatch(1);
		volatile ValidatedClass validatedObject;

		@RabbitHandler
		public void validatedListener(@Valid @Payload ValidatedClass validatedObject) {
			this.validatedObject = validatedObject;
			latch.countDown();
		}
	}

	interface TxClassLevel {

		@Transactional
		String foo(Bar bar);

		@Transactional
		String baz(@Payload Baz baz, @Header("amqp_receivedRoutingKey") String rk);

	}

	@RabbitListener(bindings = @QueueBinding
			(value = @Queue,
					exchange = @Exchange(value = "multi.exch.tx", autoDelete = "true"),
					key = "multi.rk.tx"))
	static class TxClassLevelImpl implements TxClassLevel {

		@Override
		@RabbitHandler
		public String foo(Bar bar) {
			return "BAR: " + bar.field + bar.field;
		}

		@Override
		@RabbitHandler
		public String baz(Baz baz, String rk) {
			return "BAZ: " + baz.field + baz.field + ": " + rk;
		}

	}

	@SuppressWarnings("serial")
	static class Foo implements Serializable {

		public String field;

	}

	@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	@Retention(RetentionPolicy.RUNTIME)
	@RabbitListener(bindings = @QueueBinding(
			value = @Queue,
			exchange = @Exchange(value = "test.metaFanout", type = ExchangeTypes.FANOUT, autoDelete = "true")))
	public @interface MyAnonFanoutListener {

	}

	public static class MetaListener {

		private final CountDownLatch latch = new CountDownLatch(2);

		@MyAnonFanoutListener
		public void handle1(String foo) {
			latch.countDown();
		}

		@MyAnonFanoutListener
		public void handle2(String foo) {
			latch.countDown();
		}

	}

	@SuppressWarnings("serial")
	static class Bar extends Foo {

	}

	@SuppressWarnings("serial")
	static class Baz extends Foo {

	}

	@SuppressWarnings("serial")
	static class Qux extends Foo {

	}

	@Configuration
	@EnableRabbit
	public static class EnableRabbitConfigWithCustomConversion implements RabbitListenerConfigurer {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setMessageConverter(jsonConverter());
			factory.setReceiveTimeout(10L);
			factory.setBeforeSendReplyPostProcessors(m -> {
				m.getMessageProperties().setHeader("bean",
						m.getMessageProperties().getTargetBean().getClass().getSimpleName());
				m.getMessageProperties().setHeader("method", m.getMessageProperties().getTargetMethod().getName());
				return m;
			});
			factory.setContainerCustomizer(container -> container.setMicrometerEnabled(false));
			return factory;
		}

		// Rabbit infrastructure setup

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			BrokerRunningSupport brokerRunning = RabbitAvailableCondition.getBrokerRunning();
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost(brokerRunning.getHostName());
			connectionFactory.setPort(brokerRunning.getPort());
			connectionFactory.setUsername(brokerRunning.getUser());
			connectionFactory.setPassword(brokerRunning.getPassword());
			return connectionFactory;
		}

		@Bean
		public RabbitTemplate jsonRabbitTemplate() {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory());
			rabbitTemplate.setMessageConverter(jsonConverter());
			rabbitTemplate.setReplyTimeout(60_000);
			return rabbitTemplate;
		}

		@Bean
		public Jackson2JsonMessageConverter jsonConverter() {
			Jackson2JsonMessageConverter jackson2JsonMessageConverter = new Jackson2JsonMessageConverter();
			DefaultJackson2JavaTypeMapper mapper = Mockito.spy(TestUtils.getPropertyValue(jackson2JsonMessageConverter,
					"javaTypeMapper", DefaultJackson2JavaTypeMapper.class));
			new DirectFieldAccessor(jackson2JsonMessageConverter).setPropertyValue("javaTypeMapper", mapper);
			jackson2JsonMessageConverter.setUseProjectionForInterfaces(true);
			return jackson2JsonMessageConverter;
		}

		@Bean
		public DefaultMessageHandlerMethodFactory myHandlerMethodFactory() {
			DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
			factory.setMessageConverter(new GenericMessageConverter(myConversionService()));
			return factory;
		}

		@Bean
		public ConversionService myConversionService() {
			DefaultConversionService conv = new DefaultConversionService();
			conv.addConverter(foo1To2Converter());
			return conv;
		}

		@Override
		public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
			registrar.setMessageHandlerMethodFactory(myHandlerMethodFactory());
		}

		@Bean
		public Converter<Foo1, Foo2> foo1To2Converter() {
			return new Converter<Foo1, Foo2>() {

				@SuppressWarnings("unused")
				private boolean converted;

				@Override
				public Foo2 convert(Foo1 foo1) {
					Foo2 foo2 = new Foo2();
					foo2.setBar(foo1.getBar());
					converted = true;
					return foo2;
				}

			};
		}

		@Bean
		public Foo2Service foo2Service() {
			return new Foo2Service();
		}

	}

	@Configuration
	@EnableRabbit
	public static class EnableRabbitConfigWithCustomXmlConversion implements RabbitListenerConfigurer {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setMessageConverter(xmlConverter());
			factory.setReceiveTimeout(10L);
			factory.setContainerCustomizer(container -> container.setMicrometerEnabled(false));
			return factory;
		}

		// Rabbit infrastructure setup

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			BrokerRunningSupport brokerRunning = RabbitAvailableCondition.getBrokerRunning();
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost(brokerRunning.getHostName());
			connectionFactory.setPort(brokerRunning.getPort());
			connectionFactory.setUsername(brokerRunning.getUser());
			connectionFactory.setPassword(brokerRunning.getPassword());
			return connectionFactory;
		}

		@Bean
		public RabbitTemplate xmlRabbitTemplate() {
			RabbitTemplate rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory());
			rabbitTemplate.setMessageConverter(xmlConverter());
			return rabbitTemplate;
		}

		@Bean
		public Jackson2XmlMessageConverter xmlConverter() {
			Jackson2XmlMessageConverter jackson2XmlMessageConverter = new Jackson2XmlMessageConverter();
			DefaultJackson2JavaTypeMapper mapper = Mockito.spy(TestUtils.getPropertyValue(jackson2XmlMessageConverter,
					"javaTypeMapper", DefaultJackson2JavaTypeMapper.class));
			new DirectFieldAccessor(jackson2XmlMessageConverter).setPropertyValue("javaTypeMapper", mapper);
			return jackson2XmlMessageConverter;
		}

		@Bean
		public DefaultMessageHandlerMethodFactory myHandlerMethodFactory() {
			DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
			factory.setMessageConverter(new GenericMessageConverter(myConversionService()));
			return factory;
		}

		@Bean
		public ConversionService myConversionService() {
			DefaultConversionService conv = new DefaultConversionService();
			conv.addConverter(foo1To2Converter());
			return conv;
		}

		@Override
		public void configureRabbitListeners(RabbitListenerEndpointRegistrar registrar) {
			registrar.setMessageHandlerMethodFactory(myHandlerMethodFactory());
		}

		@Bean
		public Converter<Foo1, Foo2> foo1To2Converter() {
			return new Converter<Foo1, Foo2>() {

				@SuppressWarnings("unused")
				private boolean converted;

				@Override
				public Foo2 convert(Foo1 foo1) {
					Foo2 foo2 = new Foo2();
					foo2.setBar(foo1.getBar());
					converted = true;
					return foo2;
				}

			};
		}

		@Bean
		public Foo2Service foo2Service() {
			return new Foo2Service();
		}

	}

	public static class Foo2Service {

		volatile String stringHeader;

		volatile Integer intHeader;

		volatile Object bean;

		volatile Method method;

		@RabbitListener(queues = "test.converted")
		public Foo2 foo2(Foo2 foo2) {
			return foo2;
		}

		@RabbitListener(queues = "test.converted.list")
		public Foo2 foo2(List<Foo2> foo2s) {
			Foo2 foo2 = foo2s.get(0);
			foo2.setBar("BAZZZZ");
			return foo2;
		}

		@RabbitListener(queues = "test.converted.array")
		public Foo2 foo2(Foo2[] foo2s) {
			Foo2 foo2 = foo2s[0];
			foo2.setBar("BAZZxx");
			return foo2;
		}

		@RabbitListener(queues = "test.converted.args1")
		public String foo2(Foo2 foo2, @Header("amqp_consumerQueue") String queue) {
			return foo2 + queue;
		}

		@RabbitListener(queues = "test.converted.args2")
		public String foo2a(Foo2 foo2, @Header("amqp_consumerQueue") String queue) {
			return foo2 + queue;
		}

		@RabbitListener(queues = "test.converted.message")
		public String foo2Message(@Payload Foo2 foo2, Message message) {
			this.bean = message.getMessageProperties().getTargetBean();
			this.method = message.getMessageProperties().getTargetMethod();
			return foo2.toString() + message.getMessageProperties().getTargetMethod().getName()
					+ message.getMessageProperties().getTargetBean().getClass().getSimpleName();
		}

		@RabbitListener(queues = "test.notconverted.message")
		public String justMessage(Message message) {
			this.stringHeader = message.getMessageProperties().getHeader("stringHeader");
			this.intHeader = message.getMessageProperties().getHeader("intHeader");
			return "foo" + message.getClass().getSimpleName();
		}

		@RabbitListener(queues = "test.notconverted.channel")
		public String justChannel(Channel channel) {
			return "barAndChannel";
		}

		@RabbitListener(queues = "test.notconverted.messagechannel")
		public String messageChannel(Foo2 foo2, Message message, Channel channel) {
			return foo2 + message.getClass().getSimpleName() + "AndChannel";
		}

		@RabbitListener(queues = "test.notconverted.messagingmessage")
		public String messagingMessage(org.springframework.messaging.Message<?> message) {
			return message.getClass().getSimpleName() + message.getPayload().getClass().getSimpleName();
		}

		@RabbitListener(queues = "test.converted.foomessage")
		public String messagingMessage(org.springframework.messaging.Message<Foo2> message,
				@Header(value = "", required = false) String h,
				@Header(name = AmqpHeaders.RECEIVED_USER_ID) String userId) {
			return message.getClass().getSimpleName() + message.getPayload().getClass().getSimpleName() + userId;
		}

		@RabbitListener(queues = "test.notconverted.messagingmessagenotgeneric")
		public String messagingMessage(@SuppressWarnings("rawtypes") org.springframework.messaging.Message message,
				@Header(value = "", required = false) Integer h) {
			return message.getClass().getSimpleName() + message.getPayload().getClass().getSimpleName();
		}

		@RabbitListener(queues = "test.projection")
		public String projection(Sample in) {
			return in.getUsername() + in.getName();
		}
	}

	@SuppressWarnings("serial")
	public static class ValidatedClass implements Serializable {

		private int bar;

		private volatile boolean validated;

		volatile int valCount;

		public ValidatedClass() {
		}

		public ValidatedClass(int bar) {
			this.bar = bar;
		}

		public int getBar() {
			return this.bar;
		}

		public void setBar(int bar) {
			this.bar = bar;
		}

		public boolean isValidated() {
			return this.validated;
		}

		public void setValidated(boolean validated) {
			this.validated = validated;
			if (validated) { // don't count the json deserialization call
				this.valCount++;
			}
		}

	}

	interface Sample {

		String getUsername();

		@JsonPath("$.user.name")
		String getName();

	}


}
