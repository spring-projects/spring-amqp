/*
 * Copyright 2014-2018 the original author or authors.
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

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitManagementTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.test.MessageTestUtils;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
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
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.lang.NonNull;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestExecutionListeners.MergeMode;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ErrorHandler;

import com.rabbitmq.client.Channel;

/**
 *
 * @author Stephane Nicoll
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.4
 */
@ContextConfiguration(classes = EnableRabbitIntegrationTests.EnableRabbitConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext
@TestExecutionListeners(mergeMode = MergeMode.MERGE_WITH_DEFAULTS,
	listeners = EnableRabbitIntegrationTests.DeleteQueuesExecutionListener.class)
public class EnableRabbitIntegrationTests {

	@ClassRule
	public static final BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues(
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
			"test.simple.direct2",
			"amqp656dlq", "test.simple.declare", "test.return.exceptions", "test.pojo.errors", "test.pojo.errors2");

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private RabbitTemplate jsonRabbitTemplate;

	@Autowired
	private RabbitAdmin rabbitAdmin;

	@Autowired
	private CountDownLatch errorHandlerLatch;

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

	@BeforeClass
	public static void setUp() {
		System.setProperty(RabbitListenerAnnotationBeanPostProcessor.RABBIT_EMPTY_STRING_ARGUMENTS_PROPERTY,
				"test-empty");
	}

	@AfterClass
	public static void tearDown() {
		System.getProperties().remove(RabbitListenerAnnotationBeanPostProcessor.RABBIT_EMPTY_STRING_ARGUMENTS_PROPERTY);
	}

	@Test
	public void autoDeclare() {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("auto.exch", "auto.rk", "foo"));
	}

	@Test
	public void autoSimpleDeclare() {
		assertEquals("FOOX", rabbitTemplate.convertSendAndReceive("test.simple.declare", "foo"));
	}

	@Test
	public void autoSimpleDeclareAnonymousQueue() {
		final SimpleMessageListenerContainer container = (SimpleMessageListenerContainer) registry
				.getListenerContainer("anonymousQueue575");
		assertThat(container.getQueueNames(), arrayWithSize(1));
		assertEquals("viaAnonymous:foo", rabbitTemplate.convertSendAndReceive(container.getQueueNames()[0], "foo"));
	}

	@Test
	public void tx() {
		assertTrue(AopUtils.isJdkDynamicProxy(this.txService));
		Baz baz = new Baz();
		baz.field = "baz";
		rabbitTemplate.setReplyTimeout(600000);
		assertEquals("BAZ: baz: auto.rk.tx", rabbitTemplate.convertSendAndReceive("auto.exch.tx", "auto.rk.tx", baz));
	}

	@Test
	public void autoDeclareFanout() {
		assertEquals("FOOFOO", rabbitTemplate.convertSendAndReceive("auto.exch.fanout", "", "foo"));
	}

	@Test
	public void autoDeclareAnon() {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("auto.exch", "auto.anon.rk", "foo"));
	}

	@Test
	public void autoStart() {
		MessageListenerContainer listenerContainer = this.registry.getListenerContainer("notStarted");
		assertNotNull(listenerContainer);
		assertFalse(listenerContainer.isRunning());
		this.registry.start();
		assertTrue(listenerContainer.isRunning());
		listenerContainer.stop();
	}

	@Test
	public void autoDeclareAnonWitAtts() {
		String received = (String) rabbitTemplate.convertSendAndReceive("auto.exch", "auto.anon.atts.rk", "foo");
		assertThat(received, startsWith("foo:"));
		org.springframework.amqp.core.Queue anonQueueWithAttributes
			= new org.springframework.amqp.core.Queue(received.substring(4), true, true, true);
		this.rabbitAdmin.declareQueue(anonQueueWithAttributes); // will fail if atts not correctly set
	}

	@Test
	public void simpleEndpoint() {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("test.simple", "foo"));
		assertEquals(2, this.context.getBean("testGroup", List.class).size());
	}

	@Test
	public void simpleDirectEndpoint() {
		MessageListenerContainer container = this.registry.getListenerContainer("direct");
		assertFalse(container.isRunning());
		container.start();
		String reply = (String) rabbitTemplate.convertSendAndReceive("test.simple.direct", "foo");
		assertThat(reply, startsWith("FOOfoo"));
		assertThat(reply, containsString("rabbitClientThread-")); // container runs on client thread
		assertThat(TestUtils.getPropertyValue(container, "consumersPerQueue"), equalTo(2));
	}

	@Test
	public void simpleDirectEndpointWithConcurrency() {
		String reply = (String) rabbitTemplate.convertSendAndReceive("test.simple.direct2", "foo");
		assertThat(reply, startsWith("FOOfoo"));
		assertThat(reply, containsString("rabbitClientThread-")); // container runs on client thread
		assertThat(TestUtils.getPropertyValue(this.registry.getListenerContainer("directWithConcurrency"),
				"consumersPerQueue"), equalTo(3));
	}

	@Test
	public void simpleInheritanceMethod() {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("test.inheritance", "foo"));
	}

	@Test
	public void simpleInheritanceClass() {
		assertEquals("FOOBAR", rabbitTemplate.convertSendAndReceive("test.inheritance.class", "foo"));
	}

	@Test
	public void commas() {
		assertEquals("FOOfoo", rabbitTemplate.convertSendAndReceive("test,with,commas", "foo"));
		List<?> commaContainers = this.context.getBean("commas", List.class);
		assertEquals(1, commaContainers.size());
		SimpleMessageListenerContainer container = (SimpleMessageListenerContainer) commaContainers.get(0);
		List<String> queueNames = Arrays.asList(container.getQueueNames());
		assertThat(queueNames,
				contains("test.comma.1", "test.comma.2", "test,with,commas", "test.comma.3", "test.comma.4"));
	}

	@Test
	public void multiListener() {
		Foo foo = new Foo();
		foo.field = "foo";
		assertEquals("FOO: foo handled by default handler",
				rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", foo));
		Bar bar = new Bar();
		bar.field = "bar";
		rabbitTemplate.convertAndSend("multi.exch", "multi.rk", bar);
		rabbitTemplate.setReceiveTimeout(10000);
		assertEquals("BAR: bar", this.rabbitTemplate.receiveAndConvert("sendTo.replies"));
		Baz baz = new Baz();
		baz.field = "baz";
		assertEquals("BAZ: baz", rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", baz));
		Qux qux = new Qux();
		qux.field = "qux";
		assertEquals("QUX: qux: multi.rk", rabbitTemplate.convertSendAndReceive("multi.exch", "multi.rk", qux));
		assertEquals("BAR: barbar", rabbitTemplate.convertSendAndReceive("multi.exch.tx", "multi.rk.tx", bar));
		assertEquals("BAZ: bazbaz: multi.rk.tx",
				rabbitTemplate.convertSendAndReceive("multi.exch.tx", "multi.rk.tx", baz));
		assertTrue(AopUtils.isJdkDynamicProxy(this.txClassLevel));
	}

	@Test
	public void multiListenerJson() {
		Bar bar = new Bar();
		bar.field = "bar";
		String exchange = "multi.json.exch";
		String routingKey = "multi.json.rk";
		assertEquals("BAR: barMultiListenerJsonBean",
				this.jsonRabbitTemplate.convertSendAndReceive(exchange, routingKey, bar));
		Baz baz = new Baz();
		baz.field = "baz";
		assertEquals("BAZ: baz", this.jsonRabbitTemplate.convertSendAndReceive(exchange, routingKey, baz));
		Qux qux = new Qux();
		qux.field = "qux";
		assertEquals("QUX: qux: multi.json.rk",
				this.jsonRabbitTemplate.convertSendAndReceive(exchange, routingKey, qux));

		// SpEL replyTo
		this.jsonRabbitTemplate.convertAndSend(exchange, routingKey, bar);
		this.jsonRabbitTemplate.setReceiveTimeout(10000);
		assertEquals("BAR: barMultiListenerJsonBean", this.jsonRabbitTemplate.receiveAndConvert("sendTo.replies.spel"));
		assertThat(TestUtils.getPropertyValue(this.registry.getListenerContainer("multi"), "concurrentConsumers"),
				equalTo(1));
	}

	@Test
	public void endpointWithHeader() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("prefix", "prefix-");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.header", request);
		assertEquals("prefix-FOO", MessageTestUtils.extractText(reply));
		assertEquals(reply.getMessageProperties().getHeaders().get("replyMPPApplied"), Boolean.TRUE);
	}

	@Test
	public void endpointWithMessage() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("prefix", "prefix-");
		Message request = MessageTestUtils.createTextMessage("foo", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.message", request);
		assertEquals("prefix-FOO", MessageTestUtils.extractText(reply));
	}

	@Test
	public void endpointWithComplexReply() {
		MessageProperties properties = new MessageProperties();
		properties.setHeader("foo", "fooValue");
		Message request = MessageTestUtils.createTextMessage("content", properties);
		Message reply = rabbitTemplate.sendAndReceive("test.reply", request);
		assertEquals("Wrong reply", "content", MessageTestUtils.extractText(reply));
		assertEquals("Wrong foo header", "fooValue", reply.getMessageProperties().getHeaders().get("foo"));
		assertThat((String) reply.getMessageProperties().getHeaders().get("bar"), startsWith(tagPrefix));
	}

	@Test
	@DirtiesContext
	public void simpleEndpointWithSendTo() throws InterruptedException {
		rabbitTemplate.convertAndSend("test.sendTo", "bar");
		rabbitTemplate.setReceiveTimeout(10000);
		Object result = rabbitTemplate.receiveAndConvert("test.sendTo.reply");
		assertNotNull(result);
		assertEquals("BAR", result);
	}

	@Test
	@DirtiesContext
	public void simpleEndpointWithSendToSpel() throws InterruptedException {
		rabbitTemplate.convertAndSend("test.sendTo.spel", "bar");
		rabbitTemplate.setReceiveTimeout(10000);
		Object result = rabbitTemplate.receiveAndConvert("test.sendTo.reply.spel");
		assertNotNull(result);
		assertEquals("BARbar", result);
	}

	@Test
	public void simpleEndpointWithSendToSpelRuntime() throws InterruptedException {
		rabbitTemplate.convertAndSend("test.sendTo.runtimespel", "spel");
		rabbitTemplate.setReceiveTimeout(10000);
		Object result = rabbitTemplate.receiveAndConvert("test.sendTo.reply.runtimespel");
		assertNotNull(result);
		assertEquals("runtimespel", result);
	}

	@Test
	public void simpleEndpointWithSendToSpelRuntimeMessagingMessage() throws InterruptedException {
		rabbitTemplate.convertAndSend("test.sendTo.runtimespelsource", "spel");
		rabbitTemplate.setReceiveTimeout(10000);
		Object result = rabbitTemplate.receiveAndConvert("test.sendTo.runtimespelsource.reply");
		assertNotNull(result);
		assertEquals("sourceEval", result);
	}

	@Test
	public void testInvalidPojoConversion() throws InterruptedException {
		this.rabbitTemplate.convertAndSend("test.invalidPojo", "bar");

		assertTrue(this.errorHandlerLatch.await(10, TimeUnit.SECONDS));
		Throwable throwable = this.errorHandlerError.get();
		assertNotNull(throwable);
		assertThat(throwable, instanceOf(AmqpRejectAndDontRequeueException.class));
		assertThat(throwable.getCause(), instanceOf(ListenerExecutionFailedException.class));
		assertThat(throwable.getCause().getCause(),
				instanceOf(org.springframework.messaging.converter.MessageConversionException.class));
		assertThat(throwable.getCause().getCause().getMessage(),
				containsString("Failed to convert message payload 'bar' to 'java.util.Date'"));
	}

	@Test
	public void testDifferentTypes() throws InterruptedException {
		Foo1 foo = new Foo1();
		foo.setBar("bar");
		this.jsonRabbitTemplate.convertAndSend("differentTypes", foo);
		assertTrue(this.service.latch.await(10, TimeUnit.SECONDS));
		assertThat(this.service.foos.get(0), instanceOf(Foo2.class));
		assertEquals("bar", ((Foo2) this.service.foos.get(0)).getBar());
		assertThat(TestUtils.getPropertyValue(this.registry.getListenerContainer("different"), "concurrentConsumers"),
				equalTo(2));
	}

	@Test
	public void testDifferentTypesWithConcurrency() throws InterruptedException {
		Foo1 foo = new Foo1();
		foo.setBar("bar");
		this.jsonRabbitTemplate.convertAndSend("differentTypes", foo);
		assertTrue(this.service.latch.await(10, TimeUnit.SECONDS));
		assertThat(this.service.foos.get(0), instanceOf(Foo2.class));
		assertEquals("bar", ((Foo2) this.service.foos.get(0)).getBar());
		MessageListenerContainer container = this.registry.getListenerContainer("differentWithConcurrency");
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers"), equalTo(3));
		assertNull(TestUtils.getPropertyValue(container, "maxConcurrentConsumers"));
	}

	@Test
	public void testDifferentTypesWithVariableConcurrency() throws InterruptedException {
		Foo1 foo = new Foo1();
		foo.setBar("bar");
		this.jsonRabbitTemplate.convertAndSend("differentTypes", foo);
		assertTrue(this.service.latch.await(10, TimeUnit.SECONDS));
		assertThat(this.service.foos.get(0), instanceOf(Foo2.class));
		assertEquals("bar", ((Foo2) this.service.foos.get(0)).getBar());
		MessageListenerContainer container = this.registry.getListenerContainer("differentWithVariableConcurrency");
		assertThat(TestUtils.getPropertyValue(container, "concurrentConsumers"), equalTo(3));
		assertThat(TestUtils.getPropertyValue(container, "maxConcurrentConsumers"), equalTo(4));
	}

	@Test
	public void testInterceptor() throws InterruptedException {
		this.rabbitTemplate.convertAndSend("test.intercepted", "intercept this");
		assertTrue(this.interceptor.oneWayLatch.await(10, TimeUnit.SECONDS));
		assertEquals("INTERCEPT THIS",
				this.rabbitTemplate.convertSendAndReceive("test.intercepted.withReply", "intercept this"));
		assertTrue(this.interceptor.twoWayLatch.await(10, TimeUnit.SECONDS));
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
		assertThat(returned, instanceOf(Foo2.class));
		assertEquals("bar", ((Foo2) returned).getBar());
		assertTrue(TestUtils.getPropertyValue(ctx.getBean("foo1To2Converter"), "converted", Boolean.class));
		converter.setTypePrecedence(TypePrecedence.INFERRED);

		// No type info in message
		template.setMessageConverter(new SimpleMessageConverter());
		@SuppressWarnings("resource")
		MessagePostProcessor messagePostProcessor = message -> {
			message.getMessageProperties().setContentType("application/json");
			message.getMessageProperties().setUserId("guest");
			return message;
		};
		returned = template.convertSendAndReceive("", "test.converted", "{ \"bar\" : \"baz\" }", messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("{\"bar\":\"baz\"}", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.converted.list", "[ { \"bar\" : \"baz\" } ]",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("{\"bar\":\"BAZZZZ\"}", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.converted.array", "[ { \"bar\" : \"baz\" } ]",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("{\"bar\":\"BAZZxx\"}", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.converted.args1", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"bar=baztest.converted.args1\"", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.converted.args2", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"bar=baztest.converted.args2\"", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.converted.message", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"bar=bazfoo2MessageFoo2Service\"", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.notconverted.message", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"fooMessage\"", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.notconverted.channel", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"barAndChannel\"", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.notconverted.messagechannel", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"bar=bazMessageAndChannel\"", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.notconverted.messagingmessage", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"GenericMessageLinkedHashMap\"", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.converted.foomessage", "{ \"bar\" : \"baz\" }",
				messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"GenericMessageFoo2guest\"", new String((byte[]) returned));

		returned = template.convertSendAndReceive("", "test.notconverted.messagingmessagenotgeneric",
				"{ \"bar\" : \"baz\" }", messagePostProcessor);
		assertThat(returned, instanceOf(byte[].class));
		assertEquals("\"GenericMessageLinkedHashMap\"", new String((byte[]) returned));

		Jackson2JsonMessageConverter jsonConverter = ctx.getBean(Jackson2JsonMessageConverter.class);

		DefaultJackson2JavaTypeMapper mapper = TestUtils.getPropertyValue(jsonConverter, "javaTypeMapper",
				DefaultJackson2JavaTypeMapper.class);
		Mockito.verify(mapper).setBeanClassLoader(ctx.getClassLoader());

		ctx.close();
	}

	@Test
	public void testMeta() throws Exception {
		this.rabbitTemplate.convertAndSend("test.metaFanout", "", "foo");
		assertTrue(this.metaListener.latch.await(10, TimeUnit.SECONDS));
	}

	@Test
	public void testHeadersExchange() throws Exception {
		assertEquals("FOO", rabbitTemplate.convertSendAndReceive("auto.headers", "", "foo",
				message -> {
					message.getMessageProperties().getHeaders().put("foo", "bar");
					message.getMessageProperties().getHeaders().put("baz", "qux");
					return message;
				}));
		assertEquals("BAR", rabbitTemplate.convertSendAndReceive("auto.headers", "", "bar",
				message -> {
					message.getMessageProperties().getHeaders().put("baz", "fiz");
					return message;
				}));
	}

	@Test
	public void deadLetterOnDefaultExchange() {
		this.rabbitTemplate.convertAndSend("amqp656", "foo");
		assertEquals("foo", this.rabbitTemplate.receiveAndConvert("amqp656dlq", 10000));
		try {
			RabbitManagementTemplate rmt = new RabbitManagementTemplate();
			org.springframework.amqp.core.Queue amqp656 = rmt.getQueue("amqp656");
			if (amqp656 != null) {
				assertEquals("", amqp656.getArguments().get("test-empty"));
				assertEquals("undefined", amqp656.getArguments().get("test-null"));
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
			assertThat(e.getCause().getMessage(), equalTo("return this"));
		}
	}

	@Test
	public void listenerErrorHandler() {
		assertEquals("BAR", this.rabbitTemplate.convertSendAndReceive("test.pojo.errors", "foo"));
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
			assertThat(e.getCause().getMessage(), equalTo("from error handler"));
			assertThat(e.getCause().getCause().getMessage(), equalTo("return this"));
		}
	}

	@Test
	public void testPrototypeCache() {
		RabbitListenerAnnotationBeanPostProcessor bpp =
				this.context.getBean(RabbitListenerAnnotationBeanPostProcessor.class);
		@SuppressWarnings("unchecked")
		Map<Class<?>, ?> typeCache = TestUtils.getPropertyValue(bpp, "typeCache", Map.class);
		assertFalse(typeCache.containsKey(Foo1.class));
		this.context.getBean("foo1Prototype");
		assertTrue(typeCache.containsKey(Foo1.class));
		Object value = typeCache.get(Foo1.class);
		this.context.getBean("foo1Prototype");
		assertTrue(typeCache.containsKey(Foo1.class));
		assertSame(value, typeCache.get(Foo1.class));
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

		@RabbitListener(bindings = @QueueBinding(
				value = @Queue(value = "auto.declare", autoDelete = "true"),
				exchange = @Exchange(value = "auto.exch", autoDelete = "true"),
				key = "auto.rk")
		)
		public String handleWithDeclare(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(queuesToDeclare = @Queue(name = "${jjjj:test.simple.declare}", durable = "true"))
		public String handleWithSimpleDeclare(String foo) {
			return foo.toUpperCase() + "X";
		}

		@RabbitListener(queuesToDeclare = @Queue, id = "anonymousQueue575")
		public String handleWithAnonymousQueueToDeclare(String data) throws Exception {
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
					key = "auto.anon.rk")}
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

		@RabbitListener(queues = {"#{'test.comma.1,test.comma.2'.split(',')}",
								  "test,with,commas",
								  "#{commaQueues}"},
						group = "commas")
		public String multiQueuesConfig(String foo) {
			return foo.toUpperCase() + foo;
		}

		@RabbitListener(queues = "test.header", group = "testGroup")
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
		@SendTo("test.sendTo.reply")
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

		private final List<Object> foos = new ArrayList<Object>();

		private final CountDownLatch latch = new CountDownLatch(1);

		@RabbitListener(id = "different", queues = "differentTypes", containerFactory = "jsonListenerContainerFactory")
		public void handleDifferent(Foo2 foo) {
			foos.add(foo);
			latch.countDown();
		}

		@RabbitListener(id = "differentWithConcurrency", queues = "differentTypes2",
				containerFactory = "jsonListenerContainerFactory", concurrency = "#{3}")
		public void handleDifferentWithConcurrency(Foo2 foo) {
			foos.add(foo);
			latch.countDown();
		}

		@RabbitListener(id = "differentWithVariableConcurrency", queues = "differentTypes3",
				containerFactory = "jsonListenerContainerFactory", concurrency = "3-4")
		public void handleDifferentWithVariableConcurrency(Foo2 foo) {
			foos.add(foo);
			latch.countDown();
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
					key = "auto.internal.rk")}
		)
		public String handleWithInternalExchange(String foo) {
			return foo.toUpperCase();
		}

		@RabbitListener(bindings = {
				@QueueBinding(
					value = @Queue,
					exchange = @Exchange(value = "auto.internal", autoDelete = "true",
								ignoreDeclarationExceptions = "true"),
					key = "auto.internal.rk")}
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

		@RabbitListener(queues = "test.pojo.errors", errorHandler = "alwaysBARHandler")
		public String alwaysFailsWithErrorHandler(String data) throws Exception {
			throw new Exception("return this");
		}

		@RabbitListener(queues = "test.pojo.errors2", errorHandler = "throwANewException", returnExceptions = "true")
		public String alwaysFailsWithErrorHandlerThrowAnother(String data) throws Exception {
			throw new Exception("return this");
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

	public static class ProxyListenerBPP implements BeanPostProcessor, BeanFactoryAware, Ordered, PriorityOrdered {

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
	public static class EnableRabbitConfig {

		private int increment;

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
				return m;
			});
			return factory;
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
			Map<String, Class<?>> idClassMapping = new HashMap<String, Class<?>>();
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
		public SimpleRabbitListenerContainerFactory simpleJsonListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			factory.setErrorHandler(errorHandler());
			factory.setConsumerTagStrategy(consumerTagStrategy());
			factory.setMessageConverter(new Jackson2JsonMessageConverter());
			factory.setReceiveTimeout(10L);
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
		public String tagPrefix() {
			return UUID.randomUUID().toString();
		}

		@Bean
		public Collection<org.springframework.amqp.core.Queue> commaQueues() {
			org.springframework.amqp.core.Queue comma3 = new org.springframework.amqp.core.Queue("test.comma.3");
			org.springframework.amqp.core.Queue comma4 = new org.springframework.amqp.core.Queue("test.comma.4");
			List<org.springframework.amqp.core.Queue> list = new ArrayList<org.springframework.amqp.core.Queue>();
			list.add(comma3);
			list.add(comma4);
			return list;
		}

		@Bean
		public ConsumerTagStrategy consumerTagStrategy() {
			return queue -> tagPrefix() + this.increment++;
		}

		@Bean
		public CountDownLatch errorHandlerLatch() {
			return new CountDownLatch(1);
		}

		@Bean
		public AtomicReference<Throwable> errorHandlerError() {
			return new AtomicReference<Throwable>();
		}

		@Bean
		public ErrorHandler errorHandler() {
			ErrorHandler handler = Mockito.spy(new ConditionalRejectingErrorHandler());
			doAnswer(invocation -> {
				try {
					return invocation.callRealMethod();
				}
				catch (Throwable e) {
					errorHandlerError().set(e);
					errorHandlerLatch().countDown();
					throw e;
				}
			}).when(handler).handleError(Mockito.any(Throwable.class));
			return handler;
		}

		@Bean
		public MyService myService() {
			return new MyService();
		}

		@Bean
		public RabbitListenerErrorHandler alwaysBARHandler() {
			return (m, sm, e) -> "BAR";
		}

		@Bean
		public RabbitListenerErrorHandler throwANewException() {
			return (m, sm, e) -> {
				throw new RuntimeException("from error handler", e.getCause());
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

		// Rabbit infrastructure setup

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost(brokerRunning.getHostName());
			connectionFactory.setPort(brokerRunning.getPort());
			connectionFactory.setUsername(brokerRunning.getUser());
			connectionFactory.setPassword(brokerRunning.getPassword());
			ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
			executor.setThreadNamePrefix("rabbitClientThread-");
			executor.afterPropertiesSet();
			connectionFactory.setExecutor(executor);
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
			return new MultiListenerBean();
		}

		@Bean
		public MultiListenerJsonBean multiListenerJson() {
			return new MultiListenerJsonBean();
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
			return new org.springframework.amqp.core.Queue(sendToRepliesBean(), false, false, true);
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

	}

	@RabbitListener(bindings = @QueueBinding
			(value = @Queue,
			exchange = @Exchange(value = "multi.exch", autoDelete = "true"),
			key = "multi.rk"))
	static class MultiListenerBean {

		@RabbitHandler
		@SendTo("#{sendToRepliesBean}")
		public String bar(@NonNull Bar bar) {
			return "BAR: " + bar.field;
		}

		@RabbitHandler
		public String baz(Baz baz) {
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
			key = "multi.json.rk"), containerFactory = "simpleJsonListenerContainerFactory")
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

	@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
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
	static class Bar extends Foo implements Serializable {

	}

	@SuppressWarnings("serial")
	static class Baz extends Foo implements Serializable {

	}

	@SuppressWarnings("serial")
	static class Qux extends Foo implements Serializable {

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
			return factory;
		}

		// Rabbit infrastructure setup

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
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
			return rabbitTemplate;
		}

		@Bean
		public Jackson2JsonMessageConverter jsonConverter() {
			Jackson2JsonMessageConverter jackson2JsonMessageConverter = new Jackson2JsonMessageConverter();
			DefaultJackson2JavaTypeMapper mapper = Mockito.spy(TestUtils.getPropertyValue(jackson2JsonMessageConverter,
					"javaTypeMapper", DefaultJackson2JavaTypeMapper.class));
			new DirectFieldAccessor(jackson2JsonMessageConverter).setPropertyValue("javaTypeMapper", mapper);
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

	public static class Foo2Service {

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
		public String foo2a(@Payload Foo2 foo2, @Header("amqp_consumerQueue") String queue) {
			return foo2 + queue;
		}

		@RabbitListener(queues = "test.converted.message")
		public String foo2Message(@Payload Foo2 foo2, Message message) {
			return foo2.toString() + message.getMessageProperties().getTargetMethod().getName()
					+ message.getMessageProperties().getTargetBean().getClass().getSimpleName();
		}

		@RabbitListener(queues = "test.notconverted.message")
		public String justMessage(Message message) {
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

	}

	/**
	 * Defer queue deletion until after the context has been stopped by the
	 * {@link DirtiesContext}.
	 *
	 */
	public static class DeleteQueuesExecutionListener extends AbstractTestExecutionListener {

		@Override
		public void afterTestClass(TestContext testContext) throws Exception {
			brokerRunning.removeTestQueues("sendTo.replies", "sendTo.replies.spel");
		}

		@Override
		public int getOrder() {
			return Ordered.HIGHEST_PRECEDENCE;
		}

	}

}
