package org.springframework.amqp.remoting;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Address;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.remoting.client.AmqpProxyFactoryBean;
import org.springframework.amqp.remoting.service.AmqpServiceMessageListener;
import org.springframework.amqp.remoting.testhelper.AbstractAmqpTemplate;
import org.springframework.amqp.remoting.testhelper.SentSavingTemplate;
import org.springframework.amqp.remoting.testservice.GeneralException;
import org.springframework.amqp.remoting.testservice.SpecialException;
import org.springframework.amqp.remoting.testservice.TestServiceImpl;
import org.springframework.amqp.remoting.testservice.TestServiceInterface;

public class RemotingTest {

	private TestServiceInterface riggedProxy;

	/**
	 * Set up a rig of directly wired-up proxy and service listener so that both can be tested together without needing
	 * a running rabbit.
	 */
	@Before
	public void initializeTestRig() throws Exception {
		// Set up the service
		TestServiceInterface testService = new TestServiceImpl();
		final AmqpServiceMessageListener serviceListener = new AmqpServiceMessageListener();
		final SentSavingTemplate sentSavingTemplate = new SentSavingTemplate();
		serviceListener.setAmqpTemplate(sentSavingTemplate);
		serviceListener.setService(testService);
		serviceListener.setServiceInterface(TestServiceInterface.class);
		serviceListener.afterPropertiesSet();

		// Set up the client
		AmqpProxyFactoryBean amqpProxyFactoryBean = new AmqpProxyFactoryBean();
		amqpProxyFactoryBean.setServiceInterface(TestServiceInterface.class);
		AmqpTemplate directForwardingTemplate = new AbstractAmqpTemplate() {
			@Override
			public Message sendAndReceive(Message message) throws AmqpException {
				Address replyTo = new Address("fakeExchange", "fakeExchangeName", "fakeRoutingKey");
				message.getMessageProperties().setReplyToAddress(replyTo);
				serviceListener.onMessage(message);
				return sentSavingTemplate.getLastMessage();
			}
		};
		amqpProxyFactoryBean.setAmqpTemplate(directForwardingTemplate);
		amqpProxyFactoryBean.afterPropertiesSet();
		Object rawProxy = amqpProxyFactoryBean.getObject();
		riggedProxy = (TestServiceInterface) rawProxy;
	}

	@Test
	public void testEcho() {
		Assert.assertEquals("Echo Test", riggedProxy.simpleStringReturningTestMethod("Test"));
	}

	@Test(expected = RuntimeException.class)
	public void testExceptionPropagation() {
		riggedProxy.exceptionThrowingMethod();
	}

	@Test(expected = GeneralException.class)
	public void testExceptionReturningMethod() {
		riggedProxy.notReallyExceptionReturningMethod();
	}

	@Test
	public void testActuallyExceptionReturningMethod() {
		SpecialException returnedException = riggedProxy.actuallyExceptionReturningMethod();

		Assert.assertNotNull(returnedException);
		Assert.assertTrue(returnedException instanceof SpecialException);
	}
}
