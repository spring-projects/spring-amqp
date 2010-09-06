/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.support.converter;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Hashtable;

import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author Mark Pollack
 */
public class JsonMessageConverterTests {

	@Test
	public void simpleTrade() {
		SimpleTrade trade = new SimpleTrade();
		trade.setAccountName("Acct1");
		trade.setBuyRequest(true);
		trade.setOrderType("Market");
		trade.setPrice(new BigDecimal(103.30));
		trade.setQuantity(100);
		trade.setRequestId("R123");
		trade.setTicker("VMW");
		trade.setUserName("Joe Trader");
		JsonMessageConverter converter = new JsonMessageConverter();
		Message message = converter.toMessage(trade, new MessageProperties());
		String classIdFieldName = converter.getClassMapper().getClassIdFieldName();
		Object classIdHeaderObject = message.getMessageProperties().getHeaders().get(classIdFieldName);
		assertEquals(String.class, classIdHeaderObject.getClass());
		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertEquals(trade, marshalledTrade);
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void hashtable() {
		Hashtable<String, String> hashtable = new Hashtable<String, String>();
		hashtable.put("TICKER", "VMW");
		hashtable.put("PRICE", "103.2");
		JsonMessageConverter converter = new JsonMessageConverter();
		Message message = converter.toMessage(hashtable, new MessageProperties());
		Hashtable<String, String> marhsalledHashtable = (Hashtable<String, String>) converter.fromMessage(message);
		assertEquals("VMW", marhsalledHashtable.get("TICKER"));
		assertEquals("103.2", marhsalledHashtable.get("PRICE"));
	}

}
