/*
 * Copyright 2002-2010 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package org.springframework.amqp.support.converter;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Hashtable;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ser.BeanSerializerFactory;
import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author Mark Pollack
 * @author Dave Syer
 * @author Sam Nelson
 */
public class JsonMessageConverterTests {

	private JsonMessageConverter converter;
	private SimpleTrade trade;

	@Before
	public void before(){
		converter = new JsonMessageConverter();
		trade = new SimpleTrade();
		trade.setAccountName("Acct1");
		trade.setBuyRequest(true);
		trade.setOrderType("Market");
		trade.setPrice(new BigDecimal(103.30));
		trade.setQuantity(100);
		trade.setRequestId("R123");
		trade.setTicker("VMW");
		trade.setUserName("Joe Trader");
		
	}
   @Test
   public void simpleTrade() {
      Message message = converter.toMessage(trade, new MessageProperties());

      SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
      assertEquals(trade, marshalledTrade);
   }

   @Test
   public void simpleTradeOverrideMapper() {
      ObjectMapper mapper = new ObjectMapper();
      mapper.setSerializerFactory(BeanSerializerFactory.instance);
      converter.setJsonObjectMapper(mapper);
      
      Message message = converter.toMessage(trade, new MessageProperties());

      SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
      assertEquals(trade, marshalledTrade);
   }

   @Test
   public void nestedBean() {
      Bar bar = new Bar();
      bar.getFoo().setName("spam");

      Message message = converter.toMessage(bar, new MessageProperties());

      Bar marshalled = (Bar) converter.fromMessage(message);
      assertEquals(bar, marshalled);
   }

   @Test
   @SuppressWarnings("unchecked")
   public void hashtable() {
      Hashtable<String, String> hashtable = new Hashtable<String, String>();
      hashtable.put("TICKER", "VMW");
      hashtable.put("PRICE", "103.2");
      
      Message message = converter.toMessage(hashtable, new MessageProperties());
      Hashtable<String, String> marhsalledHashtable = (Hashtable<String, String>) converter.fromMessage(message);
      
      assertEquals("VMW", marhsalledHashtable.get("TICKER"));
      assertEquals("103.2", marhsalledHashtable.get("PRICE"));
   }
   
   @Test
   public void shouldUseClassMapperWhenProvided() {
      Message message = converter.toMessage(trade, new MessageProperties());
      
      converter.setClassMapper(new DefaultClassMapper());
      converter.setJavaTypeMapper(null);
      
      SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
      assertEquals(trade, marshalledTrade);
   }

   public static class Foo {
      private String name = "foo";

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((name == null) ? 0 : name.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj) return true;
         if (obj == null) return false;
         if (getClass() != obj.getClass()) return false;
         Foo other = (Foo) obj;
         if (name == null) {
            if (other.name != null) return false;
         }
         else if (!name.equals(other.name)) return false;
         return true;
      }
   }

   public static class Bar {
      private String name = "bar";
      private Foo foo = new Foo();

      public Foo getFoo() {
         return foo;
      }

      public void setFoo(Foo foo) {
         this.foo = foo;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((foo == null) ? 0 : foo.hashCode());
         result = prime * result + ((name == null) ? 0 : name.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj) return true;
         if (obj == null) return false;
         if (getClass() != obj.getClass()) return false;
         Bar other = (Bar) obj;
         if (foo == null) {
            if (other.foo != null) return false;
         }
         else if (!foo.equals(other.foo)) return false;
         if (name == null) {
            if (other.name != null) return false;
         }
         else if (!name.equals(other.name)) return false;
         return true;
      }
   }

}
