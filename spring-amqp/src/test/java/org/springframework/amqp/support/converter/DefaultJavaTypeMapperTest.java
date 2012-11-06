/*
 * Copyright 2002-2011 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */
package org.springframework.amqp.support.converter;

import static org.codehaus.jackson.map.type.TypeFactory.collectionType;
import static org.codehaus.jackson.map.type.TypeFactory.mapType;
import static org.codehaus.jackson.map.type.TypeFactory.type;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.BDDMockito.given;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.type.JavaType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.amqp.core.MessageProperties;

/**
 * @author James Carr
 * @author Sam Nelson
 */

@RunWith(MockitoJUnitRunner.class)
public class DefaultJavaTypeMapperTest {
   @Spy
   DefaultJavaTypeMapper javaTypeMapper = new DefaultJavaTypeMapper();
   private final MessageProperties properties = new MessageProperties();

   @SuppressWarnings("rawtypes")
   private Class<ArrayList> containerClass = ArrayList.class;

   @SuppressWarnings("rawtypes")
   private Class<HashMap> mapClass = HashMap.class;

   @Test
   public void shouldThrowAnExceptionWhenClassIdNotPresent() {
      try {
         javaTypeMapper.toJavaType(properties);
      }
      catch (MessageConversionException e) {
         String classIdFieldName = javaTypeMapper.getClassIdFieldName();
         assertThat(e.getMessage(), containsString("Could not resolve " + classIdFieldName + " in header"));
         return;
      }
      fail();
   }

   @Test
   public void shouldLookInTheClassIdFieldNameToFindTheClassName() {
      properties.getHeaders().put("type", "java.lang.String");
      given(javaTypeMapper.getClassIdFieldName()).willReturn("type");

      JavaType javaType = javaTypeMapper.toJavaType(properties);

      assertThat(javaType, equalTo(type(String.class)));
   }

   @Test
   public void shouldUseTheClassProvidedByTheLookupMapIfPresent() {
      properties.getHeaders().put("__TypeId__", "trade");
      javaTypeMapper.setIdClassMapping(map("trade", SimpleTrade.class));

      JavaType javaType = javaTypeMapper.toJavaType(properties);

      assertEquals(javaType, type(SimpleTrade.class));
   }

   @Test
   public void fromJavaTypeShouldPopulateWithJavaTypeNameByDefault() {
      javaTypeMapper.fromJavaType(type(SimpleTrade.class), properties);

      String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
      assertThat(className, equalTo(SimpleTrade.class.getName()));
   }

   @Test
   public void shouldUseSpecialNameForClassIfPresent() throws Exception {
      javaTypeMapper.setIdClassMapping(map("daytrade", SimpleTrade.class));
      javaTypeMapper.afterPropertiesSet();

      javaTypeMapper.fromJavaType(type(SimpleTrade.class), properties);

      String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
      assertThat(className, equalTo("daytrade"));
   }

   @Test
   public void shouldThrowAnExceptionWhenContentClassIdIsNotPresentWhenClassIdIsContainerType() {
      properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), ArrayList.class.getName());

      try {
         javaTypeMapper.toJavaType(properties);
      }
      catch (MessageConversionException e) {
         String contentClassIdFieldName = javaTypeMapper.getContentClassIdFieldName();
         assertThat(e.getMessage(), containsString("Could not resolve " + contentClassIdFieldName + " in header"));
         return;
      }
      fail();
   }

   @Test
   public void shouldLookInTheContentClassIdFieldNameToFindTheContainerClassIDWhenClassIdIsContainerType() {
      properties.getHeaders().put("contentType", "java.lang.String");
      properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), ArrayList.class.getName());
      given(javaTypeMapper.getContentClassIdFieldName()).willReturn("contentType");

      JavaType javaType = javaTypeMapper.toJavaType(properties);

      assertThat(javaType, equalTo(collectionType(ArrayList.class, String.class)));
   }

   @Test
   public void shouldUseTheContentClassProvidedByTheLookupMapIfPresent() {

      properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), containerClass.getName());
      properties.getHeaders().put("__ContentTypeId__", "trade");

      Map<String, Class<?>> map = map("trade", SimpleTrade.class);
      map.put(javaTypeMapper.getClassIdFieldName(), containerClass);
      javaTypeMapper.setIdClassMapping(map);

      JavaType javaType = javaTypeMapper.toJavaType(properties);

      assertThat(javaType, equalTo(collectionType(containerClass, type(SimpleTrade.class))));
   }

   @Test
   public void fromJavaTypeShouldPopulateWithContentTypeJavaTypeNameByDefault() {

      javaTypeMapper.fromJavaType(collectionType(containerClass, type(SimpleTrade.class)),
                                  properties);

      String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
      String contentClassName = (String) properties.getHeaders().get(javaTypeMapper.getContentClassIdFieldName());

      assertThat(className, equalTo(ArrayList.class.getName()));
      assertThat(contentClassName, equalTo(SimpleTrade.class.getName()));
   }

   @Test
   public void shouldThrowAnExceptionWhenKeyClassIdIsNotPresentWhenClassIdIsAMap() {
      properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), HashMap.class.getName());
      properties.getHeaders().put(javaTypeMapper.getKeyClassIdFieldName(), String.class.getName());

      try {
         javaTypeMapper.toJavaType(properties);
      }
      catch (MessageConversionException e) {
         String contentClassIdFieldName = javaTypeMapper.getContentClassIdFieldName();
         assertThat(e.getMessage(), containsString("Could not resolve " + contentClassIdFieldName + " in header"));
         return;
      }
      fail();
   }

   @Test
   public void shouldLookInTheValueClassIdFieldNameToFindTheValueClassIDWhenClassIdIsAMap() {
      properties.getHeaders().put("keyType", "java.lang.Integer");
      properties.getHeaders().put(javaTypeMapper.getContentClassIdFieldName(), "java.lang.String");
      properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), HashMap.class.getName());
      given(javaTypeMapper.getKeyClassIdFieldName()).willReturn("keyType");

      JavaType javaType = javaTypeMapper.toJavaType(properties);

      assertThat(javaType, equalTo(mapType(HashMap.class, Integer.class, String.class)));
   }

   @Test
   public void shouldUseTheKeyClassProvidedByTheLookupMapIfPresent() {
      properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), mapClass.getName());
      properties.getHeaders().put(javaTypeMapper.getContentClassIdFieldName(), "java.lang.String");
      properties.getHeaders().put("__KeyTypeId__", "trade");

      Map<String, Class<?>> map = map("trade", SimpleTrade.class);
      map.put(javaTypeMapper.getClassIdFieldName(), mapClass);
      map.put(javaTypeMapper.getContentClassIdFieldName(), String.class);
      javaTypeMapper.setIdClassMapping(map);

      JavaType javaType = javaTypeMapper.toJavaType(properties);

      assertThat(javaType,
                 equalTo(mapType(mapClass, type(SimpleTrade.class),
                                             type(String.class))));
   }

   @Test
   public void fromJavaTypeShouldPopulateWithKeyTypeAndContentJavaTypeNameByDefault() {

      javaTypeMapper.fromJavaType(mapType(mapClass, type(SimpleTrade.class),
                                                      type(String.class)), properties);

      String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
      String contentClassName = (String) properties.getHeaders().get(javaTypeMapper.getContentClassIdFieldName());
      String keyClassName = (String) properties.getHeaders().get(javaTypeMapper.getKeyClassIdFieldName());

      assertThat(className, equalTo(HashMap.class.getName()));
      assertThat(contentClassName, equalTo(String.class.getName()));
      assertThat(keyClassName, equalTo(SimpleTrade.class.getName()));
   }
   
   @Test
   public void fromClassShouldPopulateWithJavaTypeNameByDefault() {
      javaTypeMapper.fromClass(SimpleTrade.class, properties);

      String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
      assertThat(className, equalTo(SimpleTrade.class.getName()));
   }

   @Test
   public void toClassShouldUseTheClassProvidedByTheLookupMapIfPresent() {
      properties.getHeaders().put("__TypeId__", "trade");
      javaTypeMapper.setIdClassMapping(map("trade", SimpleTrade.class));

      Class<?> clazz = javaTypeMapper.toClass(properties);

      assertEquals(SimpleTrade.class, clazz);
   }
   
   private Map<String, Class<?>> map(String string, Class<?> clazz) {
      Map<String, Class<?>> map = new HashMap<String, Class<?>>();
      map.put(string, clazz);
      return map;
   }
}
