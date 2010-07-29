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

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;

/**
 * 
 * @author Mark Pollack
 *
 */
public class DefaultClassMapper implements ClassMapper, InitializingBean {

	
	public static final String DEFAULT_CLASSID_FIELD_NAME = "__TypeId__";
	
	private Map<String, Class<?>> idClassMapping = new HashMap<String, Class<?>>();
	
	private Map<Class<?>, String> classIdMapping = new HashMap<Class<?>, String>();

	private String defaultHashtableTypeId = "Hashtable";
	
	private Class<?> defaultHashtableClass = Hashtable.class;

	
	public void setDefaultHashtableClass(Class<?> defaultHashtableClass) {
		this.defaultHashtableClass = defaultHashtableClass;
	}
	
	public String getClassIdFieldName() {
		return DEFAULT_CLASSID_FIELD_NAME;
	}
		
	public void setIdClassMapping(Map<String, Class<?>> idClassMapping) {
		this.idClassMapping = idClassMapping;
	}

	public String fromClass(Class<?> classOfObjectToConvert) {
		if (classIdMapping.containsKey(classOfObjectToConvert)) {
			return classIdMapping.get(classOfObjectToConvert);
		}
		if (Map.class.isAssignableFrom(classOfObjectToConvert))
		{
			return this.defaultHashtableTypeId;
		}
		return classOfObjectToConvert.getName();
	}

	public Class<?> toClass(String classId) {
		if (this.idClassMapping.containsKey(classId)) {
			return idClassMapping.get(classId);
		}
		if (classId.equals(this.defaultHashtableTypeId))
		{
			return this.defaultHashtableClass;
		}
		try {
			return ClassUtils.forName(classId, getClass().getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new MessageConversionException("failed to resolve class name [" + classId + "]", e);
		} catch (LinkageError e) {
			throw new MessageConversionException("failed to resolve class name [" + classId + "]", e);
		}
	}



	public void afterPropertiesSet() throws Exception {
		validateIdTypeMapping();		
	}

	private void validateIdTypeMapping() {
		Map<String, Class<?>> finalIdClassMapping = new HashMap<String, Class<?>>();
		for (Entry<String, Class<?>> entry : idClassMapping.entrySet()) {
		    String id = entry.getKey();
		    Class<?> clazz = entry.getValue();
		    finalIdClassMapping.put(id, clazz);
		    classIdMapping.put(clazz, id);		    
		}
		this.idClassMapping = finalIdClassMapping; 
	}

}
