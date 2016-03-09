/*
 * Copyright 2002-2016 the original author or authors.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author Mark Pollack
 * @author Sam Nelson
 * @author Andreas Asplund
 */
public abstract class AbstractJavaTypeMapper implements InitializingBean {
    public static final String DEFAULT_CLASSID_FIELD_NAME = "__TypeId__";
    public static final String DEFAULT_CONTENT_CLASSID_FIELD_NAME = "__ContentTypeId__";
    public static final String DEFAULT_KEY_CLASSID_FIELD_NAME = "__KeyTypeId__";

    private Map<String, Class<?>> idClassMapping = new HashMap<String, Class<?>>();
    private final Map<Class<?>, String> classIdMapping = new HashMap<Class<?>, String>();

    public String getClassIdFieldName() {
        return DEFAULT_CLASSID_FIELD_NAME;
    }

    public String getContentClassIdFieldName() {
        return DEFAULT_CONTENT_CLASSID_FIELD_NAME;
    }

    public String getKeyClassIdFieldName() {
        return DEFAULT_KEY_CLASSID_FIELD_NAME;
    }

    public void setIdClassMapping(Map<String, Class<?>> idClassMapping) {
        this.idClassMapping = idClassMapping;
    }

    protected void addHeader(MessageProperties properties, String headerName,
                           Class<?> clazz) {
        if (this.classIdMapping.containsKey(clazz)) {
            properties.getHeaders().put(headerName, this.classIdMapping.get(clazz));
        }
        else {
            properties.getHeaders().put(headerName, clazz.getName());
        }
    }

    protected String retrieveHeader(MessageProperties properties,
                                  String headerName) {
        Map<String, Object> headers = properties.getHeaders();
        Object classIdFieldNameValue = headers.get(headerName);
        String classId = null;
        if (classIdFieldNameValue != null) {
            classId = classIdFieldNameValue.toString();
        }
        if (classId == null) {
            throw new MessageConversionException(
                    "failed to convert Message content. Could not resolve "
                            + headerName + " in header");
        }
        return classId;
    }

    private void validateIdTypeMapping() {
        Map<String, Class<?>> finalIdClassMapping = new HashMap<String, Class<?>>();
        for (Map.Entry<String, Class<?>> entry : this.idClassMapping.entrySet()) {
            String id = entry.getKey();
            Class<?> clazz = entry.getValue();
            finalIdClassMapping.put(id, clazz);
            this.classIdMapping.put(clazz, id);
        }
        this.idClassMapping = finalIdClassMapping;
    }

    public Map<String, Class<?>> getIdClassMapping() {
        return Collections.unmodifiableMap(this.idClassMapping);
    }

    @Override
	public void afterPropertiesSet() throws Exception {
        validateIdTypeMapping();
    }
}
