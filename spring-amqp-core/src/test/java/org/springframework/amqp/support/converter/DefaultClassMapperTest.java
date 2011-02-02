package org.springframework.amqp.support.converter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.BDDMockito.given;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.amqp.core.MessageProperties;

@RunWith(MockitoJUnitRunner.class)
public class DefaultClassMapperTest {
	@Spy DefaultClassMapper classMapper = new DefaultClassMapper();
	private final MessageProperties props = new MessageProperties();
	@Test
	public void shouldThrowAnExceptionWhenClassIdNotPresent(){
		try{
			classMapper.toClass(props);
		}catch (MessageConversionException e) {
			String classIdFieldName = classMapper.getClassIdFieldName();
			assertThat(e.getMessage(), 
					containsString("Could not resolve " + classIdFieldName + " in header"));
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void shouldLookInTheClassIdFieldNameToFindTheClassName(){
		props.getHeaders().put("type", "java.lang.String");
		given(classMapper.getClassIdFieldName()).willReturn("type");
		
		Class<String> clazz = (Class<String>) classMapper.toClass(props);
		
		assertThat(clazz, equalTo(String.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void shouldUseTheCLassProvidedByTheLookupMapIfPresent(){
		props.getHeaders().put("__TypeId__", "trade");
		classMapper.setIdClassMapping(map("trade", SimpleTrade.class));
		
		Class clazz = classMapper.toClass(props);
		
		assertThat(clazz, equalTo(SimpleTrade.class));
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void shouldReturnHashtableForFieldWithHashtable(){
		props.getHeaders().put("__TypeId__", "Hashtable");

		Class<Hashtable> clazz = (Class<Hashtable>) classMapper.toClass(props);
		
		assertThat(clazz, equalTo(Hashtable.class));
	}

	@Test
	public void fromClassShouldPopulateWithClassNameByDefailt(){
		classMapper.fromClass(SimpleTrade.class, props);
		
		String className = (String) props.getHeaders().get(classMapper.getClassIdFieldName());
		assertThat(className, equalTo(SimpleTrade.class.getName()));
	}
	@Test
	public void shouldUseSpecialnameForClassIfPresent() throws Exception{
		classMapper.setIdClassMapping(map("daytrade", SimpleTrade.class));
		classMapper.afterPropertiesSet();
		
		classMapper.fromClass(SimpleTrade.class, props);
		
		String className = (String) props.getHeaders().get(classMapper.getClassIdFieldName());
		assertThat(className, equalTo("daytrade"));
	}
	
	@Test
	public void shouldConvertAnyMapToUseHashtables(){
		classMapper.fromClass(LinkedHashMap.class, props);

		String className = (String) props.getHeaders().get(classMapper.getClassIdFieldName());
		
		assertThat(className, equalTo("Hashtable"));
	}
	private Map<String, Class<?>> map(String string, Class<?> class1) {
		Map<String, Class<?>> map = new HashMap<String, Class<?>>();
		map.put(string, class1);
		return map;
	}
}
