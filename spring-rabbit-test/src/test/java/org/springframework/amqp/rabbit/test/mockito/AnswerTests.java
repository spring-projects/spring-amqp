/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.test.mockito;

import java.util.Collection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.spy;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
public class AnswerTests {

	@Test
	public void testLambda() {
		Foo delegate = new Foo();
		Foo foo = spy(delegate);
		willAnswer(new LambdaAnswer<String>(true, (i, r) -> r + r, delegate)).given(foo).foo(anyString());
		assertThat(foo.foo("foo")).isEqualTo("FOOFOO");
		willAnswer(new LambdaAnswer<String>(true, (i, r) -> r + i.getArguments()[0], delegate))
				.given(foo).foo(anyString());
		assertThat(foo.foo("foo")).isEqualTo("FOOfoo");
		willAnswer(new LambdaAnswer<String>(false, (i, r) ->
			"" + i.getArguments()[0] + i.getArguments()[0], delegate)).given(foo).foo(anyString());
		assertThat(foo.foo("foo")).isEqualTo("foofoo");
		LambdaAnswer<String> answer = new LambdaAnswer<>(true, (inv, result) -> result, delegate);
		willAnswer(answer).given(foo).foo("fail");
		assertThatIllegalArgumentException().isThrownBy(() -> foo.foo("fail"));
		Collection<Exception> exceptions = answer.getExceptions();
		assertThat(exceptions).hasSize(1);
		assertThat(exceptions.iterator().next()).isInstanceOf(IllegalArgumentException.class);
	}

	private static class Foo {

		Foo() {
		}

		public String foo(String foo) {
			if (foo.equals("fail")) {
				throw new IllegalArgumentException("fail");
			}
			return foo.toUpperCase();
		}

	}

}
