package io.kestra.plugin.powerbi;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class EmptyTest {
    @Test
    void run() throws Exception {
        assertThat(true, is(true));
    }
}
