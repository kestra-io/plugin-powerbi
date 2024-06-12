package io.kestra.plugin.powerbi;

import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class EmptyTest {
    @Test
    void run() throws Exception {
        assertThat(true, is(true));
    }
}
