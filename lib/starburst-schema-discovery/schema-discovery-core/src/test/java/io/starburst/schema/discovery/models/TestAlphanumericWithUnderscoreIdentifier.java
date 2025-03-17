/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.schema.discovery.models.AlphanumericWithUnderscore.toAlphanumericWithUnderscore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestAlphanumericWithUnderscoreIdentifier
{
    @Test
    public void testAlphanumericWithUnderscoreNames()
    {
        List<String> validNames = ImmutableList.of(
                "table_name",
                "table_name_123",
                "t_1able_name",
                "t");
        validNames.forEach(validName ->
                assertThatNoException().isThrownBy(() -> new AlphanumericWithUnderscore(validName)));

        List<String> invalidNames = ImmutableList.of(
                "12invalid",
                "t-a-b-l-e",
                "$",
                "żółć",
                "123");
        invalidNames.forEach(invalidName ->
                assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new AlphanumericWithUnderscore(invalidName)));
    }

    @Test
    public void testAlphanumericWithUnderscoreConvertion()
    {
        assertThat(toAlphanumericWithUnderscore("12invalid")).isEqualTo(new AlphanumericWithUnderscore("invalid"));
        assertThat(toAlphanumericWithUnderscore("invalid12")).isEqualTo(new AlphanumericWithUnderscore("invalid12"));
        assertThat(toAlphanumericWithUnderscore("table$")).isEqualTo(new AlphanumericWithUnderscore("table"));
        assertThat(toAlphanumericWithUnderscore("t-a-b-l-e")).isEqualTo(new AlphanumericWithUnderscore("t_a_b_l_e"));
        assertThat(toAlphanumericWithUnderscore("test_-()<>~.?schema")).isEqualTo(new AlphanumericWithUnderscore("test__schema"));
        assertThat(toAlphanumericWithUnderscore("Zażółć gęślą jaźń")).isEqualTo(new AlphanumericWithUnderscore("zazolc_gesla_jazn"));
        assertThat(toAlphanumericWithUnderscore("ZAŻÓŁĆ GĘŚLĄ JAŹŃ")).isEqualTo(new AlphanumericWithUnderscore("zazolc_gesla_jazn"));
        assertThat(toAlphanumericWithUnderscore("ấ ê ŏ õ ô ì")).isEqualTo(new AlphanumericWithUnderscore("a_e_o_o_o_i"));
        assertThat(toAlphanumericWithUnderscore("__valid")).isEqualTo(new AlphanumericWithUnderscore("__valid"));
        assertThat(toAlphanumericWithUnderscore("      ")).isEqualTo(new AlphanumericWithUnderscore("______"));
        assertThat(toAlphanumericWithUnderscore("rm -fr .")).isEqualTo(new AlphanumericWithUnderscore("rm__fr_"));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> toAlphanumericWithUnderscore(""));
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> toAlphanumericWithUnderscore("1234".repeat(100)));
    }
}
