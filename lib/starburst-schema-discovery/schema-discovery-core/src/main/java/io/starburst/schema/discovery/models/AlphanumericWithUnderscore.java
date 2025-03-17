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

import com.google.common.collect.ImmutableMap;

import java.text.Normalizer;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/*
 * Alphanumeric identifier with restrictions:
 * - contain only lowercase base block ascii letters, underscore or digit
 * - digit cannot be first char
 * - length limited to 128
 * - `-`, ` ` converted to underscore
 */
public final class AlphanumericWithUnderscore
        implements DiscoveredIdentifier
{
    // pre UTF8 NFKD normalization remapping
    private static final Map<String, String> REMAP = ImmutableMap.<String, String>builder()
            .put("ł", "l")
            .put("Ł", "L")
            .put("þ", "b")
            .put("ø", "0")
            .put("Ø", "0")
            .put("ð", "D")
            .put("Þ", "B")
            .put("ß", "ss")
            .put("Ð", "D")
            .put("µ", "m")
            .put("æ", "ae")
            .put("Æ", "AE")
            .buildOrThrow();

    private static final Pattern PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

    private final String name;

    public AlphanumericWithUnderscore(String name)
    {
        checkArgument(!name.isBlank(), "name is blank");
        checkArgument(name.length() <= 128, "name is longer than 128 characters");
        checkArgument(PATTERN.matcher(name).matches(), "name must be alphanumeric with underscore");
        this.name = name;
    }

    @Override
    public String string()
    {
        return name;
    }

    @Override
    public String toJsonValue()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return string();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null) {
            return false;
        }
        if (o instanceof DiscoveredIdentifier that) {
            return Objects.equals(this.string(), that.string());
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    public static AlphanumericWithUnderscore toAlphanumericWithUnderscore(String rawIdentifier)
    {
        // Apply best effort ASCII folding with alphanumeric constraints
        StringBuilder normalized = new StringBuilder();
        for (int raw : Normalizer.normalize(rawIdentifier, Normalizer.Form.NFKD).codePoints().toArray()) {
            for (int codePoint : remap(Character.toString(raw)).codePoints().toArray()) {
                if (normalized.isEmpty() && isDigit(codePoint)) {
                    continue;
                }
                if (isAlphanumeric(codePoint)) {
                    normalized.append(Character.toString(Character.toLowerCase(codePoint)));
                }
                if (isUnderscore(codePoint)) {
                    normalized.append('_');
                }
            }
        }

        return new AlphanumericWithUnderscore(normalized.toString());
    }

    private static String remap(String codePoint)
    {
        return REMAP.getOrDefault(codePoint, codePoint);
    }

    private static boolean isAlphanumeric(int codePoint)
    {
        return isBasicAsciiLetter(codePoint) || isDigit(codePoint);
    }

    private static boolean isUnderscore(int codePoint)
    {
        return codePoint == '_' || codePoint == '-' || codePoint == ' ';
    }

    private static boolean isBasicAsciiLetter(int codePoint)
    {
        return (codePoint >= 'A' && codePoint <= 'Z') ||
                (codePoint >= 'a' && codePoint <= 'z');
    }

    private static boolean isDigit(int codePoint)
    {
        return codePoint >= '0' && codePoint <= '9';
    }
}
