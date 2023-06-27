/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Integer.parseInt;
import static java.time.ZoneOffset.UTC;

public final class HudiTypeUtil
{
    private HudiTypeUtil() {}

    private static final int[] NANO_FACTOR = {
            -1, // 0, no need to multiply
            100_000_000, // 1 digit after the dot
            10_000_000, // 2 digits after the dot
            1_000_000, // 3 digits after the dot
            100_000, // 4 digits after the dot
            10_000, // 5 digits after the dot
            1000, // 6 digits after the dot
            100, // 7 digits after the dot
            10, // 8 digits after the dot
            1, // 9 digits after the dot
    };

    public static long toTrinoTimestamp(String datetime)
    {
        Instant instant = toLocalDateTime(datetime).toInstant(UTC);
        return (instant.getEpochSecond() * MICROSECONDS_PER_SECOND) + (instant.getNano() / NANOSECONDS_PER_MICROSECOND);
    }

    private static LocalDateTime toLocalDateTime(String datetime)
    {
        int dotPosition = datetime.indexOf('.');
        if (dotPosition == -1) {
            // no sub-second element
            return LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime));
        }
        LocalDateTime result = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(datetime.substring(0, dotPosition)));
        // has sub-second element, so convert to nanosecond
        String nanosStr = datetime.substring(dotPosition + 1);
        int nanoOfSecond = parseInt(nanosStr) * NANO_FACTOR[nanosStr.length()];
        return result.withNano(nanoOfSecond);
    }
}
