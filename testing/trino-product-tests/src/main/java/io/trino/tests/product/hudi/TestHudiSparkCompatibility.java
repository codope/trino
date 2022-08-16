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

package io.trino.tests.product.hudi;

import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HUDI;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestHudiSparkCompatibility
        extends ProductTest
{
    private static final String TRINO_CATALOG = "hudi";
    private static final String TEST_SCHEMA_NAME = "default";

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testTrinoReadingSparkData()
    {
        String baseTableName = "hudi_cow_nonpcf_tbl";
        String trinoTableName = trinoTableName(baseTableName);
        String sparkTableName = sparkTableName(baseTableName);

        onSpark().executeQuery("DROP TABLE IF EXISTS " + sparkTableName);

        onSpark().executeQuery(format(
                "CREATE TABLE %s (" +
                        "  uuid int" +
                        ", name string" +
                        ", price double" +
                        ", ts bigint" +
                        ") USING HUDI " +
                        "TBLPROPERTIES (primaryKey = 'id', preCombineField = 'ts')",
                sparkTableName));

        // Validate queries on an empty table created by Spark
        assertThat(onTrino().executeQuery(format("SELECT * FROM %s", trinoTableName))).hasNoRows();

        onSpark().executeQuery(format(
                "INSERT INTO %s VALUES (" +
                        "1" +
                        ", 'a1'" +
                        ", 20.2" +
                        ", 1000" +
                        ")",
                sparkTableName));

        QueryAssert.Row row = row(
                1,
                "a1",
                20.2,
                1000L);

        assertThat(onSpark().executeQuery(
                "SELECT " +
                        "  uuid" +
                        ", name" +
                        ", price" +
                        ", ts" +
                        " FROM " + sparkTableName))
                .containsOnly(row);

        assertThat(onTrino().executeQuery(
                "SELECT " +
                        "  uuid" +
                        ", name" +
                        ", price" +
                        ", ts" +
                        " FROM " + trinoTableName))
                .containsOnly(row);

        onSpark().executeQuery("DROP TABLE " + sparkTableName);
    }

    private static String sparkTableName(String tableName)
    {
        return format("%s.%s.%s", "SPARK_CATALOG", TEST_SCHEMA_NAME, tableName);
    }

    private static String trinoTableName(String tableName)
    {
        return format("%s.%s.%s", TRINO_CATALOG, TEST_SCHEMA_NAME, tableName);
    }
}
