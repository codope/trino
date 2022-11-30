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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.hudi.HudiQueryRunner.createHudiQueryRunner;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.HUDI_NON_PART_COW;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.STOCK_TICKS_COW;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.STOCK_TICKS_MOR;

public class TestHudiSmokeTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHudiQueryRunner(ImmutableMap.of(), ImmutableMap.of(), new ResourceHudiTablesInitializer());
    }

    @Test
    public void readNonPartitionedTable()
    {
        assertQuery(
                "SELECT rowid, name FROM " + HUDI_NON_PART_COW,
                "SELECT * FROM VALUES ('row_1', 'bob'), ('row_2', 'john'), ('row_3', 'tom')");
    }

    @Test
    public void readPartitionedTables()
    {
        assertQuery("SELECT symbol, max(ts) FROM " + STOCK_TICKS_COW + " GROUP BY symbol HAVING symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");

        assertQuery("SELECT symbol, max(ts) FROM " + STOCK_TICKS_MOR + " GROUP BY symbol HAVING symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");

        assertQuery("SELECT dt, count(1) FROM " + STOCK_TICKS_MOR + " GROUP BY dt",
                "SELECT * FROM VALUES ('2018-08-31', '99')");
    }

    @Test
    public void readPartitionedTableJoin()
    {
        assertQuery(
                "SELECT symbol, ts FROM " + STOCK_TICKS_COW + " WHERE dt = '2018-08-31' AND symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");

        /*assertQuery(
                "SELECT symbol, ts FROM " + STOCK_TICKS_COW + " WHERE symbol NOT IN (SELECT t1.symbol FROM stock_ticks_cow t1 join stock_ticks_cow t2 on t1.key = t2.key AND t1.symbol = 'GOOG')",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");*/
    }
}
