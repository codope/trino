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
import io.trino.plugin.hudi.testing.ResourceHudiDataLoader;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.hudi.HudiQueryRunners.createHudiQueryRunner;
import static io.trino.plugin.hudi.testing.ResourceHudiDataLoader.TestingTable.HUDI_NON_PART_COW;
import static io.trino.plugin.hudi.testing.ResourceHudiDataLoader.TestingTable.STOCK_TICKS_COW;
import static io.trino.plugin.hudi.testing.ResourceHudiDataLoader.TestingTable.STOCK_TICKS_MOR;

public class TestHudiSmokeTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHudiQueryRunner(ImmutableMap.of(), ImmutableMap.of(), ResourceHudiDataLoader.factory());
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
}
