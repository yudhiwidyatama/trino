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
package io.trino.plugin.mysql;

import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class SplittingRule
{
    List<Rule> rules = new ArrayList<>();
    int stride;

    public enum RuleType {
        INDEX,
        NTILE
    }

    public static class Rule
    {
        String tableName;
        RuleType ruleType;
        int partitions;
        int stride;
        boolean anyTable;
        String colOrIdx;

        Rule(String table, RuleType rule)
        {
            this.ruleType = rule;
            this.tableName = table;
            if ("*".equals(tableName)) {
                this.anyTable = true;
            }
        }

        public Rule withPartitions(int i)
        {
            this.partitions = i;
            return this;
        }

        public Rule withColOrIdx(String s)
        {
            this.colOrIdx = s;
            return this;
        }

        public Rule withStride(int i)
        {
            this.stride = i;
            return this;
        }

        public boolean isAnyTable()
        {
            return this.anyTable;
        }
    }

    public Rule newRule(String tableName, RuleType ruleName)
    {
        Rule r = new Rule(tableName, ruleName);
        rules.add(r);
        return r;
    }

    public static SplittingRule parseRules(String splitRule)
    {
        var rules = new SplittingRule();
        try {
            var splitRules = splitRule.split(";");
            for (int i = 0; i < splitRules.length; i++) {
                StringTokenizer tk = new StringTokenizer(splitRules[i], ":(), ");
                String ruleName = tk.hasMoreTokens() ? tk.nextToken() : ""; // INDEX:
                String tableName = tk.hasMoreTokens() ? tk.nextToken() : "*"; // TABLENAME(

                if (ruleName.equalsIgnoreCase("INDEX")) {
                    String colOrIdx = tk.hasMoreTokens() ? tk.nextToken() : "ROWNO"; // rowno
                    int stride = 50000;
                    if (tk.hasMoreTokens()) {
                        stride = Integer.parseInt(tk.nextToken()); // ,1000
                    }
                    rules.newRule(tableName, SplittingRule.RuleType.INDEX)
                            .withColOrIdx(colOrIdx)
                            .withStride(stride);
                    //rangeInfos = getRangeInfosIntegerIndex(tableRelationHandle, connection, stride, colOrIdx);
                }
                else if (ruleName.equalsIgnoreCase("NTILE")) { // NTILE:*(ROWNO,100)
                    String colOrIdx = tk.hasMoreTokens() ? tk.nextToken() : "ROWNO"; // rowno
                    int parts = 100;
                    if (tk.hasMoreTokens()) {
                        parts = Integer.parseInt(tk.nextToken()); // ,1000
                    }
                    rules.newRule(tableName, SplittingRule.RuleType.NTILE)
                            .withPartitions(parts)
                            .withColOrIdx(colOrIdx);
                    //rangeInfos = getRangeInfosNtileIndex(tableRelationHandle, connection, parts, colOrIdx);
                }
            }
        }
        catch (Exception x) {
            Logger log0 = Logger.get(MySqlClient.class);
            log0.error(" Unable to parse rule : " + splitRule);
        }
        return rules;
    }
}
