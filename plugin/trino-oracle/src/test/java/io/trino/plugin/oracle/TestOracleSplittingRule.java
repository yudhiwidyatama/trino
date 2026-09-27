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
package io.trino.plugin.oracle;

import io.trino.plugin.oracle.OracleClient.RangeInfo;
import io.trino.plugin.oracle.SplittingRule.Rule;
import io.trino.plugin.oracle.SplittingRule.RuleType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.oracle.OracleClient.convertRangeInfoIntoPredicate;
import static io.trino.plugin.oracle.OracleClient.parseRules;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOracleSplittingRule
{
    @Test
    public void testParseRowidRule()
    {
        SplittingRule rules = parseRules("ROWID:orders(400)");
        List<Rule> ruleList = rules.getRules();
        assertThat(ruleList).hasSize(1);

        Rule rule = ruleList.get(0);
        assertThat(rule.getRuleType()).isEqualTo(RuleType.ROWID);
        assertThat(rule.getTableName()).isEqualTo("orders");
        assertThat(rule.getPartitions()).isEqualTo(400);
        assertThat(rule.isAnyTable()).isFalse();
    }

    @Test
    public void testParseRowidRuleWildcard()
    {
        SplittingRule rules = parseRules("ROWID:*(100)");
        List<Rule> ruleList = rules.getRules();
        assertThat(ruleList).hasSize(1);

        Rule rule = ruleList.get(0);
        assertThat(rule.getRuleType()).isEqualTo(RuleType.ROWID);
        assertThat(rule.getTableName()).isEqualTo("*");
        assertThat(rule.getPartitions()).isEqualTo(100);
        assertThat(rule.isAnyTable()).isTrue();
    }

    @Test
    public void testParseIndexRule()
    {
        SplittingRule rules = parseRules("INDEX:orders(orderkey,1000)");
        List<Rule> ruleList = rules.getRules();
        assertThat(ruleList).hasSize(1);

        Rule rule = ruleList.get(0);
        assertThat(rule.getRuleType()).isEqualTo(RuleType.INDEX);
        assertThat(rule.getTableName()).isEqualTo("orders");
        assertThat(rule.getColOrIdx()).isEqualTo("orderkey");
        assertThat(rule.getStride()).isEqualTo(1000);
        assertThat(rule.isAnyTable()).isFalse();
    }

    @Test
    public void testParseIndexRuleDefaultStride()
    {
        SplittingRule rules = parseRules("INDEX:orders(orderkey)");
        List<Rule> ruleList = rules.getRules();
        assertThat(ruleList).hasSize(1);

        Rule rule = ruleList.get(0);
        assertThat(rule.getRuleType()).isEqualTo(RuleType.INDEX);
        assertThat(rule.getTableName()).isEqualTo("orders");
        assertThat(rule.getColOrIdx()).isEqualTo("orderkey");
        assertThat(rule.getStride()).isEqualTo(50000);
    }

    @Test
    public void testParseIndexRuleDefaultColumnAndStride()
    {
        SplittingRule rules = parseRules("INDEX:customer");
        List<Rule> ruleList = rules.getRules();
        assertThat(ruleList).hasSize(1);

        Rule rule = ruleList.get(0);
        assertThat(rule.getRuleType()).isEqualTo(RuleType.INDEX);
        assertThat(rule.getTableName()).isEqualTo("customer");
        assertThat(rule.getColOrIdx()).isEqualTo("ROWNO");
        assertThat(rule.getStride()).isEqualTo(50000);
    }

    @Test
    public void testParseNtileRulePreservesColOrIdx()
    {
        // Bugfix test: NTILE rule parsing previously failed to record colOrIdx
        SplittingRule rules = parseRules("NTILE:lineitem(orderkey,300)");
        List<Rule> ruleList = rules.getRules();
        assertThat(ruleList).hasSize(1);

        Rule rule = ruleList.get(0);
        assertThat(rule.getRuleType()).isEqualTo(RuleType.NTILE);
        assertThat(rule.getTableName()).isEqualTo("lineitem");
        assertThat(rule.getColOrIdx()).isEqualTo("orderkey");
        assertThat(rule.getPartitions()).isEqualTo(300);
    }

    @Test
    public void testParseMultipleRules()
    {
        SplittingRule rules = parseRules("ROWID:table_a(100);INDEX:table_b(id,5000);NTILE:table_c(custkey,50)");
        List<Rule> ruleList = rules.getRules();
        assertThat(ruleList).hasSize(3);

        Rule r1 = ruleList.get(0);
        assertThat(r1.getRuleType()).isEqualTo(RuleType.ROWID);
        assertThat(r1.getTableName()).isEqualTo("table_a");
        assertThat(r1.getPartitions()).isEqualTo(100);

        Rule r2 = ruleList.get(1);
        assertThat(r2.getRuleType()).isEqualTo(RuleType.INDEX);
        assertThat(r2.getTableName()).isEqualTo("table_b");
        assertThat(r2.getColOrIdx()).isEqualTo("id");
        assertThat(r2.getStride()).isEqualTo(5000);

        Rule r3 = ruleList.get(2);
        assertThat(r3.getRuleType()).isEqualTo(RuleType.NTILE);
        assertThat(r3.getTableName()).isEqualTo("table_c");
        assertThat(r3.getColOrIdx()).isEqualTo("custkey");
        assertThat(r3.getPartitions()).isEqualTo(50);
    }

    @Test
    public void testParseEmptyAndBlankRules()
    {
        assertThat(parseRules("").getRules()).isEmpty();
        assertThat(parseRules("   ").getRules()).isEmpty();
    }

    @Test
    public void testRangeInfoTypeSafetyWithRowidAndInteger()
    {
        // Bugfix test: RangeInfo bounds can be Integer or ROWID String without ClassCastException
        RangeInfo rowidRange = new RangeInfo("ROWID", Optional.of("AAABBB111"), Optional.of("AAABBB222"));
        assertThat(rowidRange.isInteger()).isFalse();
        assertThat(rowidRange.getLowerBound()).contains("AAABBB111");
        assertThat(rowidRange.getUpperBound()).contains("AAABBB222");

        RangeInfo intRange = new RangeInfo("id", Optional.of(100), Optional.of(200));
        assertThat(intRange.isInteger()).isTrue();
        assertThat(intRange.getLowerBound()).contains(100);
        assertThat(intRange.getUpperBound()).contains(200);
    }

    @Test
    public void testConvertRangeInfoIntoPredicateNumeric()
    {
        // Exclusive upper bound
        RangeInfo exclusiveRange = new RangeInfo("id", Optional.of(1), Optional.of(100));
        assertThat(convertRangeInfoIntoPredicate(exclusiveRange)).isEqualTo("id >= 1 AND id < 100");

        // Bugfix test: withUpperInclusive(true) must generate '<=' to avoid boundary data loss
        RangeInfo inclusiveRange = new RangeInfo("id", Optional.of(1), Optional.of(100)).withUpperInclusive(true);
        assertThat(convertRangeInfoIntoPredicate(inclusiveRange)).isEqualTo("id >= 1 AND id <= 100");
    }

    @Test
    public void testConvertRangeInfoIntoPredicateRowid()
    {
        // Bugfix test: ROWID ranges with withUpperInclusive(true) must quote string bounds and use '<='
        RangeInfo rowidRange = new RangeInfo("ROWID", Optional.of("AAABBB111"), Optional.of("AAABBB222"))
                .withUpperInclusive(true);
        assertThat(convertRangeInfoIntoPredicate(rowidRange))
                .isEqualTo("ROWID >= 'AAABBB111' AND ROWID <= 'AAABBB222'");
    }

    @Test
    public void testConvertRangeInfoIntoPredicateSingleBound()
    {
        RangeInfo lowerOnly = new RangeInfo("id", Optional.of(50), Optional.empty());
        assertThat(convertRangeInfoIntoPredicate(lowerOnly)).isEqualTo("id >= 50");

        RangeInfo upperOnlyExclusive = new RangeInfo("id", Optional.empty(), Optional.of(50));
        assertThat(convertRangeInfoIntoPredicate(upperOnlyExclusive)).isEqualTo("id < 50");

        RangeInfo upperOnlyInclusive = new RangeInfo("id", Optional.empty(), Optional.of(50)).withUpperInclusive(true);
        assertThat(convertRangeInfoIntoPredicate(upperOnlyInclusive)).isEqualTo("id <= 50");
    }

    @Test
    public void testRangeInfoEqualsAndHashCode()
    {
        RangeInfo r1 = new RangeInfo("id", Optional.of(1), Optional.of(100));
        RangeInfo r2 = new RangeInfo("id", Optional.of(1), Optional.of(100));
        RangeInfo r3 = new RangeInfo("id", Optional.of(1), Optional.of(200));

        assertThat(r1).isEqualTo(r2);
        assertThat(r1.hashCode()).isEqualTo(r2.hashCode());
        assertThat(r1).isNotEqualTo(r3);
    }

    @Test
    public void testOracleConfigExperimentalSplitSetterBugfix()
    {
        // Bugfix test: setExperimentalSplit previously hardcoded 'this.experimentalSplit = true'
        OracleConfig config = new OracleConfig();
        config.setExperimentalSplit(false);
        assertThat(config.getExperimentalSplit()).isFalse();

        config.setExperimentalSplit(true);
        assertThat(config.getExperimentalSplit()).isTrue();
    }
}
