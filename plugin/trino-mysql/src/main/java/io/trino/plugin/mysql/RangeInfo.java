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
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.spi.TrinoException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RangeInfo
{
    private final boolean isInteger;
    private final String expression;
    /**
     * The lower bound of the range (included)
     */
    private final Optional lowerBound;
    /**
     * The upper bound of the range (not include the upperBond itself)
     */
    private final Optional upperBound;

    boolean upperInclusive;

    public RangeInfo(String expression, Optional lowerBound, Optional upperBound)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.lowerBound = requireNonNull(lowerBound, "lowerBound is null");
        this.upperBound = requireNonNull(upperBound, "upperBound is null");
        if ((lowerBound.isPresent() && (lowerBound.get() instanceof Integer))
                || (upperBound.isPresent() && (upperBound.get() instanceof Integer))) {
            this.isInteger = true;
        }
        else {
            this.isInteger = false;
        }
    }

    public void setUpperInclusive(boolean b)
    {
        this.upperInclusive = b;
    }

    public RangeInfo withUpperInclusive(boolean b)
    {
        this.upperInclusive = b;
        return this;
    }

    public String getExpression()
    {
        return expression;
    }

    public Optional getLowerBound()
    {
        return lowerBound;
    }

    public Optional getUpperBound()
    {
        return upperBound;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RangeInfo that = (RangeInfo) obj;

        return Objects.equals(this.expression, that.expression)
                && Objects.equals(this.lowerBound, that.lowerBound)
                && Objects.equals(this.upperBound, that.upperBound);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, lowerBound, upperBound);
    }

    @Override
    public String toString()
    {
        return "[" + lowerBound + ", " + upperBound + ")";
    }

    public boolean isUpperInclusive()
    {
        return this.upperInclusive;
    }

    public boolean isInteger()
    {
        return this.isInteger;
    }

    public static Optional<List<RangeInfo>> adjustRange(Optional<List<RangeInfo>> rangeInfoList, Map<JdbcColumnHandle, String> projections)
    {
        if (rangeInfoList.isEmpty() || (rangeInfoList.get().size() == 0)) {
            return rangeInfoList; // if empty, do nothing
        }
        var rangeInfoList1 = rangeInfoList.get();
        var firstRange = rangeInfoList1.get(0);
        var columnPart = firstRange.expression;
        for (var key : projections.keySet()) {
            if (key.getColumnName().equalsIgnoreCase(columnPart)) {
                // bingo
                columnPart = projections.get(key);
                break;
            }
        }

        for (int i = 0; i < rangeInfoList1.size(); i++) {
            var range = rangeInfoList1.get(i);
            rangeInfoList1.set(i, new RangeInfo(columnPart, range.getLowerBound(), range.getUpperBound()));
            // replace elements
        }
        return rangeInfoList; // which contract did I break?
    }

    public static Optional<List<RangeInfo>> getRangeInfos(JdbcNamedRelationHandle tableRelationHandle, Connection connection, SplittingRule rules)
    {
        var thisTableName = tableRelationHandle.getSchemaTableName().getTableName();
        for (SplittingRule.Rule r : rules.rules) {
            boolean tableNameMatch = r.isAnyTable() ||
                    r.tableName
                            .equalsIgnoreCase(thisTableName);
            if (!tableNameMatch) {
                continue;
            }
            switch (r.ruleType) {
                case SplittingRule.RuleType.INDEX:
                    return getRangeInfosIntegerIndex(tableRelationHandle, connection, r.stride, r.colOrIdx);
                case SplittingRule.RuleType.NTILE:
                    return getRangeInfosNtileIndex(tableRelationHandle, connection, r.partitions, r.colOrIdx);
                default:
                    return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public static Optional<List<RangeInfo>> getRangeInfosIntegerIndex(JdbcNamedRelationHandle tableRelationHandle,
            Connection connection, int stride, String colOrIdx)
    {
        Map<String, List<String>> masterIndex = new HashMap<>();
        Logger log = Logger.get(RangeInfo.class);
        log.info("getting split range => " + tableRelationHandle.toString());
        //String sql = "select index_name, table_owner, column_name, column_position from all_ind_columns where table_owner=? and table_name=?";
        var sql = "SELECT index_name, table_schema, column_name, seq_in_index FROM information_schema.statistics WHERE  table_schema =? and table_name=?";
        Optional<List<RangeInfo>> result = Optional.empty();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            var schemaName = tableRelationHandle.getSchemaTableName().getSchemaName().toUpperCase(ENGLISH);
            var tableName = tableRelationHandle.getSchemaTableName().getTableName().toUpperCase(ENGLISH);
            log.info("sql => " + sql + " -- schemaName " + schemaName + " tableName " + tableName);
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String indexName = rs.getString(1);
                if (!masterIndex.containsKey(indexName)) {
                    masterIndex.put(indexName, new ArrayList<>());
                }
                masterIndex.get(indexName).add(rs.getString(3));
            }
            String columnPart = null;
            for (Map.Entry<String, List<String>> entry : masterIndex.entrySet()) {
                if (entry.getValue().size() == 1) {
                    String theColumn = entry.getValue().get(0);
                    if (theColumn.contains(colOrIdx) || entry.getKey().contains(colOrIdx)) {
                        columnPart = theColumn;
                    }
                }
            }
            if (columnPart == null) {
                return result;
            }
            rs.close();
            log.info("getSplitRange: split on column " + columnPart);
            preparedStatement.close();
            String sql2 = "SELECT MIN(" + columnPart
                    + ") MINV, MAX(" + columnPart + ") MAXV FROM " + tableRelationHandle.getSchemaTableName().getSchemaName()
                    + "." + tableRelationHandle.getSchemaTableName().getTableName();
            preparedStatement = connection.prepareStatement(sql2);
            ResultSet rs2 = preparedStatement.executeQuery();
            log.info("getSplitRange: executed " + sql2);
            while (rs2.next()) {
                Object ob1 = rs2.getObject(1);
                if (ob1 == null) {
                    return Optional.empty();
                }
                ob1 = rs2.getObject(2);
                if (ob1 == null) {
                    return Optional.empty();
                }
                int minVal = rs2.getInt(1);
                int maxVal = rs2.getInt(2);
                int curPos = minVal;

                log.info("getSplitRange: minVal " + minVal + "maxVal " + maxVal);
                List<RangeInfo> result1 = new ArrayList<>();
                Optional<Integer> lowerBound = Optional.empty();
                Optional<Integer> upperBound = Optional.of(curPos + stride);
                result1.add(new RangeInfo(columnPart, lowerBound, upperBound));
                curPos += stride;
                while (curPos <= maxVal) {
                    lowerBound = upperBound; // = curPos
                    curPos += stride;
                    upperBound = Optional.of(curPos);
                    result1.add(new RangeInfo(columnPart, lowerBound, upperBound));
                }
                for (RangeInfo r : result1) {
                    log.info("getSplitRanges : " + r.toString());
                }
                result = Optional.of(result1);
            }
            rs2.close();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return result;
    }

    private static Optional<List<RangeInfo>> getRangeInfosNtileIndex(JdbcNamedRelationHandle tableRelationHandle,
            Connection connection, int parts, String colOrIdx)
    {
        Map<String, List<String>> masterIndex = new HashMap<>();
        Logger log = Logger.get(RangeInfo.class);
        log.info("getting split range => " + tableRelationHandle.toString());
        var sql = "SELECT index_name, table_schema, column_name, seq_in_index FROM information_schema.statistics WHERE  table_schema =? and table_name=?";
        Optional<List<RangeInfo>> result = Optional.empty();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            var schemaName = tableRelationHandle.getSchemaTableName().getSchemaName().toUpperCase(ENGLISH);
            var tableName = tableRelationHandle.getSchemaTableName().getTableName().toUpperCase(ENGLISH);
            log.info("sql => " + sql + " -- schemaName " + schemaName + " tableName " + tableName);
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                String indexName = rs.getString(1);
                if (!masterIndex.containsKey(indexName)) {
                    masterIndex.put(indexName, new ArrayList<>());
                }
                masterIndex.get(indexName).add(rs.getString(3));
            }
            String columnPart = null;
            for (Map.Entry<String, List<String>> entry : masterIndex.entrySet()) {
                if (entry.getValue().size() == 1) {
                    String theColumn = entry.getValue().get(0);
                    if (theColumn.contains(colOrIdx) || entry.getKey().contains(colOrIdx)) {
                        columnPart = theColumn;
                    }
                }
            }
            if (columnPart == null) {
                return Optional.empty();
            }
            rs.close();
            log.info("getSplitRange: split on column " + columnPart);
            preparedStatement.close();
            //select min (vkont) minvkont, max(vkont) maxvkont, bucketno from (select vkont,ntile(100) over (order by vkont) bucketno from sapsr3.fkkvkp) a group by bucketno;
            String sql2 = "SELECT MIN(" + columnPart
                    + ") MINV, MAX(" + columnPart + ") MAXV, BUCKETNO FROM (SELECT "
                    + columnPart + ", NTILE(" + parts + ") over (ORDER BY " + columnPart
                    + ") BUCKETNO FROM " + tableRelationHandle.getSchemaTableName().getSchemaName()
                    + "." + tableRelationHandle.getSchemaTableName().getTableName()
                    + ") A GROUP BY BUCKETNO";
            preparedStatement = connection.prepareStatement(sql2);
            ResultSet rs2 = preparedStatement.executeQuery();
            log.info("getSplitRange: executed " + sql2);
            List<RangeInfo> result1 = new ArrayList<>();
            while (rs2.next()) {
                Object ob1 = rs2.getObject(1);
                if (ob1 == null) {
                    return Optional.empty();
                }
                Object ob2 = rs2.getObject(2);
                if (ob2 == null) {
                    return Optional.empty();
                }
                log.info("getSplitRange: minVal " + ob1 + "maxVal " + ob2);
                result1.add(new RangeInfo(columnPart, Optional.of(ob1), Optional.of(ob2))
                        .withUpperInclusive(true));
            }
            rs2.close();
            result = Optional.of(result1);
            return result;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    static String convertRangeInfoIntoPredicate(RangeInfo rangeInfo)
    {
        StringBuilder sql = new StringBuilder();
        String expression = rangeInfo.getExpression();
        if (rangeInfo.getLowerBound().isPresent()) {
            if (rangeInfo.getLowerBound().get() instanceof Number) {
                sql.append(expression).append(" >= ").append(rangeInfo.getLowerBound().get());
            }
            else {
                sql.append(expression).append(" >= '").append(rangeInfo.getLowerBound().get()).append("'");
            }
        }

        if (rangeInfo.getUpperBound().isPresent()) {
            if (rangeInfo.getLowerBound().isPresent()) {
                sql.append(" AND ");
            }
            var upperOperator = " < ";
            if (rangeInfo.isUpperInclusive()) {
                upperOperator = " <= ";
            }
            if (rangeInfo.getUpperBound().get() instanceof Number) {
                sql.append(expression).append(upperOperator).append(rangeInfo.getUpperBound().get());
            }
            else {
                sql.append(expression).append(upperOperator).append("'").append(rangeInfo.getUpperBound().get()).append("'");
            }
        }

        return sql.toString();
    }
}
