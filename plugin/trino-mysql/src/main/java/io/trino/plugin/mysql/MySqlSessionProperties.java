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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public class MySqlSessionProperties
        implements SessionPropertiesProvider
{
    private final List<PropertyMetadata<?>> sessionProperties;

    public static Boolean getExperimentalSplit(ConnectorSession session)
    {
        return session.getProperty("experimental_split", Boolean.class);
    }

    public static String getSplitRule(ConnectorSession session)
    {
        return session.getProperty("split_rule", String.class);
    }

    @Inject
    public MySqlSessionProperties(MySqlConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        "experimental_split",
                        "Experimental split",
                        config.getExperimentalSplit(), false))
                .add(stringProperty(
                        "split_rule",
                        "Splitting rule",
                        config.getSplitRule(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
