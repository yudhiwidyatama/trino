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
