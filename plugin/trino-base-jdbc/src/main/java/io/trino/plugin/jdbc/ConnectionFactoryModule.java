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
package io.trino.plugin.jdbc;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.keepalive.KeepAliveModule;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.joining;

public class ConnectionFactoryModule
        extends AbstractConfigurationAwareModule
{
    private final Logger log = Logger.get(ConnectionFactoryModule.class);

    @Override
    protected void setup(Binder binder)
    {
        install(new RetryingConnectionFactoryModule());
        install(new KeepAliveModule());
    }

    @Provides
    @Singleton
    protected ConnectionFactory provideConnectionFactory(@ForBaseJdbc ConnectionFactory delegate, Set<ConnectionFactoryDecorator> decorators)
    {
        List<ConnectionFactoryDecorator> sortedDecorators = decorators.stream()
                .filter(ConnectionFactoryDecorator::shouldBeApplied)
                .sorted(Comparator.comparing(ConnectionFactoryDecorator::getPriority)) // higher priority is installed last
                .collect(toImmutableList());

        String chainOfDecorators = sortedDecorators.stream()
                .map(ConnectionFactoryDecorator::getClass)
                .map(Class::getName)
                .collect(joining("\n\t -> "));

        log.info("Decorating %s with\n\t -> %s", delegate, chainOfDecorators);

        ConnectionFactory value = delegate;

        for (ConnectionFactoryDecorator decorator : sortedDecorators) {
            value = decorator.decorate(value);
        }

        return value;
    }
}
