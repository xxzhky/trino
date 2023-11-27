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

public interface ConnectionFactoryDecorator
{
    int SKIPPED_PRIORITY = -1;
    int REUSABLE_PRIORITY = 1;
    int RETRYING_PRIORITY = 2;
    int KEEP_ALIVE_PRIORITY = 3;

    // Highest priority is the outermost decorator
    int STATISTICS_PRIORITY = 100;

    int getPriority();

    default boolean shouldBeApplied()
    {
        return getPriority() != SKIPPED_PRIORITY;
    }

    ConnectionFactory decorate(ConnectionFactory delegate);

    static ConnectionFactoryDecorator noop()
    {
        return new ConnectionFactoryDecorator()
        {
            @Override
            public int getPriority()
            {
                return SKIPPED_PRIORITY;
            }

            @Override
            public ConnectionFactory decorate(ConnectionFactory delegate)
            {
                return delegate;
            }
        };
    }
}
