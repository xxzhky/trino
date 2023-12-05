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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.kafka.schema.ContentSchemaProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final KafkaConsumerFactory consumerFactory;
    private final KafkaFilterManager kafkaFilterManager;
    private final ContentSchemaProvider contentSchemaProvider;
    private final int messagesPerSplit;

    @Inject
    public KafkaSplitManager(KafkaConsumerFactory consumerFactory, KafkaConfig kafkaConfig, KafkaFilterManager kafkaFilterManager, ContentSchemaProvider contentSchemaProvider)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.messagesPerSplit = kafkaConfig.getMessagesPerSplit();
        this.kafkaFilterManager = requireNonNull(kafkaFilterManager, "kafkaFilterManager is null");
        this.contentSchemaProvider = requireNonNull(contentSchemaProvider, "contentSchemaProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) table;
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create(session)) {
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(kafkaTableHandle.getTopicName());

            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(KafkaSplitManager::toTopicPartition)
                    .collect(toImmutableList());

            Map<TopicPartition, Long> partitionBeginOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> partitionEndOffsets = kafkaConsumer.endOffsets(topicPartitions);
            KafkaFilteringResult kafkaFilteringResult = kafkaFilterManager.getKafkaFilterResult(session, kafkaTableHandle,
                    partitionInfos, partitionBeginOffsets, partitionEndOffsets);
            partitionInfos = kafkaFilteringResult.getPartitionInfos();
            partitionBeginOffsets = kafkaFilteringResult.getPartitionBeginOffsets();
            partitionEndOffsets = kafkaFilteringResult.getPartitionEndOffsets();

            int totalRows = 0;

            ImmutableList.Builder<KafkaSplit> splits = ImmutableList.builder();
            Optional<String> keyDataSchemaContents = contentSchemaProvider.getKey(kafkaTableHandle);
            Optional<String> messageDataSchemaContents = contentSchemaProvider.getMessage(kafkaTableHandle);

            Map<TopicPartition, Long> finalPartitionBeginOffsets = partitionBeginOffsets;
            Map<TopicPartition, Long> finalPartitionEndOffsets = partitionEndOffsets;
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = toTopicPartition(partitionInfo);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());

                class RangeToRepartition {
                    private int partitionSize;
                    private OptionalLong limit;

                    {
                        this.partitionSize = messagesPerSplit;
                        this.limit = kafkaTableHandle.getLimit().isPresent() ? kafkaTableHandle.getLimit() : OptionalLong.empty();
                    }

                    public RangeToRepartition() {
                    }

                    public RangeToRepartition(int partitionSize)
                    {
                        this.partitionSize = partitionSize;
                    }

                    private List<Range> getParts()
                    {
                        return new Range(finalPartitionBeginOffsets.get(topicPartition), finalPartitionEndOffsets.get(topicPartition)).partition(partitionSize);
                    }

                    public long size()
                    {
                        return getParts().size();
                    }

                    private Stream<Range> rangeStream()
                    {
                        return getParts().stream();
                    }

                    public Stream<KafkaSplit> stream()
                    {
                        return rangeStream().map(range ->
                           new KafkaSplit(
                                   kafkaTableHandle.getTopicName(),
                                   kafkaTableHandle.getKeyDataFormat(),
                                   kafkaTableHandle.getMessageDataFormat(),
                                   keyDataSchemaContents,
                                   messageDataSchemaContents,
                                   partitionInfo.partition(),
                                   range,
                                   leader,
                                   limit));
                    }

                    public int getPartitionSize() {
                        return partitionSize;
                    }

                    public RangeToRepartition setPartitionSize(int partitionSize) {
                        this.partitionSize = partitionSize;
                        return this;
                    }
                }

                RangeToRepartition repartition = new RangeToRepartition();
                if (kafkaTableHandle.getLimit().isPresent()) {
                    long limit = kafkaTableHandle.getLimit().getAsLong();
                    if (limit > messagesPerSplit && messagesPerSplit > 0) {
                        // It's very true for: round > 0
                        long round = floorDiv(limit, messagesPerSplit);
                        int remainder = (int) (limit % (long) messagesPerSplit);
                        // probably: size == 0
                        long size = repartition.size();
                        if (size == 0) {
                            continue;
                        }
                        final Stream<KafkaSplit> splitStream = repartition.stream();

                        long rowsInRound = splitStream.limit(round)
                                .map(KafkaSplit::getMessagesRange)
                                .mapToLong(r -> r.getEnd() - r.getBegin())
                                .sum();
                        long rowsInReminder = splitStream.skip(round).limit(1L)
                                .map(split ->
                                    split.getSplitByRange(split.getMessagesRange()
                                            .slice(split.getMessagesRange().getBegin(), remainder)))
                                .findFirst()
                                .map(KafkaSplit::getMessagesRange)
                                .map(r -> r.getEnd() - r.getBegin())
                                .orElse(0L);

                        long rows = rowsInRound + rowsInReminder;
                        totalRows += rows;
                        if (totalRows > limit) {
                            rows -= totalRows - limit;
                            // in fact, the finalRows exists
                            long finalRows = rows;
                            if (finalRows > 0) {
                                long round2 = floorDiv(finalRows, messagesPerSplit);
                                int remainder2 = (int) (finalRows % (long) messagesPerSplit);
                                splitStream.limit(round2).forEach(splits::add);
                                if (remainder2 > 0) {
                                    splitStream.skip(round2).limit(1L)
                                            .map(split ->
                                                    split.getSplitByRange(split.getMessagesRange()
                                                            .slice(split.getMessagesRange().getBegin(), remainder2)))
                                            .peek(splits::add);
                                }
                            }
                            break;
                        }
                        // we can fetch all the splits if the constraint is size > limit
                        splitStream.limit(round).forEach(splits::add);
                        // the limit-bound is more than the partitionSize
                        if (remainder > 0 && size >= round) {
                            splitStream.skip(round).limit(1L)
                                    .map(split ->
                                            split.getSplitByRange(split.getMessagesRange()
                                                    .slice(split.getMessagesRange().getBegin(), remainder)))
                                    .findFirst()
                                    .ifPresent(splits::add);
                        }
                    }
                    else {
                        final Optional<KafkaSplit> first = repartition.setPartitionSize((int) limit)
                                .stream()
                                .findFirst();
                        if (first.isEmpty()) {
                            continue;
                        }

                        long rows = first.map(KafkaSplit::getMessagesRange)
                                .map(r -> r.getEnd() - r.getBegin())
                                .orElse(0L);
                        totalRows += rows;
                        if (totalRows > limit) {
                            rows -= totalRows - limit;
                            long finalRows = rows;
                            if (finalRows > 0) {
                                first.map(split ->
                                        // evidently, here the finalRows must be no more than rows
                                        // since we can fetch what we need
                                                split.getSplitByRange(split.getMessagesRange()
                                                        .slice(split.getMessagesRange().getBegin(), toIntExact(finalRows))))
                                        .ifPresent(splits::add);
                            }
                            break;
                        }
                        // there should be no more rows to be shown if the stuff of variable FIRST is not enough
                        first.ifPresent(splits::add);
                    }
                }
                else {
                    repartition.stream().forEach(splits::add);
                }
            }
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
    }

    private static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }
}
