using KafkaStreams.SecondTask.Models;
using KafkaStreams.SecondTask.Services;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using Confluent.Kafka.Admin;
using Confluent.Kafka;

namespace KafkaStreams.SecondTask.Helpers;

public static class KafkaHelper
{
    public static async Task CreateTopicsAsync(ConfigurationInfo configuration)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = configuration.BootstrapServers
        };

        using var client = new AdminClientBuilder(config).Build();

        var meta = client.GetMetadata(TimeSpan.FromSeconds(20));

        await CreateAsync(configuration.From.TopicName);

        async Task CreateAsync(string topicName)
        {
            if (meta.Topics.Any(meta => meta.Topic.Equals(topicName)))
            {
                return;
            }

            await client.CreateTopicsAsync([new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 3,
                ReplicationFactor = 1
            }]);
        }
    }

    public static async Task StartStreamAsync(ConfigurationInfo configuration, Action<string> logger)
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = configuration.ApplicationId,
            BootstrapServers = configuration.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            NumStreamThreads = 3
        };

        var topology = new TopologyBuilder()
            .WithIntermediateResultHandler(PrintWord)
            .WithResultHandler(PrintFinalWord)
            .Build(configuration.From.TopicName);

        using var stream = new KafkaStream(topology, config);

        await stream.StartAsync();

        var start = 1;

        while (start < 500)
        {
            start = await GenerateConsumerTopicMessagesAsync(configuration, start, start + 15);

            await Task.Delay(5_000);
        }

        await Task.Delay(100_000);

        void PrintWord(int key, string value)
        {
            logger($"Word - Length {key} - value {value}");
        }

        void PrintFinalWord(int key, string value)
        {
            if (key < 10)
            {
                logger($"Short Word - Length {key} - value {value}");
            }
            else
            {
                logger($"Long Word - Length {key} - value {value}");
            }
        }
    }

    private static async Task<int> GenerateConsumerTopicMessagesAsync(ConfigurationInfo configuration, int start, int end)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = configuration.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = int.MaxValue,
            MessageTimeoutMs = 10_000
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        while (start <= end)
        {
            var message = new Message<Null, string>
            {
                Value = $"My a custom longmessagelong {start}"
            };

            await producer.ProduceAsync(configuration.From.TopicName, message);

            start++;
        }

        producer.Flush();

        return start;
    }
}
