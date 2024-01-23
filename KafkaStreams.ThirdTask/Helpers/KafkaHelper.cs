using KafkaStreams.ThirdTask.Models;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net;
using Confluent.Kafka.Admin;
using Confluent.Kafka;

namespace KafkaStreams.ThirdTask.Helpers;

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

        foreach (var topicName in configuration.From.TopicNames)
        {
            await CreateAsync(topicName);
        }

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

        var builder = new StreamBuilder();

        var innerStreams = configuration.From.TopicNames
            .Select(topicName => builder
                    .Stream<string, string>(topicName)
                    .Filter(Filter)
                    .Map(CreateKey)
                    .Peek(Print));

        var innerStream = innerStreams.First();
        var valueSerDes = new StringSerDes();
        var keySerDes = new Int64SerDes();

        /*
         * "In the JOIN add 30 seconds to join records which can be out of order" cannot be done as the library doesn't support
         * "out of order" joins
         */
        foreach (var nextInnerStream in innerStreams.Skip(1))
        {
            innerStream = innerStream.Join(nextInnerStream, Join, JoinWindowOptions.Of(TimeSpan.FromMinutes(1)), StreamJoinProps.With(keySerDes, valueSerDes, valueSerDes));
        }

        innerStream.Foreach(PrintJoin);

        using var stream = new KafkaStream(builder.Build(), config);

        await stream.StartAsync();

        var start = 1;

        while (start < 50)
        {
            start = await GenerateConsumerTopicMessagesAsync(configuration, start, start + 15);

            await Task.Delay(10_000);
        }

        await Task.Delay(100_000);

        bool Filter(string key, string value)
        {
            return value is not null && value.Contains(':');
        }

        KeyValuePair<long, string> CreateKey(string key, string value)
        {
            var words = value.Split(':', count: 2);

            return KeyValuePair.Create(long.Parse(words.First()), words.Last());
        }

        void Print(long key, string value)
        {
            logger($"Key: {key} - Value: {value}");
        }

        string Join(string left, string right)
        {
            return $"{left} - {right}";
        }

        void PrintJoin(long key, string value)
        {
            logger($"Key: {key} - Value: {value}");
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
            foreach (var topicName in configuration.From.TopicNames)
            {
                var message = new Message<Null, string>
                {
                    Value = $"{start}:{topicName}'s value"
                };

                await producer.ProduceAsync(topicName, message);
            }

            start++;
        }

        producer.Flush();

        return start;
    }
}
