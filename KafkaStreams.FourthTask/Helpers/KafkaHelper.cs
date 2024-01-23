using KafkaStreams.FourthTask.Models;
using KafkaStreams.FourthTask.Services;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;

namespace KafkaStreams.FourthTask.Helpers;

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
        var config = new StreamConfig<StringSerDes, Streamiz.Kafka.Net.SerDes.JsonSerDes<EmployeeInfo>>
        {
            ApplicationId = configuration.ApplicationId,
            BootstrapServers = configuration.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            NumStreamThreads = 3
        };

        var topology = new TopologyBuilder()
            .WithResultHandler(Print)
            .Build(configuration.From.TopicName);

        using var stream = new KafkaStream(topology, config);

        await stream.StartAsync();

        var start = 1;

        while (start < 50)
        {
            start = await GenerateConsumerTopicMessagesAsync(configuration, start, start + 15);

            await Task.Delay(10_000);
        }

        await Task.Delay(100_000);

        void Print(string key, EmployeeInfo value)
        {
            logger($"Employee:\t{value.Name};{value.Company};{value.Position};{value.Experience}");
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
                Value = JsonConvert.SerializeObject(new EmployeeInfo
                {
                    Company = $"MyCompany{start}",
                    Name = $"Mr. {start}",
                    Position = $"My position #{start}",
                    Experience = (short)start
                })
            };

            await producer.ProduceAsync(configuration.From.TopicName, message);

            start++;
        }

        producer.Flush();

        return start;
    }
}
