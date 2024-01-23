using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaStreams.FirstTask.Models;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using Newtonsoft.Json;

namespace KafkaStreams.FirstTask.Helpers;

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
        await CreateAsync(configuration.To.TopicName);

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

    public static void SubscribeToProducerTopic(ConfigurationInfo configuration, Action<string> logger)
    {
        var config = new ConsumerConfig
        {
            GroupId = $"{configuration.ApplicationId}-result-group-id",
            BootstrapServers = configuration.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        var consumer = new ConsumerBuilder<Null, string>(config).Build();

        consumer.Subscribe(configuration.To.TopicName);

        Task.Run(() =>
        {
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume();

                    try
                    {
                        var message = JsonConvert.DeserializeObject<MessageInfo>(consumeResult.Message.Value);

                        logger($"[Result Consumer] Message processed successfully - {message}");
                    }
                    catch
                    {
                        logger($"[Result Consumer]Invalid format - {consumeResult.Message.Value}");
                    }
                }
                catch (Exception ex)
                {
                    logger($"[Result Consumer] Error: {ex.Message}");
                }
            }
        });
    }

    public static async Task StartStreamAsync(ConfigurationInfo configuration)
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = configuration.ApplicationId,
            BootstrapServers = configuration.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            NumStreamThreads = 3
        };

        var builder = new StreamBuilder();

        builder.Stream<string, string>(configuration.From.TopicName)
            .MapValues(Map)
            .To(configuration.To.TopicName);

        using var stream = new KafkaStream(builder.Build(), config);

        await stream.StartAsync();

        var start = 1;

        while (start < 500)
        {
            start = await GenerateConsumerTopicMessagesAsync(configuration, start, start + 15);

            await Task.Delay(5_000);
        }

        await Task.Delay(100_000);

        string Map(string json)
        {
            var message = JsonConvert.DeserializeObject<MessageInfo>(json);

            Console.WriteLine($"[Stream] Mapping {json}");

            return JsonConvert.SerializeObject(new MessageInfo($"[Filtered] {message?.Text}"));
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
                Value = JsonConvert.SerializeObject(new MessageInfo($"auto generated message #{start}"))
            };

            await producer.ProduceAsync(configuration.From.TopicName, message);

            start++;
        }

        producer.Flush();

        return start;
    }
}
