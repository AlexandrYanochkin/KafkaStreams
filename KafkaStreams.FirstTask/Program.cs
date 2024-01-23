using KafkaStreams.FirstTask.Models;

using static KafkaStreams.FirstTask.Helpers.KafkaHelper;

var configuration = new ConfigurationInfo(
    "task1-app-id",
    "localhost:9092",
    new ConsumerInfo("task1-1"),
    new ProducerInfo("task1-2")
);

await CreateTopicsAsync(configuration);

SubscribeToProducerTopic(configuration, Console.WriteLine);

await StartStreamAsync(configuration);

//"127.0.0.1:29092,127.0.0.1:39092,127.0.0.1:49092",