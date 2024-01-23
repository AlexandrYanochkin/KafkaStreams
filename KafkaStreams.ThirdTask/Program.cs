using KafkaStreams.ThirdTask.Models;

using static KafkaStreams.ThirdTask.Helpers.KafkaHelper;

var configuration = new ConfigurationInfo(
    "task4-app-id",
    "localhost:9092",
    new ConsumerInfo(["task3-1", "task3-2"])
);

await CreateTopicsAsync(configuration);
await StartStreamAsync(configuration, Console.WriteLine);