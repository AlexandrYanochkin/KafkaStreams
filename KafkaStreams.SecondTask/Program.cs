using KafkaStreams.SecondTask.Models;

using static KafkaStreams.SecondTask.Helpers.KafkaHelper;

var configuration = new ConfigurationInfo(
    "task2-app-id",
    "localhost:9092",
    new ConsumerInfo("task2")
);

await CreateTopicsAsync(configuration);
await StartStreamAsync(configuration, Console.WriteLine);