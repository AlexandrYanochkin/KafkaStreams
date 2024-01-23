using KafkaStreams.FourthTask.Models;

using static KafkaStreams.FourthTask.Helpers.KafkaHelper;

var configuration = new ConfigurationInfo(
    "task3-app-id",
    "localhost:9092",
    new ConsumerInfo("task4")
);

await CreateTopicsAsync(configuration);
await StartStreamAsync(configuration, Console.WriteLine);