namespace KafkaStreams.FirstTask.Models;

public record ConfigurationInfo(string ApplicationId, string BootstrapServers, ConsumerInfo From, ProducerInfo To);