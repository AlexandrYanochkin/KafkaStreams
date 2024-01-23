using KafkaStreams.FourthTask.Models;
using KafkaStreams.FourthTask.Services;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Mock;
using NUnit.Framework;
using Moq;

namespace KafkaStreams.FourthTask.Tests;

public class StreamTests
{
    [Test]
    public void Should_FilterValue_When_ValueNull()
    {
        // Arrange
        var mock = new Mock<Action<string, EmployeeInfo>>();
        var config = new StreamConfig<StringSerDes, Services.JsonSerDes<EmployeeInfo>>()
        {
            ApplicationId = "Should_FilterValue_When_ValueNull"
        };
        var inputTopicName = "my-test-topic";
        var topology = new TopologyBuilder()
            .WithResultHandler(mock.Object)
            .Build(inputTopicName);
        using var driver = new TopologyTestDriver(topology, config);
        var inputTopic = driver.CreateInputTopic<string, EmployeeInfo>(inputTopicName);

        // Act
        inputTopic.PipeInput("1", null);
        inputTopic.PipeInput("2", null);
        inputTopic.PipeInput("3", null);

        // Assert
        mock.Verify(d => d(It.IsAny<string>(), It.IsAny<EmployeeInfo>()), Times.Never);
    }

    [Test]
    public void Should_InvokeResultHandler()
    {
        // Arrange
        var mock = new Mock<Action<string, EmployeeInfo>>();
        var config = new StreamConfig<StringSerDes, Services.JsonSerDes<EmployeeInfo>>()
        {
            ApplicationId = "Should_InvokeResultHandler"
        };
        var inputTopicName = "my-test-topic";
        var topology = new TopologyBuilder()
            .WithResultHandler(mock.Object)
            .Build(inputTopicName);
        using var driver = new TopologyTestDriver(topology, config);
        var inputTopic = driver.CreateInputTopic<string, EmployeeInfo>(inputTopicName);

        // Act
        inputTopic.PipeInput("1", new EmployeeInfo());
        inputTopic.PipeInput("2", new EmployeeInfo() { Company = "test company " });
        inputTopic.PipeInput("3", new EmployeeInfo() { Experience = 1 });

        // Assert
        mock.Verify(d => d(It.IsAny<string>(), It.IsAny<EmployeeInfo>()), Times.Exactly(3));
    }
}
