using KafkaStreams.SecondTask.Services;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net;
using NUnit.Framework;
using Moq;

namespace KafkaStreams.SecondTask.Tests;

public class StreamTests
{
    [Test]
    public void Should_FilterValue_When_ValueNull()
    {
        // Arrange
        var mock = new Mock<Action<int, string>>();
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = "Should_FilterValue_When_ValueNull"
        };
        var inputTopicName = "my-test-topic";
        var topology = new TopologyBuilder()
            .WithIntermediateResultHandler(mock.Object)
            .Build(inputTopicName);
        using var driver = new TopologyTestDriver(topology, config);
        var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);

        // Act
        inputTopic.PipeInput("1", string.Empty);
        inputTopic.PipeInput("2", string.Empty);

        // Assert
        mock.Verify(d => d(It.IsAny<int>(), It.IsAny<string>()), Times.Never);
    }

    [Test]
    public void Should_SplitSentenceIntoWords()
    {
        // Arrange
        var mock = new Mock<Action<int, string>>();
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = "Should_SplitSentenceIntoWords"
        };
        var inputTopicName = "my-test-topic";
        var topology = new TopologyBuilder()
            .WithIntermediateResultHandler(mock.Object)
            .Build(inputTopicName);
        using var driver = new TopologyTestDriver(topology, config);
        var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);

        // Act
        inputTopic.PipeInput("any key", "this is my test sentence");

        // Assert
        mock.Verify(d => d(4, "this"), Times.Once);
        mock.Verify(d => d(2, "is"), Times.Once);
        mock.Verify(d => d(2, "my"), Times.Once);
        mock.Verify(d => d(4, "test"), Times.Once);
        mock.Verify(d => d(8, "sentence"), Times.Once);
    }

    [Test]
    public void Should_Not_SplitSingleWord()
    {
        // Arrange
        var mock = new Mock<Action<int, string>>();
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = "Should_Not_SplitSingleWord"
        };
        var inputTopicName = "my-test-topic";
        var topology = new TopologyBuilder()
            .WithIntermediateResultHandler(mock.Object)
            .Build(inputTopicName);
        using var driver = new TopologyTestDriver(topology, config);
        var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);

        // Act
        inputTopic.PipeInput("any key", "sentence");

        // Assert
        mock.Verify(d => d(It.IsAny<int>(), It.IsAny<string>()), Times.Once);
        mock.Verify(d => d(8, "sentence"), Times.Once);
    }

    [Test]
    public void Should_FilterValue_When_ContainLetterA()
    {
        // Arrange
        var intermediateResultHandler = new Mock<Action<int, string>>();
        var resultHandlerMock = new Mock<Action<int, string>>();
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = "Should_FilterValue_When_ContainLetterA"
        };
        var inputTopicName = "my-test-topic";
        var topology = new TopologyBuilder()
            .WithIntermediateResultHandler(intermediateResultHandler.Object)
            .WithResultHandler(resultHandlerMock.Object)
            .Build(inputTopicName);
        using var driver = new TopologyTestDriver(topology, config);
        var inputTopic = driver.CreateInputTopic<string, string>(inputTopicName);

        // Act
        inputTopic.PipeInput("any key", "a sentence");

        // Assert
        intermediateResultHandler.Verify(d => d(It.IsAny<int>(), "a"), Times.Once);
        resultHandlerMock.Verify(d => d(It.IsAny<int>(), "a"), Times.Never);
    }
}
