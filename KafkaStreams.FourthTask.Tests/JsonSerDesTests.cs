using System.Text;
using KafkaStreams.FourthTask.Models;
using KafkaStreams.FourthTask.Services;
using Confluent.Kafka;
using NUnit.Framework;
using Newtonsoft.Json;

namespace KafkaStreams.FourthTask.Tests;

public class JsonSerDesTests
{
    [Test]
    public void Should_SerifalizeObject()
    {
        // Arrange
        var serDes = new JsonSerDes<EmployeeInfo>();
        var obj = new EmployeeInfo() { Company = "company", Name = "name" };

        // Act
        var result = serDes.Serialize(obj, SerializationContext.Empty);

        // Assert
        Assert.That(result, Is.EqualTo(JsonConvert.SerializeObject(obj)));
    }

    [Test]
    public void Should_DeserifalizeObject()
    {
        // Arrange
        var serDes = new JsonSerDes<EmployeeInfo>();
        var obj = new EmployeeInfo() { Company = "company", Name = "name" };
        var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj));

        // Act
        var result = serDes.Deserialize(bytes, SerializationContext.Empty);

        // Assert
        Assert.That(result, Is.EqualTo(obj));
    }

    [Test]
    public void Deserifalize_Should_ReturnNull_When_DataArrayNull()
    {
        // Arrange
        var serDes = new JsonSerDes<EmployeeInfo>();

        // Act
        var result = serDes.Deserialize(null, SerializationContext.Empty);

        // Assert
        Assert.That(result, Is.Null);
    }

    [Test]
    public void Deserifalize_Should_ReturnNull_When_DataArrayEmpty()
    {
        // Arrange
        var serDes = new JsonSerDes<EmployeeInfo>();

        // Act
        var result = serDes.Deserialize(Array.Empty<byte>(), SerializationContext.Empty);

        // Assert
        Assert.That(result, Is.Null);
    }

    [Test]
    public void Serifalize_Should_ReturnNull_When_DataArrayNull()
    {
        // Arrange
        var serDes = new JsonSerDes<EmployeeInfo>();

        // Act
        var result = serDes.Serialize(null, SerializationContext.Empty);

        // Assert
        Assert.That(result, Is.Null);
    }
}
