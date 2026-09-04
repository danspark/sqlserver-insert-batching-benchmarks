using SqlBench.Core;

namespace SqlBench.IntegrationTests;

public sealed class MessageSerializerTests
{
    [Fact]
    public void SourceGeneratedSerializerPreservesTheDeterministicPayload()
    {
        BenchmarkMessage expected = WorkloadGenerator.Generate(
            1,
            0x5EED_2026,
            ParentDistribution.ModerateSkew)[0];

        byte[] payload = MessageSerializer.Serialize(expected);
        BenchmarkMessage actual = MessageSerializer.Deserialize(payload);

        Assert.Equal(expected.MessageId, actual.MessageId);
        Assert.Equal(expected.ParentId, actual.ParentId);
        Assert.Equal(expected.CorrelationId, actual.CorrelationId);
        Assert.Equal(expected.OccurredAt, actual.OccurredAt);
        Assert.Equal(expected.SequenceNo, actual.SequenceNo);
        Assert.Equal(expected.CounterValue, actual.CounterValue);
        Assert.Equal(expected.Priority, actual.Priority);
        Assert.Equal(expected.Amount, actual.Amount);
        Assert.Equal(expected.IsActive, actual.IsActive);
        Assert.Equal(expected.Code, actual.Code);
        Assert.Equal(expected.Description, actual.Description);
        Assert.Equal(expected.PayloadHash, actual.PayloadHash);
        Assert.Equal(expected.OptionalNote, actual.OptionalNote);
        Assert.Equal(payload, MessageSerializer.Serialize(actual));
    }
}
