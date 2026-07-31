using System.Text.Json;
using Pulse.Abstractions;

namespace Pulse.Abstractions.Tests;

public class FilterExprRoundTripTests
{
    private static readonly JsonSerializerOptions Options = new();

    [Theory]
    [InlineData(CompareOp.Eq, "pending")]
    [InlineData(CompareOp.Ne, "shipped")]
    [InlineData(CompareOp.Gt, 100L)]
    [InlineData(CompareOp.Gte, 50L)]
    [InlineData(CompareOp.Lt, 200L)]
    [InlineData(CompareOp.Lte, 150L)]
    [InlineData(CompareOp.In, new[] { "a", "b", "c" })]
    [InlineData(CompareOp.NotIn, new[] { "x", "y" })]
    [InlineData(CompareOp.Exists, null)]
    public void AllCompareOps_RoundTripEqual(CompareOp op, object? value)
    {
        var original = new FieldCompare("status", op, value);
        var json = JsonSerializer.Serialize(original, Options);
        var roundTripped = JsonSerializer.Deserialize<FilterExpr>(json, Options);

        Assert.Equal(original, roundTripped);
    }

    [Fact]
    public void NestedAndOrNot_RoundTripEqual()
    {
        var original = new And(new FilterExpr[]
        {
            new FieldCompare("status", CompareOp.Eq, "pending"),
            new Or(new FilterExpr[]
            {
                new FieldCompare("total", CompareOp.Gte, 100L),
                new Not(new FieldCompare("vip", CompareOp.Eq, true)),
            }),
            new Not(new And(new FilterExpr[]
            {
                new FieldCompare("region", CompareOp.In, new List<object?> { "emea", "apac" }),
            })),
        });

        var json = JsonSerializer.Serialize(original, Options);
        var roundTripped = JsonSerializer.Deserialize<FilterExpr>(json, Options);

        Assert.Equal(original, roundTripped);
    }

    [Fact]
    public void FlatFieldCompare_RoundTripEqual_StringValue()
    {
        var original = new FieldCompare("customer.name", CompareOp.Eq, "Alice");
        var json = JsonSerializer.Serialize(original, Options);

        Assert.Equal(original, JsonSerializer.Deserialize<FilterExpr>(json, Options));
    }

    [Fact]
    public void IntegralJsonNumbers_NormalizeToLong()
    {
        var json = """{"field":"total","op":"gte","value":100}""";
        var filter = JsonSerializer.Deserialize<FilterExpr>(json, Options);

        var compare = Assert.IsType<FieldCompare>(filter);
        Assert.Equal(100L, compare.Value);
        Assert.IsType<long>(compare.Value);
    }

    [Fact]
    public void NonIntegralJsonNumbers_NormalizeToDouble()
    {
        var json = """{"field":"score","op":"gte","value":3.5}""";
        var filter = JsonSerializer.Deserialize<FilterExpr>(json, Options);

        var compare = Assert.IsType<FieldCompare>(filter);
        Assert.Equal(3.5, compare.Value);
        Assert.IsType<double>(compare.Value);
    }

    [Fact]
    public void NestedObjects_ValueNormalizeToDictionary()
    {
        var json = """{"field":"address.city","op":"eq","value":{"$ref":"x"}}""";
        var filter = JsonSerializer.Deserialize<FilterExpr>(json, Options);

        var compare = Assert.IsType<FieldCompare>(filter);
        Assert.Equal(new Dictionary<string, object?> { ["$ref"] = "x" }, compare.Value);
    }

    [Fact]
    public void UnknownNode_ThrowsJsonException()
    {
        var json = """{"foo":123}""";
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<FilterExpr>(json, Options));
    }

    [Fact]
    public void MissingOp_ThrowsJsonException()
    {
        var json = """{"field":"status"}""";
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<FilterExpr>(json, Options));
    }

    [Fact]
    public void UnknownOp_ThrowsJsonException()
    {
        var json = """{"field":"status","op":"bogus","value":1}""";
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<FilterExpr>(json, Options));
    }
}
