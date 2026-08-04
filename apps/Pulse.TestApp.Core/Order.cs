using System.Text.Json.Serialization;
using Pulse.Abstractions;

namespace Pulse.TestApp.Core;

/// <summary>
/// The single entity the test app exercises, identical across all three providers. The
/// primary key is surfaced by Pulse as <c>_id</c> on the wire (Mongo ObjectId / Postgres
/// uuid / SQL Server uniqueidentifier), so <see cref="Id"/> maps explicitly from <c>_id</c>.
/// </summary>
public sealed class Order
{
    [JsonPropertyName("_id")]
    public string Id { get; set; } = "";

    public string CustomerName { get; set; } = "";

    public string Status { get; set; } = "";

    public decimal Total { get; set; }

    public int Items { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }

    public string Region { get; set; } = "";
}

/// <summary>The finite sets the UI dropdowns expose, so filters combine on stable values.</summary>
public static class OrderState
{
    public static readonly string[] Statuses = { "pending", "processing", "shipped", "delivered", "cancelled" };
    public static readonly string[] Regions = { "NA", "EU", "APAC" };

    /// <summary>Builds the list-screen filter: <c>Status = s AND Region = r</c>.</summary>
    public static FilterExpr? ListFilter(string? status, string? region)
    {
        var clauses = new List<FilterExpr>();
        if (status is not null)
        {
            clauses.Add(new FieldCompare("Status", CompareOp.Eq, status));
        }

        if (region is not null)
        {
            clauses.Add(new FieldCompare("Region", CompareOp.Eq, region));
        }

        return clauses.Count == 0 ? null : clauses.Count == 1 ? clauses[0] : new And(clauses);
    }

    /// <summary>Builds the detail-screen filter: a single document by id.</summary>
    public static FilterExpr DetailFilter(string id)
        => new FieldCompare("_id", CompareOp.Eq, id);
}