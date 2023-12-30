namespace StressTest.Controllers;

public class PointRequest
{
    public int UniqueCode { get; set; }
    public List<PointData> PointData { get; set; } = null!;
}