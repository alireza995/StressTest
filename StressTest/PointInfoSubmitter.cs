using System.Data;
using System.Data.SqlClient;
using StressTest.Controllers;

namespace StressTest;

public class PointInfoSubmitter
{
    private readonly DbConnect _dbConnect;

    public PointInfoSubmitter(DbConnect dbConnect)
    {
        _dbConnect = dbConnect;
    }

    private const string PointDataParameterName = "@Point_Memory_UDT";
    private const string SpName = "Point_Memory_Insert_TVP";
    private const string AvlLastPointParameterName = "@AVlLastPointInfo_UDT";
    private const string AvlLastPointSpName = "Avl_UpdateLastPointInfo_TVP";
    private const int CommandTimeOut = 25;

    public async Task ExecuteSpNew(List<PointData> pointDataList)
    {
        await _dbConnect.ExecuteSp(
            CreateInsertPointsCommand(pointDataList),
            CreateCommandAvlLastPoints(pointDataList)
        );
    }

    private SqlCommand CreateInsertPointsCommand(List<PointData> pointDataList)
    {
        var command = new SqlCommand
        {
            CommandText = SpName,
            CommandTimeout = CommandTimeOut,
            CommandType = CommandType.StoredProcedure
        };

        command.Parameters.AddWithValue(PointDataParameterName, pointDataList.CopyToDataTable());

        return command;
    }

    private SqlCommand CreateCommandAvlLastPoints(List<PointData> pointDataList)
    {
        var avlLastPoints = pointDataList
            .GroupBy(pointData => pointData.AvlID)
            .Select(grp => new AvlLastPoint
            {
                AvlId = grp.Key,
                PointDate = grp.Max(pointData => pointData.PointDate),
                DeviceVersion = grp.Max(pointData => pointData.DeviceVersion)
            }).CopyToDataTable();

        var command = new SqlCommand
        {
            CommandText = AvlLastPointSpName,
            CommandTimeout = CommandTimeOut,
            CommandType = CommandType.StoredProcedure
        };

        command.Parameters.AddWithValue(AvlLastPointParameterName, avlLastPoints);

        return command;
    }
}