using System.Data;
using System.Data.SqlClient;
using StressTest.Controllers;

namespace StressTest;

public class DbConnection
{
    private const string ConnectionString = "";

    private const string PointDataParameterName = "@Point_Memory_UDT";
    private const string SpName = "Point_Memory_Insert_TVP";
    private const string AvlLastPointParameterName = "@AVlLastPointInfo_UDT";
    private const string AvlLastPointSpName = "Avl_UpdateLastPointInfo_TVP";
    private const int CommandTimeOut = 25;

    public async Task ExecuteSpNew(List<PointData> pointDataList)
    {
        var connection = new SqlConnection(ConnectionString);
        var pointsDataParameter = CreatePointsDataParameter(pointDataList);
        var avlLastPointsDataParameter = CreateAvlLastPointsDataParameter(pointDataList);

        await connection.OpenAsync();
        var transaction = connection.BeginTransaction();
        var insertPointCommand = CreatePointsDataSpCommand(transaction, connection, pointsDataParameter);
        var avlLastPointsDataCommand = InitialAvlLastPointsDataSp(transaction, connection, avlLastPointsDataParameter);
        try
        {
            await insertPointCommand.ExecuteNonQueryAsync();
            await avlLastPointsDataCommand.ExecuteNonQueryAsync();

            await transaction.CommitAsync();
            await connection.CloseAsync();
        }
        catch (Exception)
        {
            await transaction.RollbackAsync();
            await connection.CloseAsync();

            throw;
        }
    }

    private SqlCommand CreatePointsDataSpCommand(
        SqlTransaction transaction,
        SqlConnection connection,
        params SqlParameter[] parameters)
    {
        var com = new SqlCommand(SpName)
        {
            CommandTimeout = CommandTimeOut,
            Transaction = transaction,
            Connection = connection,
            CommandType = CommandType.StoredProcedure
        };

        com.Parameters.AddRange(parameters);

        return com;
    }

    private SqlParameter CreatePointsDataParameter(List<PointData> pointDataList)
    {
        var pointDataTable = new DataTable();
        AddPointDataColumns(pointDataTable);
        PopulatePointData(pointDataList, pointDataTable);

        return new SqlParameter(PointDataParameterName, SqlDbType.Structured)
        {
            Value = pointDataTable
        };
    }

    private void AddPointDataColumns(DataTable pointDataTable)
    {
        pointDataTable.Columns.AddRange(new[]
        {
            new DataColumn("AvlID", typeof(int)),
            new DataColumn("PointDate", typeof(DateTime)),
            new DataColumn("InsertDate", typeof(DateTime)),
            new DataColumn("PacketID", typeof(long)),
            new DataColumn("Longitude", typeof(decimal)),
            new DataColumn("Latitude", typeof(decimal)),
            new DataColumn("Altitude", typeof(short)),
            new DataColumn("Angle", typeof(short)),
            new DataColumn("SatelliteCount", typeof(byte)),
            new DataColumn("Speed", typeof(short)),
            new DataColumn("Reset", typeof(short)),
            new DataColumn("ChangeOff", typeof(bool)),
            new DataColumn("HighSpeed", typeof(bool)),
            new DataColumn("Memory1IsConnect", typeof(bool)),
            new DataColumn("Memory2IsConnect", typeof(bool)),
            new DataColumn("IsFromMemory", typeof(bool)),
            new DataColumn("InternalFlashPointCount", typeof(short)),
            new DataColumn("OffTimes", typeof(int)),
            new DataColumn("PauseTimes", typeof(short)),
            new DataColumn("ExtraJson", typeof(string)),
            new DataColumn("Alarm", typeof(bool)),
            new DataColumn("IsOn", typeof(bool)),
            new DataColumn("ConnectToBattery", typeof(bool)),
            new DataColumn("Sos", typeof(bool)),
            new DataColumn("SimCardCharge", typeof(int)),
            new DataColumn("Counter", typeof(int)),
            new DataColumn("SignalQualityGSM", typeof(byte)),
            new DataColumn("MaxSpeed", typeof(short))
        });
    }

    private void PopulatePointData(List<PointData> pointDataList, DataTable pointDataTable)
    {
        foreach (var pointData in pointDataList)
            pointDataTable.Rows.Add(
                pointData.AvlID,
                pointData.PointDate,
                pointData.InsertDate,
                pointData.PacketID,
                pointData.Longitude,
                pointData.Latitude,
                pointData.Altitude,
                pointData.Angle,
                pointData.SatelliteCount,
                pointData.Speed,
                pointData.Reset,
                pointData.ChangeOff,
                pointData.HighSpeed,
                pointData.Memory1IsConnect,
                pointData.Memory2IsConnect,
                pointData.IsFromMemory,
                pointData.InternalFlashPointCount,
                pointData.OffTimes,
                pointData.PauseTimes,
                pointData.ExtraJson,
                pointData.Alarm,
                pointData.IsOn,
                pointData.ConnectToBattery,
                pointData.Sos,
                pointData.SimCardCharge,
                pointData.Counter,
                pointData.SignalQualityGSM,
                pointData.MaxSpeed
            );
    }

    private SqlCommand InitialAvlLastPointsDataSp(
        SqlTransaction transaction,
        SqlConnection connection,
        params SqlParameter[] parameters)
    {
        var com = new SqlCommand(AvlLastPointSpName)
        {
            CommandTimeout = CommandTimeOut,
            Transaction = transaction,
            Connection = connection,
            CommandType = CommandType.StoredProcedure
        };

        com.Parameters.AddRange(parameters);

        return com;
    }

    private SqlParameter CreateAvlLastPointsDataParameter(List<PointData> pointDataList)
    {
        var avlLastPoints = pointDataList
            .GroupBy(pointData => pointData.AvlID)
            .Select(grp => new AvlLastPoint
            {
                AvlId = grp.Key,
                PointDate = grp.Max(pointData => pointData.PointDate),
                DeviceVersion = grp.Max(pointData => pointData.DeviceVersion)
            }).ToList();

        var pointDataTable = new DataTable();
        AddLastAvlPointDataColumns(pointDataTable);
        PopulateAvlLastPointData(avlLastPoints, pointDataTable);

        return new SqlParameter(AvlLastPointParameterName, SqlDbType.Structured)
        {
            Value = pointDataTable
        };
    }

    private void AddLastAvlPointDataColumns(DataTable avlLastPointDataTable)
    {
        avlLastPointDataTable.Columns.AddRange(new[]
        {
            new DataColumn("AvlID", typeof(int)),
            new DataColumn("PointDate", typeof(DateTime)),
            new DataColumn("DeviceVersion", typeof(int))
        });
    }

    private void PopulateAvlLastPointData(List<AvlLastPoint> avlLastPoints, DataTable avlLastPointDataTable)
    {
        foreach (var pointData in avlLastPoints)
            avlLastPointDataTable.Rows.Add(
                pointData.AvlId,
                pointData.PointDate,
                pointData.DeviceVersion
            );
    }
}