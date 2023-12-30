namespace StressTest;

public class PointData
{
    public int AvlID { get; set; }
    public DateTime PointDate { get; set; }
    public DateTime InsertDate { get; set; }
    public long PacketID { get; set; }
    public decimal Longitude { get; set; }
    public decimal Latitude { get; set; }
    public short Altitude { get; set; }
    public short Angle { get; set; }
    public byte SatelliteCount { get; set; }
    public short Speed { get; set; }
    public short Reset { get; set; }
    public bool ChangeOff { get; set; }
    public bool HighSpeed { get; set; }
    public bool Memory1IsConnect { get; set; }
    public bool Memory2IsConnect { get; set; }
    public bool IsFromMemory { get; set; }
    public short InternalFlashPointCount { get; set; }
    public int OffTimes { get; set; }
    public short PauseTimes { get; set; }
    public string ExtraJson { get; set; }
    public bool Alarm { get; set; }
    public bool IsOn { get; set; }
    public bool ConnectToBattery { get; set; }
    public bool Sos { get; set; }
    public int SimCardCharge { get; set; }
    public int Counter { get; set; }
    public byte SignalQualityGSM { get; set; }
    public short MaxSpeed { get; set; }
    public int? DeviceVersion { get; set; }
}