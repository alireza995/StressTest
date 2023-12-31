using System.Data;
using System.Reflection;

namespace StressTest;

public static class Utility
{
    public static DataTable CopyToDataTable<T>(this IEnumerable<T> data)
    {
        var dt = new DataTable();

        var properties = typeof(T).GetProperties();
        AddColumns(dt, properties);
        AddRows(dt, properties, data);

        return dt;
    }

    private static void AddColumns(DataTable destinationDt, IEnumerable<PropertyInfo> properties)
    {
        foreach (var property in properties)
            destinationDt.Columns.Add(property.Name,
                Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
    }

    private static void AddRows<T>(
        DataTable destinationDt,
        PropertyInfo[] properties,
        IEnumerable<T> data
    )
    {
        foreach (var row in data) AddRow(destinationDt, properties, row);
    }

    private static void AddRow<T>(
        DataTable destinationDt,
        IEnumerable<PropertyInfo> properties,
        T data
    )
    {
        var row = destinationDt.NewRow();
        foreach (var property in properties)
            try
            {
                row[property.Name] = property.GetValue(data) ?? DBNull.Value;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        destinationDt.Rows.Add(row);
    }
}