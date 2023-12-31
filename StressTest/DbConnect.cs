using System.Data;
using System.Data.SqlClient;

namespace StressTest;

public class DbConnect
{
    private readonly SqlConnection _connection;

    public DbConnect()
    {
        _connection = new SqlConnection(ConnectionString);
        _connection.StateChange += ConnectionOnStateChange;
        _connection.Open();
    }

    private void ConnectionOnStateChange(object sender, StateChangeEventArgs e)
    {
        ReOpenConnection(e.CurrentState).Wait();
    }

    private async Task ReOpenConnection(ConnectionState state)
    {
        if (state is ConnectionState.Broken or ConnectionState.Closed)
            await _connection.OpenAsync();
    }

    private const string ConnectionString =
        "Server=192.168.155.100;" +
        "Database=RadshidAvl;" +
        "User Id=Bagheri;" +
        "Password=zhjbg&^%*&kmv";

    public async Task ExecuteSp(params SqlCommand[] commands)
    {
        await ReOpenConnection(_connection.State);

        foreach (var command in commands) await command.ExecuteNonQueryAsync();
    }
}