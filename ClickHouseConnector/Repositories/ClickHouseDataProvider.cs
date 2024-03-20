using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using ClickHouseConnector.Interfaces;
using ClickHouseConnector.Models;
using System.ComponentModel.Design;
using System.Data;

namespace ClickHouseConnector.Repositories
{
    public class ClickHouseDataProvider : IClickHouseDataProvider
    {
        private readonly ClickHouseConnection _connection;

        public ClickHouseDataProvider(ClickHouseConnection connection)
        {
            _connection = connection;
        }

        public async Task<DataTable> FetchDataAsync(string queryString, 
            IEnumerable<QueryParameter> parameters = null)
        {
            DataTable dt = new DataTable();

            using (var command = new ClickHouseCommand(_connection))
            {
                try
                {
                    command.CommandText = queryString;
                    command.CommandTimeout = 900;

                    if (parameters != null)
                    {
                        foreach (var parameter in parameters)
                        {
                            var param = command.CreateParameter();
                            param.ParameterName = parameter.Name;
                            param.Value = parameter.Value;
                            param.DbType = parameter.DataType;
                            command.Parameters.Add(param);
                        }
                    }

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            // Add columns to DataTable based on schema information
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                string columnName = reader.GetName(i);
                                Type dataType = reader.GetFieldType(i);

                                // Add column to DataTable with the same name and data type
                                dt.Columns.Add(columnName, dataType);
                            }

                            // Read data from reader and populate DataTable
                            while (await reader.ReadAsync())
                            {
                                DataRow row = dt.NewRow();

                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    // Populate each column in the DataRow
                                    row[i] = reader.GetValue(i);
                                }

                                // Add populated DataRow to DataTable
                                dt.Rows.Add(row);
                            }
                        }
                    }   
                }
                catch (Exception ex)
                {
                    throw;
                }
                finally
                {                    
                    command.Parameters.Clear();
                }
                return dt;
            }
        }
    }
}
