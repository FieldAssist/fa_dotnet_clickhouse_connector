// Copyright (c) FieldAssist. All Rights Reserved.

using System.Data;
using System.Data.SqlClient;
using ClickHouse.Client.ADO;
using ClickHouseConnector.Interfaces;
using ClickHouseConnector.Models;

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
            var dt = new DataTable();

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
                            for (var i = 0; i < reader.FieldCount; i++)
                            {
                                var columnName = reader.GetName(i);
                                var dataType = reader.GetFieldType(i);

                                // Add column to DataTable with the same name and data type
                                dt.Columns.Add(columnName, dataType);
                            }

                            // Read data from reader and populate DataTable
                            while (await reader.ReadAsync())
                            {
                                var row = dt.NewRow();

                                for (var i = 0; i < reader.FieldCount; i++)
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

        public async Task<string> GetSingleResultOfQuery(string queryString, List<SqlParameter> parameters = null,
            int? commandTimeout = null)
        {
            using (var command = new ClickHouseCommand(_connection))
            {
                string result = null;
                try
                {
                    command.CommandTimeout = 900;
                    command.CommandText = queryString;
                    if (parameters != null)
                    {
                        foreach (var parameter in parameters)
                        {
                            var param = command.CreateParameter();
                            param.ParameterName = parameter.ParameterName;
                            param.Value = parameter.Value;
                            //param.DbType = parameter.DataType;
                            command.Parameters.Add(param);
                        }
                    }

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            reader.Read();
                            result = reader[0].ToString();
                        }

                        return result;
                    }
                }
                catch (Exception ex)
                {
                    await Console.Error.WriteLineAsync(ex.ToString());
                    return "No Data";
                }
                finally
                {
                    command.Parameters.Clear();
                }
            }
        }
    }
}
