// Copyright (c) FieldAssist. All Rights Reserved.

using ClickHouse.Client.ADO;

namespace ClickHouseConnector
{

    public interface IClickHouseConnectionManager
    {
        ClickHouseConnection GetConnection();
        void ReleaseConnection(ClickHouseConnection connection);

    }
    public class ClickHouseConnectionManager : IClickHouseConnectionManager, IDisposable
    {
        private static readonly object _lock = new object();
        private static Queue<ClickHouseConnection> _connectionPool;

        private readonly string _connectionString;
        private readonly int _poolSize;

        public ClickHouseConnectionManager(string connectionString, int poolSize = 10)
        {
            _connectionString = connectionString;
            _poolSize = poolSize;

            // Ensure connection pool is initialized only once
            lock (_lock)
            {
                if (_connectionPool == null)
                {
                    InitializeConnectionPool();
                }
            }
        }

        private void InitializeConnectionPool()
        {
            _connectionPool = new Queue<ClickHouseConnection>(_poolSize);

            for (var i = 0; i < _poolSize; i++)
            {
                var connection = CreateNewConnection();
                _connectionPool.Enqueue(connection);
            }
        }

        public ClickHouseConnection GetConnection()
        {
            lock (_connectionPool)
            {
                if (_connectionPool.Count > 0)
                {
                    return _connectionPool.Dequeue();
                }
            }

            return CreateNewConnection();
        }

        public void ReleaseConnection(ClickHouseConnection connection)
        {
            lock (_connectionPool)
            {
                if (_connectionPool.Count < _poolSize)
                {
                    _connectionPool.Enqueue(connection);
                }
                else
                {
                    connection.Dispose();
                }
            }
        }

        private ClickHouseConnection CreateNewConnection()
        {
            var connection = new ClickHouseConnection(_connectionString);
            connection.Open();
            return connection;
        }

        public void Dispose()
        {
            lock (_connectionPool)
            {
                foreach (var connection in _connectionPool)
                {
                    connection.Dispose();
                }

                _connectionPool.Clear();
            }
        }
    }
}
