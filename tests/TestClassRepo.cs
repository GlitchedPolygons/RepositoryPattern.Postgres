using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dapper;
using GlitchedPolygons.RepositoryPattern.Postgres;

namespace Tests
{
    public class TestClassRepo : PostgresRepository<TestClass, long>
    {
        private readonly string insertionSql;

        public TestClassRepo(string connectionString) : base(connectionString)
        {
            insertionSql = $"INSERT INTO \"{TableName}\" (\"TestLong\", \"TestBool\", \"TestDouble\", \"TestString\") VALUES (@TestLong, @TestBool, @TestDouble, @TestString)";
        }

        public override async Task<bool> Add(TestClass entity)
        {
            bool success = false;

            using (var dbcon = OpenConnection())
            {
                success = await dbcon.ExecuteAsync(insertionSql, new
                {
                    entity.TestLong,
                    entity.TestBool,
                    entity.TestDouble,
                    entity.TestString
                }) > 0;
            }

            return success;
        }

        public override Task<bool> AddRange(IEnumerable<TestClass> entities)
        {
            throw new NotImplementedException();
        }

        public override Task<bool> Update(TestClass entity)
        {
            throw new NotImplementedException();
        }
    }
}