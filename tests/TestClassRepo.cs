using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dapper;
using GlitchedPolygons.RepositoryPattern.Postgres;

namespace Tests
{
    public class TestClassRepo : PostgresRepository<TestClass, long>
    {
        private readonly string insertionSql;

        public TestClassRepo(string connectionString) : base(connectionString, idColumnName: "Id")
        {
            insertionSql = $@"INSERT INTO ""{TableName}"" (""TestLong"", ""TestBool"", ""TestDouble"", ""TestString"") VALUES (@TestLong, @TestBool, @TestDouble, @TestString)";
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

        public override async Task<bool> AddRange(IEnumerable<TestClass> entities)
        {
            bool success = false;

            using (var dbcon = OpenConnection())
            using (var t = dbcon.BeginTransaction())
            {
                success = await dbcon.ExecuteAsync(insertionSql, entities, t) > 0;
                
                if (success)
                {
                    t.Commit();
                }
            }

            return success;
        }

        public override async Task<bool> Update(TestClass entity)
        {
            var sql = new StringBuilder(256)
                .Append($"UPDATE \"{TableName}\" SET ")
                .Append("\"TestLong\" = @TestLong, ")
                .Append("\"TestBool\" = @TestBool, ")
                .Append("\"TestDouble\" = @TestDouble, ")
                .Append("\"TestString\" = @TestString ")
                .Append("WHERE \"Id\" = @Id")
                .ToString();

            using (var dbcon = OpenConnection())
            {
                return await dbcon.ExecuteAsync(sql, entity) > 0;
            }
        }
    }
}
