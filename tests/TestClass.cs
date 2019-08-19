using GlitchedPolygons.RepositoryPattern;

namespace Tests
{
    public class TestClass : IEntity<long>
    {
        public long Id { get; set; }
        public long TestLong { get; set; }
        public bool TestBool { get; set; }
        public double TestDouble { get; set; }
        public string TestString { get; set; }
    }
}
