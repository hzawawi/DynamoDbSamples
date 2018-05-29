using System;
using System.Threading.Tasks;
using LearnDynamo;
using Xunit;

namespace LearnDynamoTests
{
    public class DynamoClientTests
    {
        private readonly DynamoClient _dynamoDbClient;

        public DynamoClientTests()
        {
            _dynamoDbClient = new DynamoClient();
            try
            {
                _dynamoDbClient.SetupAsync().Wait();
            }
            catch (AggregateException e)
            {
                //ignore table already created
                Console.WriteLine(e);
            }
        }
        
        [Fact]
        public async Task SaveOrUpdateAStudentAndRetrieveItBackUsingHashKey()
        {
            var person = new Student
            {
                Id = 1,
                FirstName = "sam",
                LastName = "griffen",
            };

            await _dynamoDbClient.SaveOrUpdateStudent(person);

            var returnedPerson =  await _dynamoDbClient.GetStudentUsingHashKey(person.Id);

            Assert.Equal(returnedPerson, person);
        }
        
        [Fact]
        public async Task SaveOnlyAStudentAndRetrieveItBackUsingHashKey()
        {
            var person = new Student
            {
                Id = 3,
                FirstName = "sam",
                LastName = "griffen",
            };

            await _dynamoDbClient.SaveOnlyStudent(person);

            var returnedPerson =  await _dynamoDbClient.GetStudentUsingHashKey(person.Id);

            Assert.Equal(returnedPerson, person);
        }
        
        [Fact]
        public async Task SaveAStudentAndRetrieveItBackUsingScanOnFirstName()
        {
            var person = new Student
            {
                Id = 2,
                FirstName = "tom",
                LastName = "walker",
            };

            await _dynamoDbClient.SaveOrUpdateStudent(person);

            var returnedPerson = await _dynamoDbClient.ScanForStudentUsingFirstName(person.FirstName);

            Assert.Equal(returnedPerson, person);
        }
    }
}