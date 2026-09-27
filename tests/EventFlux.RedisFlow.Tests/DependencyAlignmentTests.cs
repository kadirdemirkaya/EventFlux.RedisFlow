using EventFlux.RedisFlow.Workers;
using Xunit;

namespace EventFlux.RedisFlow.Tests
{
    public class DependencyAlignmentTests
    {
        [Fact]
        public void ReferencedMicrosoftExtensionsAssemblies_MatchRuntimeMajor()
        {
            var runtimeMajor = Environment.Version.Major;

            var mismatched = typeof(RedisStreamWorker).Assembly
                .GetReferencedAssemblies()
                .Where(a => a.Name!.StartsWith("Microsoft.Extensions.", StringComparison.Ordinal))
                .Where(a => a.Version!.Major != runtimeMajor)
                .Select(a => $"{a.Name} {a.Version}")
                .ToArray();

            Assert.True(mismatched.Length == 0, $"Runtime {runtimeMajor}.x, mismatched references: {string.Join(", ", mismatched)}");
        }
    }
}
