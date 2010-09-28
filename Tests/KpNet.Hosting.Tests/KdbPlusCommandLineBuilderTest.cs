using NUnit.Framework;

namespace KpNet.Hosting.Tests
{
    [TestFixture]
    public class KdbPlusCommandLineBuilderTest
    {
        [Test]
        public void EmptyCommandLineTest()
        {
            KdbPlusCommandLineBuilder builder = new KdbPlusCommandLineBuilder();

            Assert.AreEqual(builder.CreateNew(), string.Empty);
        }

        [Test]
        public void CommandLineWithoutSyncLoggingAndMultithreadingTest()
        {
            string template = @"logpath -l -p 1001 -s 5 ";

            KdbPlusCommandLineBuilder builder = new KdbPlusCommandLineBuilder();

            builder.SetLog("logpath").SetPort(1001).SetThreadCount(5);

            Assert.AreEqual(builder.CreateNew(), template);
        }

        [Test]
        public void CommandLineWithSyncLoggingAndMultithreadingTest()
        {
            string template = @"logpath -L -p -1001 -s 5 ";

            KdbPlusCommandLineBuilder builder = new KdbPlusCommandLineBuilder();

            builder.SetLog("logpath").EnableSyncLogging().SetPort(1001).EnableMultiThreading().SetThreadCount(5);

            Assert.AreEqual(builder.CreateNew(), template);
        }
    }
}
