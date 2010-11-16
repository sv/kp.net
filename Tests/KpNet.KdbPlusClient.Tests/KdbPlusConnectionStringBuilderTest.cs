using System;
using NUnit.Framework;

namespace KpNet.KdbPlusClient.Tests
{
    [TestFixture]
    public sealed class KdbPlusConnectionStringBuilderTest
    {
        [Test]
        public void CorrectStringParseTest()
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder("server=1;port=2;user id=3;password=4;buffer size=16384;pooling=false;min pool size=10;max pool size=11;load balance timeout=10;inactivity timeout=10");
            Assert.AreEqual("1", builder.Server);
            Assert.AreEqual(2, builder.Port);
            Assert.AreEqual("3", builder.UserID);
            Assert.AreEqual("4", builder.Password);
            Assert.AreEqual(16384, builder.BufferSize);
            Assert.AreEqual(false, builder.Pooling);
            Assert.AreEqual(10, builder.MinPoolSize);
            Assert.AreEqual(11, builder.MaxPoolSize);
            Assert.AreEqual(10, builder.LoadBalanceTimeout);
            Assert.AreEqual(10, builder.InactivityTimeout);
        }

        [Test]
        public void DifferentCaseStringParseTest()
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder("Server=1;Port=2;User ID=3;Password=4;buffersize=16384");
            Assert.AreEqual("1", builder.Server);
            Assert.AreEqual(2, builder.Port);
            Assert.AreEqual("3", builder.UserID);
            Assert.AreEqual("4", builder.Password);
            Assert.AreEqual(16384, builder.BufferSize);
        }

        [Test]
        public void DefaultsTest()
        {
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder("Server=1;Port=2");
            Assert.AreEqual("1", builder.Server);
            Assert.AreEqual(2, builder.Port);
            Assert.AreEqual(String.Empty, builder.UserID);
            Assert.AreEqual(String.Empty, builder.Password);
            Assert.AreEqual(KdbPlusConnectionStringBuilder.DefaultBufferSize, builder.BufferSize);
            Assert.AreEqual(KdbPlusConnectionStringBuilder.DefaultPooling, builder.Pooling);
            Assert.AreEqual(KdbPlusConnectionStringBuilder.DefaultMinPoolSize, builder.MinPoolSize);
            Assert.AreEqual(KdbPlusConnectionStringBuilder.DefaultMaxPoolSize, builder.MaxPoolSize);
            Assert.AreEqual(KdbPlusConnectionStringBuilder.DefaultLoadBalanceTimeoutSeconds, builder.LoadBalanceTimeout);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void EmptyStringParseTest()
        {
            new KdbPlusConnectionStringBuilder(null);
        }
    }
}
