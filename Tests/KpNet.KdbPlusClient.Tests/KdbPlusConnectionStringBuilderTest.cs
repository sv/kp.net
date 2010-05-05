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
            KdbPlusConnectionStringBuilder builder = new KdbPlusConnectionStringBuilder("server=1;port=2;user id=3;password=4;buffersize=16384");
            Assert.AreEqual("1", builder.Server);
            Assert.AreEqual(2, builder.Port);
            Assert.AreEqual("3", builder.UserID);
            Assert.AreEqual("4", builder.Password);
            Assert.AreEqual(16384, builder.BufferSize);
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
        [ExpectedException(typeof(ArgumentNullException))]
        public void EmptyStringParseTest()
        {
            new KdbPlusConnectionStringBuilder(null);
        }
    }
}
