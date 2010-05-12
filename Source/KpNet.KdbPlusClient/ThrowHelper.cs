using System;

namespace KpNet.KdbPlusClient
{
    internal static class ThrowHelper
    {
        public static T ThrowNotSupported<T>()
        {
            ThrowNotSupported();
            return default(T);
        }

        public static void ThrowNotSupported()
        {
            throw new NotSupportedException(Resources.NotSupportedInKdbPlus);
        }

        public static T ThrowNotImplemented<T>()
        {
            ThrowNotImplemented();

            return default(T);
        }

        public static void ThrowNotImplemented()
        {
            throw new NotSupportedException(Resources.NotImplementedInKdbPlus);
        }
    }
}
