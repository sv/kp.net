using KpNet.KdbPlusClient;

namespace KpNet.Hosting
{
    public abstract class KdbPlusProcess
    {
        protected KdbPlusProcess()
        {}

        public abstract void Start();

        public abstract void Kill();

        public abstract bool IsAlive
        { get; }

        public abstract IDatabaseClient GetConnection();

        public static KdbPlusProcessBuilder Builder
        {
            get { return new KdbPlusProcessBuilder(); }
        }
    }
}
