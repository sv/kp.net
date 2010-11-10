using KpNet.KdbPlusClient;

namespace KpNet.Hosting
{
    /// <summary>
    /// Abstract class for the Kdb process.
    /// </summary>
    public abstract class KdbPlusProcess
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="KdbPlusProcess"/> class.
        /// </summary>
        protected KdbPlusProcess()
        {}


        /// <summary>
        /// Restarts this instance.
        /// </summary>
        /// <returns>Connection</returns>
        public abstract bool Restart(out IDatabaseClient client);

        /// <summary>
        /// Starts this instance.
        /// </summary>
        public abstract void Start();

        /// <summary>
        /// Kills this instance.
        /// </summary>
        public abstract void Kill();

        /// <summary>
        /// Gets a value indicating whether this instance is alive.
        /// </summary>
        /// <value><c>true</c> if this instance is alive; otherwise, <c>false</c>.</value>
        public abstract bool IsAlive
        { get; }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <returns>Connection.</returns>
        public abstract IDatabaseClient GetConnection();

        /// <summary>
        /// Gets the builder.
        /// </summary>
        /// <value>The builder.</value>
        public static KdbPlusProcessBuilder Builder
        {
            get { return new KdbPlusProcessBuilder(); }
        }

        /// <summary>
        /// Sets the port.
        /// </summary>
        /// <param name="port">The port.</param>
        public abstract void SetPort(int port);

        /// <summary>
        /// Loads the directory.
        /// </summary>
        /// <param name="path">The path.</param>
        public abstract void LoadDirectory(string path);

        /// <summary>
        /// Loads the file.
        /// </summary>
        /// <param name="path">The path.</param>
        public abstract void LoadFile(string path);
    }
}
