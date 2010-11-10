using System;
using System.Collections.Generic;
using KpNet.Common;
using KpNet.KdbPlusClient;

namespace KpNet.Hosting
{
    /// <summary>
    /// Class for the composite Kdb process.
    /// </summary>
    internal sealed class CompositeKdbPlusProcess : KdbPlusProcess
    {
        private readonly IEnumerable<KdbPlusProcess> _processes;

        /// <summary>
        /// Initializes a new instance of the <see cref="CompositeKdbPlusProcess"/> class.
        /// </summary>
        /// <param name="processes">The processes.</param>
        public CompositeKdbPlusProcess(IEnumerable<KdbPlusProcess> processes)
        {
            Guard.ThrowIfNull(processes, "processes");

            _processes = processes;
        }

        /// <summary>
        /// Restarts this instance.
        /// </summary>
        public override bool Restart(out IDatabaseClient client)
        {
            List<IDatabaseClient> connections = new List<IDatabaseClient>();

            foreach (KdbPlusProcess process in _processes)
            {
                IDatabaseClient dbClient;

                if(process.Restart(out dbClient))
                {
                    connections.Add(dbClient);
                }
            }

            if(connections.Count > 0)
            {
                client = new CompositeDatabaseClient(connections);

                return true;
            }

            client = null;

            return false;
        }

        /// <summary>
        /// Starts this instance.
        /// </summary>
        public override void Start()
        {
            try
            {
                foreach (KdbPlusProcess process in _processes)
                {
                    process.Start();
                }
            }
            catch (Exception)
            {
                Kill();

                throw;
            }            
        }

        /// <summary>
        /// Kills this instance.
        /// </summary>
        public override void Kill()
        {
            foreach (KdbPlusProcess process in _processes)
            {
                process.Kill();
            }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is alive.
        /// </summary>
        /// <value><c>true</c> if this instance is alive; otherwise, <c>false</c>.</value>
        public override bool IsAlive
        {
            get
            {
                foreach (KdbPlusProcess process in _processes)
                {
                    if(!process.IsAlive)
                    {
                        return false;
                    }
                }
                return true;
            }
        }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <returns>Connection.</returns>
        public override IDatabaseClient GetConnection()
        {
            List<IDatabaseClient> connections = new List<IDatabaseClient>();

            foreach (KdbPlusProcess process in _processes)
            {
                connections.Add(process.GetConnection());
            }

            return new CompositeDatabaseClient(connections);
        }

        /// <summary>
        /// Sets the port.
        /// </summary>
        /// <param name="port">The port.</param>
        public override void SetPort(int port)
        {
            foreach (KdbPlusProcess process in _processes)
            {
                process.SetPort(port);
            }
        }

        /// <summary>
        /// Loads the directory.
        /// </summary>
        /// <param name="path">The path.</param>
        public override void LoadDirectory(string path)
        {
            foreach (KdbPlusProcess process in _processes)
            {
                process.LoadDirectory(path);
            }
        }

        /// <summary>
        /// Loads the file.
        /// </summary>
        /// <param name="path">The path.</param>
        public override void LoadFile(string path)
        {
            foreach (KdbPlusProcess process in _processes)
            {
                process.LoadFile(path);
            }
        }
    }
}
