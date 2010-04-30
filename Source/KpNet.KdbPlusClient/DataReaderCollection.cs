using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;

namespace KpNet.KdbPlusClient
{
    public sealed class DataReaderCollection
    {
        private readonly Dictionary<string, IDataReader> _readers;

        public DataReaderCollection(Dictionary<string, IDataReader> readers)
        {
            Guard.ThrowIfNull(readers,"readers");
            
            _readers = readers;
        }

        public int Count
        {
            get
            {
                return _readers.Count;
            }
        }

        public IDataReader this[string key]
        {
            get
            {
                IDataReader reader;

                if (!_readers.TryGetValue(key, out reader))
                {
                    throw new ApplicationException(string.Format("Cannot find reader '{0}'.", key));
                }

                return reader;
            }
        }

        public IDataReader this[int index]
        {
            get
            {
                if (index < 0 || index >= _readers.Count)
                {
                    throw new ApplicationException(string.Format("Index value '{0}' is out of the dictionary scope.", index));
                }

                return _readers.ElementAt(index).Value;
            }
        }
    }
}
