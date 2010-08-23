using System.Collections.Generic;
using Kdbplus;
using System.Data;

namespace KpNet.KdbPlusClient
{
    internal sealed class KdbMultipleResult : IMultipleResult
    {
        private readonly DataReaderCollection _readers;
        
        public KdbMultipleResult(c.Dict kdbResult)
        {
            Guard.ThrowIfNull(kdbResult, "kdbResult");
            
            _readers = CreateReaders(kdbResult);
        }

        
        private static DataReaderCollection CreateReaders(c.Dict kdbResult)
        {
            Dictionary<string, IDataReader> tableReaders = new Dictionary<string, IDataReader>();

            string[] tableNames = (string[])kdbResult.x;

            object[] tables = (object[])kdbResult.y;
            
            for (int i = 0; i < tableNames.Length; i++)
            {
                object[] temp = tables[i] as object[];
                if (temp != null && temp.Length == 0)
                {
                    tableReaders[tableNames[i]] = KdbPlusDataReader.CreateEmptyReader();
                }
                else
                {
                    tableReaders[tableNames[i]] = new KdbPlusDataReader(c.td(tables[i]));    
                }
            }

            return new DataReaderCollection(tableReaders);
        }

        
        #region IMultipleResult Members

        public IDataReader GetResult(string name)
        {
            return _readers[name];
        }

        public IDataReader GetResult(int i)
        {
            return _readers[i];
        }

        public int Count
        {
            get { return _readers.Count; }
        }

        public DataReaderCollection Results
        {
            get { return _readers; }
        }

        #endregion

        
    }
}
