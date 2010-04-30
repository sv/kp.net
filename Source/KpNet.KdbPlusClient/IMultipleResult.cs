using System.Data;

namespace KpNet.KdbPlusClient
{
    public interface IMultipleResult
    {
        IDataReader GetResult(string name);
        IDataReader GetResult(int i);

        int Count { get; }
        DataReaderCollection Results { get; }
    }
}
