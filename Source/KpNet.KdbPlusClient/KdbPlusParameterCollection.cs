using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace KpNet.KdbPlusClient
{
    public sealed class KdbPlusParameterCollection : DbParameterCollection
    {
        private const int NoIndex = -1;
        private readonly List<KdbPlusParameter> _parameters = new List<KdbPlusParameter>();
        private readonly object _syncRoot = new object();
        public event EventHandler ParametersChanged;

        public override int Add(object value)
        {
            KdbPlusParameter param = GetParameter(value);

            AddParameter(param);

            return _parameters.IndexOf(param);
        }

        public override bool Contains(object value)
        {
            KdbPlusParameter param = GetParameter(value);

            return _parameters.Contains(param); 
        }

        public override void Clear()
        {
            foreach (KdbPlusParameter param in _parameters)
            {
                UnSubscribeToParameterChanges(param);
            }

            _parameters.Clear();
            InvokeParametersChanged();
        }

        public override int IndexOf(object value)
        {
            KdbPlusParameter param = GetParameter(value);

            return _parameters.IndexOf(param);
        }

        public override void Insert(int index, object value)
        {
            KdbPlusParameter param = GetParameter(value);

            InsertParameter(index, param);
        }

        public override void Remove(object value)
        {
            KdbPlusParameter param = GetParameter(value);

            _parameters.Remove(param);
            UnSubscribeToParameterChanges(param);
            InvokeParametersChanged();
        }

        public override void RemoveAt(int index)
        {
            RemoveParameterAtIndex(index);
        }

        public override IEnumerator GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        protected override DbParameter GetParameter(int index)
        {
            return _parameters[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return FindParameter(parameterName);
        }

        public override void AddRange(Array values)
        {
            IEnumerable<KdbPlusParameter> parameters = (IEnumerable<KdbPlusParameter>) values;
            _parameters.AddRange(parameters);

            foreach (KdbPlusParameter param in parameters)
            {
                SubscribeToParameterChanges(param);
            }

            InvokeParametersChanged();
        }

        public override bool Contains(string parameterName)
        {
            return FindParameter(parameterName) != null; 
        }

        public override void CopyTo(Array array, int index)
        {
            _parameters.CopyTo((KdbPlusParameter[])array,index);
        }

        public override int Count
        {
            get { return _parameters.Count; }
        }

        public override object SyncRoot
        {
            get { return _syncRoot; }
        }

        public override bool IsFixedSize
        {
            get { return false; }
        }

        public override bool IsReadOnly
        {
            get { return false; }
        }

        public override bool IsSynchronized
        {
            get { return false; }
        }

        public override int IndexOf(string parameterName)
        {
            return FindIndex(parameterName);
        }

        public override void RemoveAt(string parameterName)
        {
            int idx = FindIndex(parameterName);

            if (idx != NoIndex)
            {
                RemoveParameterAtIndex(idx);
            }

            else throw new InvalidOperationException("Parameter is missing in the collection.");
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            KdbPlusParameter param = GetParameter(value);

            InsertParameter(index,param);
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            KdbPlusParameter param = GetParameter(value);
            int idx = FindIndex(parameterName);

            if (idx >= 0)
            {
                RemoveParameterAtIndex(idx);
            }

            AddParameter(param);
        }

        private static KdbPlusParameter GetParameter(object value)
        {
            Guard.ThrowIfNull(value, "value");

            KdbPlusParameter param = value as KdbPlusParameter;

            if (param == null)
                throw new ArgumentException("Parameter is not KdbPlusParameter.");
            return param;
        }

        private KdbPlusParameter FindParameter(string parameterName)
        {
            foreach (KdbPlusParameter param in _parameters)
            {
                if (String.Compare(param.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase) == 0)
                    return param;
            }

            return null;
        }

        private int FindIndex(string parameterName)
        {
            Guard.ThrowIfNullOrEmpty(parameterName, "parameterName");

            for (int i = 0; i < _parameters.Count; i++)
            {
                if (String.Compare(_parameters[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase) == 0)
                    return i;
            }

            return NoIndex;
        }

        private void AddParameter(KdbPlusParameter param)
        {
            _parameters.Add(param);
            SubscribeToParameterChanges(param);
            InvokeParametersChanged();
        }

        private void InsertParameter(int index, KdbPlusParameter param)
        {
            _parameters.Insert(index, param);
            SubscribeToParameterChanges(param);
            InvokeParametersChanged();
        }

        private void RemoveParameterAtIndex(int index)
        {
            UnSubscribeToParameterChanges(_parameters[index]);
            _parameters.RemoveAt(index);
            InvokeParametersChanged();
        }

        private void SubscribeToParameterChanges(KdbPlusParameter parameter)
        {
            parameter.ParameterChanged += SingleParameterChanged;
        }

        private void UnSubscribeToParameterChanges(KdbPlusParameter parameter)
        {
            parameter.ParameterChanged -= SingleParameterChanged;
        }

        private void InvokeParametersChanged()
        {
            EventHandler handler = ParametersChanged;
            if (handler != null) handler(this, EventArgs.Empty);
        }

        void SingleParameterChanged(object sender, EventArgs e)
        {
            InvokeParametersChanged();
        }
    }

}
