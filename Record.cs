using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace ExternalSort
{
    /// <summary>
    /// Defines a record of fields typed integer.
    /// </summary>
    public class Record
    {
        private int[] _fields;
        public int[] Fields { get { return _fields; } }

        public Record(params int[] fields)
        {
            _fields = fields;
        }

        public Record(Record record)
        {
            _fields = record.Fields;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(_fields.Length);
            for (int j = 0; j < _fields.Length; j++)
            {
                sb.Append(_fields[j] + ",");
            }
            sb.Remove(sb.Length - 1, 1);
            return sb.ToString();
        }

        public int this[int index]
        {
            get { return _fields[index]; }
        }
    }
}