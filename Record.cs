using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace ExternalSort
{
    public class Record : IComparable
    {
        private int[] _fields;
        public int[] Fields { get { return _fields; } }
        public int FieldToSortBy { get; set; }

        public Record(int fieldToSortBy, params int[] fields)
        {
            FieldToSortBy = fieldToSortBy;
            _fields = fields;
        }

        public Record(Record record)
        {
            FieldToSortBy = record.FieldToSortBy;
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

        public int CompareTo(object obj)
        {
            Record other = obj as Record;
            Debug.Assert(other != null, "Cannot compare a Record type to a different type!");
            return this[FieldToSortBy] - other[other.FieldToSortBy];
        }
    }
}