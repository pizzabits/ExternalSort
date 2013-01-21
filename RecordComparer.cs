using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExternalSort
{
    public class RecordComparer : IComparer<Record>
    {
        private int _fieldToSortBy;
        public RecordComparer(int fieldToSortBy)
        {
            _fieldToSortBy = fieldToSortBy;
        }
        public int Compare(Record x, Record y)
        {
            return x[_fieldToSortBy] - y[_fieldToSortBy];
        }
    }
}