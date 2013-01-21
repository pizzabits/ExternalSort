using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace ExternalSort
{
    public class Page
    {
        private Record[] _records;
        private int _numberOfFields;
        public int FieldCount { get { return _numberOfFields; } }
        public Record[] Records { get { return _records; } }
        public int RecordOffset { get; set; }
        
        public Page(int fieldCount, int recordCount)
        {
            _records = new Record[recordCount];
            _numberOfFields = fieldCount;
        }

        public Page(int fieldToSortBy, string[] records)
        {
            int i;
            for (i = 0; i < records.Length && records[i] != null; i++)
            {
                string[] fieldsToParse = records[i].Split(',');
                if (_records == null)
                {
                    _numberOfFields = fieldsToParse.Length;
                    _records = new Record[records.Length];
                }

                int[] fields = new int[_numberOfFields];

                for (int j = 0; j < _numberOfFields; j++)
                {
                    fields[j] = int.Parse(fieldsToParse[j]);
                }

                _records[i] = new Record(fieldToSortBy ,fields);
            }
        }

        public Page(Page page)
        {
            _records = page.Records;
            _numberOfFields = page.FieldCount;
        }

        internal void Sort(int indexField)
        {
            Array.Sort(_records);
        }

        internal void Print()
        {
            foreach (var item in _records)
            {
                Console.WriteLine(item.ToString());
            }
        }

        internal IEnumerable<String> ProduceStringLinesRepresentation()
        {
            for (int i = 0; i < _records.Length && _records[i] != null; i++)
            {
                yield return _records[i].ToString();
            }
        }

        internal void WritePage(System.IO.StreamWriter streamWriter)
        {
            foreach (var line in ProduceStringLinesRepresentation())
            {
                streamWriter.WriteLine(line);
            }
        }
    }
}