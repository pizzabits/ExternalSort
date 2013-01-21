using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace ExternalSort
{
    /// <summary>
    /// Defines a page of multiple <typeparamref name="Record"/>.
    /// Able to parse the fields of an input string array,
    /// or create an empty page.
    /// The page, provided a comparer, can sort the records within.
    /// Also can be written, provided a stream writer.
    /// </summary>
    public class Page
    {
        public int FieldCount { get; private set; }
        public Record[] Records { get; private set; }
        
        public Page(int fieldCount, int recordCount)
        {
            Records = new Record[recordCount];
            FieldCount = fieldCount;
        }

        public Page(string[] records)
        {
            int i;
            for (i = 0; i < records.Length && records[i] != null; i++)
            {
                string[] fieldsToParse = records[i].Split(',');
                if (Records == null)
                {
                    FieldCount = fieldsToParse.Length;
                    Records = new Record[records.Length];
                }

                int[] fields = new int[FieldCount];

                for (int j = 0; j < FieldCount; j++)
                {
                    fields[j] = int.Parse(fieldsToParse[j]);
                }

                Records[i] = new Record(fields);
            }
        }

        public Page(Page page)
        {
            Records = page.Records;
            FieldCount = page.FieldCount;
        }

        internal void Sort(IComparer<Record> comparer)
        {
            Array.Sort(Records, comparer);
        }

        internal void Print()
        {
            foreach (var item in Records)
            {
                Console.WriteLine(item.ToString());
            }
        }

        internal IEnumerable<String> ProduceStringLinesRepresentation()
        {
            for (int i = 0; i < Records.Length && Records[i] != null; i++)
            {
                yield return Records[i].ToString();
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