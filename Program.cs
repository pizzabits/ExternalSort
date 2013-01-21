using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;

namespace ExternalSort
{
    public class Program
    {
        public static String CreateUnsortedFile(int min, int max, int fields, int records)
        {
            Debug.Assert(min < max, "The minimal value must be smaller than the maximum value!");
            Debug.Assert(fields > 1, "Must provide more than one field!");
            String filename = Path.GetTempFileName();
            Random rand = new Random();
            byte linesBufferSize = 200;
            String[] lines = new String[linesBufferSize];
            for (int i = 1; i <= records; i++)
            {
                StringBuilder line = new StringBuilder();
                for (int j = 0; j < fields; j++)
                {
                    line.Append(String.Format("{0},", rand.Next(min, max)));
                }
                lines[i % linesBufferSize] = line.Remove(line.Length - 1, 1).ToString();
                if (i > 1 && i % linesBufferSize == 0)
                {
                    File.AppendAllLines(filename, lines);
                    lines = new String[linesBufferSize];
                }
            }
            File.AppendAllLines(filename, lines.Where(s => s != null));
            return filename;
        }

        public static void Main(string[] args)
        {
            int min = 0;
            int max = 10000000;
            int fields = 3;
            int fieldToSortBy = 2;
            int records = 10000;
            int bufferPages = 3;
            int recordsPerPage = 500;

            Console.WriteLine("Creating an unsorted random file containing {0} records,\neach record contains {1} fields, " +
                                "and the values are between {2} and {3}", records, fields, min, max);
            String filename = CreateUnsortedFile(min, max, fields, records);
            Console.WriteLine("The unsorted file was created successfully and can found at: {0}", filename);
            PressAnyKeyToContinue();

            ExternalSort bp = new ExternalSort(bufferPages, recordsPerPage);
            Console.WriteLine("\nStarting a sort on field #{0} using external sort with {1} buffer pages, each page may contain {2} records",
                fieldToSortBy, bufferPages, recordsPerPage);

            String sortedFilename = bp.Sort(filename, fieldToSortBy, fields);

            Console.WriteLine("\nThe sorted file was created successfully and can be found at: {0}", sortedFilename);
            PressAnyKeyToContinue();
        }

        private static void PressAnyKeyToContinue()
        {
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }
    }
}