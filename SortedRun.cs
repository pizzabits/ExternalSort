using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace ExternalSort
{
    public class SortedRun
    {
        public String Filename;
        public StreamReader Reader;
        public StreamWriter Writer;

        internal StreamReader GetReader()
        {
            if (Writer != null)
            {
                Writer.Close();
                Writer = null;
            }
            if (Reader == null)
            {
                Reader = File.OpenText(Filename);
            }
            return Reader;
        }

        internal StreamWriter GetWriter()
        {
            if (Reader != null)
            {
                Reader.Close();
                Reader = null;
            }
            if (Writer == null)
            {
                Writer = new StreamWriter(Filename);
            }
            return Writer;
        }
    }
}