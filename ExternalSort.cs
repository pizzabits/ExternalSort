using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;

namespace ExternalSort
{
    public class ExternalSort
    {
        private int _numberOfBufferPages;
        private int _pageSizeInRecords;
        private int _iteration;
        private Page[] _pool;
        private Dictionary<int, List<SortedRun>> _iterationToSortedruns;

        public ExternalSort(int bufferPages, int pageSizeInRecords)
        {
            Debug.Assert(bufferPages > 1, "Buffer pages must more than one page!");
            _numberOfBufferPages = bufferPages;
            _pageSizeInRecords = pageSizeInRecords;
            _pool = new Page[_numberOfBufferPages];
        }

        public string Sort(string filename, int fieldIndexToSortBy, int fieldCount)
        {
            FirstIteration(filename, fieldIndexToSortBy);

            while (_iterationToSortedruns[_iteration - 1].Count > 1)
            {
                Console.WriteLine("\nCommencing iteration {0} \n", _iteration);

                int sortedrunIndex = 0;
                int sortedrunsUsed = 0;
                _pool = new Page[_numberOfBufferPages];
                    
                // load the first page of each of the B-1 sortedruns from the last iteration 
                // and merge sort them using the last page in the buffer pool
                for (int i = 0; i < _iterationToSortedruns[_iteration - 1].Count; i++)
                {
                    if (i == _numberOfBufferPages - 1) // Use only B-1 sortedruns
                    {
                        sortedrunsUsed = i;
                        break;
                    }
                    _pool[i] = new Page(fieldIndexToSortBy, ReadPage(_iterationToSortedruns[_iteration - 1][i].GetReader()));
                }

                // point to current index in each sortedrun and also the buffer page
                int[] pointers = new int[_numberOfBufferPages];
                // reset the buffer page to contains 0 records
                _pool[_numberOfBufferPages - 1] = new Page(fieldCount, _pageSizeInRecords);

                // initialize the current iteration's pages
                int producedSortedruns = 0;
                _iterationToSortedruns[_iteration] = new List<SortedRun>();
                string newSortedrunFilename = Path.GetTempFileName();
                var sortedRun = new SortedRun();
                sortedRun.Filename = newSortedrunFilename;
                sortedRun.GetWriter();
                _iterationToSortedruns[_iteration].Add(sortedRun);

                int pageCount = 0;
                bool finishedMergingSet = false;
                
                // while there are still sortedruns from previous iteration
                while (!finishedMergingSet)
                {
                    finishedMergingSet = true;
                    for (int i = 0; i < _numberOfBufferPages - 1; i++)
                    {
                        if (_pool[i] != null && _pool[i].Records != null && _pool[i].Records[pointers[i]] != null)
                        {
                            finishedMergingSet = false;
                            break;
                        }
                    }
                    if (finishedMergingSet)
                    {
                        // close the latest sortedrun written
                        _iterationToSortedruns[_iteration][producedSortedruns++].Writer.Close();

                        // load the first page from B-1 sortedruns of the previous iteration
                        // continuing from the next sortedrun which was not read
                        sortedrunIndex = sortedrunsUsed;
                        for (int i = sortedrunsUsed; i < _iterationToSortedruns[_iteration - 1].Count; i++)
                        {
                            if (i - sortedrunIndex == (_numberOfBufferPages - 1)) // Use up to B-1 sortedruns
                            {
                                break;
                            }
                            _pool[i - sortedrunIndex] = new Page(fieldIndexToSortBy, ReadPage(_iterationToSortedruns[_iteration - 1][i].GetReader()));
                            sortedrunsUsed++;
                        }

                        // no more sorted runs?
                        if (sortedrunIndex == sortedrunsUsed)
                        {
                            continue;
                        }
                        foreach (var item in _pool.Take(_numberOfBufferPages - 1))
                        {
                            if (item != null && item.Records != null)
                            {
                                finishedMergingSet = false;
                            }
                        }
                        if (finishedMergingSet)
                            continue;

                        // there're more sorted runs, prepare a new merged sortedrun
                        String tempFilename = Path.GetTempFileName();
                        _iterationToSortedruns[_iteration].Add(new SortedRun() { Filename = tempFilename });
                        pageCount = 0;
                    }

                    Record smallest = null;
                    // while the buffer page doesn't exceed
                    while (pointers[_numberOfBufferPages-1] < _pageSizeInRecords)
                    {
                        // initialize the lowest records
                        smallest = null;
                        int firstPageThatIsNotEmpty = 0;
                        // find the first page that is not empty
                        for (int i = 0; i < _numberOfBufferPages; i++)
                        {
                            if (_pool[i] != null && _pool[i].Records != null && _pool[i].Records[pointers[i]] != null)
                            {
                                // choose the first non-null record as the smallest
                                smallest = new Record(_pool[i].Records[pointers[i]]);
                                firstPageThatIsNotEmpty = i;
                                break;
                            }
                        }

                        // ran out of values?
                        if (smallest == null)
                            break;

                        int pageIndexContainingLowestKey = firstPageThatIsNotEmpty;
                        // search for the smallest key in any of the current pages
                        for (int i = firstPageThatIsNotEmpty + 1; i < _numberOfBufferPages; i++)
                        {
                            if (_pool[i] != null && _pool[i].Records != null 
                                && _pool[i].Records[pointers[i]] != null 
                                && _pool[i].Records[pointers[i]].Fields[fieldIndexToSortBy] < smallest[fieldIndexToSortBy])
                            {
                                smallest = _pool[i].Records[pointers[i]];
                                pageIndexContainingLowestKey = i;
                            }
                        }

                        // insert the smallest record into the buffer page and increase the page's pointers.
                        _pool[_numberOfBufferPages - 1].Records[pointers[_numberOfBufferPages-1]] = smallest;
                        pointers[_numberOfBufferPages-1]++;
                        pointers[pageIndexContainingLowestKey]++;

                        if (pointers[pageIndexContainingLowestKey] == _pageSizeInRecords)
                        {
                            // get the next page of the sortedrun, and start the count over.
                            _pool[pageIndexContainingLowestKey] = new Page(fieldIndexToSortBy, 
                                ReadPage(_iterationToSortedruns[_iteration - 1][sortedrunIndex + pageIndexContainingLowestKey].GetReader()));

                            pointers[pageIndexContainingLowestKey] = 0;
                        }
                    }

                    // completed another page, write it to the current sortedrun in the current iteration
                    _pool[_numberOfBufferPages - 1].WritePage(_iterationToSortedruns[_iteration][producedSortedruns].GetWriter());
                    pageCount++;
                    Console.WriteLine("Wrote page {0} of sorted run #{1}", pageCount, producedSortedruns);
                    // reset the buffer page to contains 0 records
                    _pool[_numberOfBufferPages - 1] = new Page(fieldCount, _pageSizeInRecords);
                    pointers[_numberOfBufferPages - 1] = 0;
                }
                _iteration++;
            }
            return _iterationToSortedruns.Last().Value.Last().Filename;
        }

        /// <summary>
        /// Adds all the records in all the pages from the pool and sorts them.
        /// </summary>
        /// <remarks>
        /// Purges the pool to conserve memory,
        /// GC collects on each page coppied.
        /// </remarks>
        /// <returns>
        /// A list of sorted records.
        /// </returns>
        private List<Record> GenerateSortedRun(int pageCount)
        {
            List<Record> allRecordsInPool = new List<Record>();
            for (int i = 0; i < pageCount; i++)
            {
                int j;
                // count the effective (non-null) records of this page
                for (j = 0; j < _pool[i].Records.Length && _pool[i].Records[j] != null; j++) ;
                // add the page's records into a flattened pool
                allRecordsInPool.AddRange(_pool[i].Records.Take(j));
                // and remove the page from the original pool to conserve memory
                _pool[i] = null;
                GC.Collect();
            }
            _pool = null;
            GC.Collect();

            allRecordsInPool.Sort();
            return allRecordsInPool;
        }

        /// <summary>
        /// Creates and initializes the basic structure for holding iterations data.
        /// Reads and sort each B pages from the input file
        /// and writes them as sortedruns to be used in the next iteration.
        /// </summary>
        /// <param name="filename">The input file full path</param>
        /// <param name="fieldIndexToSortBy">The field of each record, on which the sort will work</param>
        private void FirstIteration(string filename, int fieldIndexToSortBy)
        {
            double actualPages = Math.Ceiling(File.ReadAllLines(filename).Length / (double)_pageSizeInRecords);
            Debug.Assert(_numberOfBufferPages < actualPages, string.Format(
                "The number of buffer pages ({0}) must be smaller than the number of pages ({1}) that the file consumes!",
                _numberOfBufferPages, actualPages));

            _iterationToSortedruns = new Dictionary<int, List<SortedRun>>();           
            int pageCount = 0;
            int lineCount = 0;
            int sortedrunCount = 0;
            String[] records = new String[_pageSizeInRecords];
            List<Record> sortedPool;

            // read records from the input file and place sort them as pages
            using (StreamReader sr = File.OpenText(filename))
            {
                while (!sr.EndOfStream)
                {
                    records[lineCount++] = sr.ReadLine();

                    // check if enough records to fill a page
                    if (lineCount > 0 && lineCount % _pageSizeInRecords == 0)
                    {
                        _pool[pageCount] = new Page(fieldIndexToSortBy, records);
                        pageCount++;
                        records = new String[_pageSizeInRecords];
                        lineCount = 0;

                        // check if reached B pages
                        if (pageCount == _numberOfBufferPages)
                        {
                            Console.WriteLine("Read {0} pages from the disk to the memory", pageCount);
                            sortedPool = GenerateSortedRun(pageCount);
                            String tempFilename = Path.GetTempFileName();

                            // write the sorted pool into a sorted run file
                            for (int i = 0; i < pageCount; i++)
                            {
                                File.AppendAllLines(tempFilename, sortedPool.Skip(i * _pageSizeInRecords)
                                    .Take(_pageSizeInRecords)
                                    .Select(x => x.ToString()));
                                Console.WriteLine("Wrote page {0} of sorted run #{1}", i, sortedrunCount++);
                            }
                            if (!_iterationToSortedruns.ContainsKey(_iteration))
                            {
                                _iterationToSortedruns.Add(_iteration, new List<SortedRun>((int)Math.Ceiling(actualPages / _numberOfBufferPages)));
                            }
                            _iterationToSortedruns[_iteration].Add(new SortedRun() { Filename = tempFilename });
                            _pool = new Page[_numberOfBufferPages];
                            pageCount = 0;
                        }
                    }
                }
                sr.Close();
            }

            // check for remaining records and add them too
            if (records.First() != null)
            {
                _pool[pageCount] = new Page(fieldIndexToSortBy, records);
                pageCount++;
            }

            // check for remaining pages and write to disk as another sortedrun - the leftovers sortedrun.
            if (_pool.First() != null)
            {
                sortedPool = GenerateSortedRun(pageCount);
                String tempFilename = Path.GetTempFileName();
                File.AppendAllLines(tempFilename, sortedPool.Select(x => x.ToString()));
                Console.WriteLine("Wrote {0} pages of sorted run #{1}", pageCount, sortedrunCount++);
                _iterationToSortedruns[_iteration].Add(new SortedRun() { Filename = tempFilename });
            }

            // first iteration completed
            _iteration++;
        }

        /// <summary>
        /// Selection sort the B pages then write them to the disk as a sorted-run
        /// Finds the minimum each time and in-memory replaces it, cross-paging.
        /// Works at O(n²) runs on the input, recommended not to use!
        /// </summary>
        /// <param name="fieldIndexToSortBy"></param>
        /// <param name="pageCount"></param>
        private void SelectionSort(int fieldIndexToSortBy, int pageCount)
        {
            Record smallestRecord;
            int smallestIndex = 0;
            int smallestRecordPage = 0;
            int smallestRecordIndexInPage = 0;
            int position = 0;
            while (position < pageCount * _pageSizeInRecords)
            {
                smallestRecord = _pool[position / _pageSizeInRecords].Records[position % _pageSizeInRecords];
                // find the smallest record's position within the pages
                for (int i = position + 1; i < pageCount * _pageSizeInRecords; i++)
                {
                    int page = i / _pageSizeInRecords;
                    Record current = _pool[page].Records[i % _pageSizeInRecords];
                    if (current != null && current[fieldIndexToSortBy] < smallestRecord[fieldIndexToSortBy])
                    {
                        smallestRecord = current;
                        smallestIndex = i;
                        smallestRecordPage = page;
                        smallestRecordIndexInPage = i % _pageSizeInRecords;
                    }
                }
                if (smallestIndex > position)
                {
                    // Swap with the record at the current position
                    int currentPage = position / _pageSizeInRecords;
                    Record temp = _pool[currentPage].Records[position % _pageSizeInRecords];
                    _pool[currentPage].Records[position % _pageSizeInRecords] = smallestRecord;
                    _pool[smallestRecordPage].Records[smallestRecordIndexInPage] = temp;
                }
                position++;
            }
        }

        /// <summary>
        /// Reads a page-sized records from a file, or the whole file in case it is small.
        /// </summary>
        /// <param name="sr"></param>
        /// <returns>An array of strings containing the records.</returns>
        public String[] ReadPage(StreamReader sr)
        {
            int lineCount = 0;
            String[] records = new string[_pageSizeInRecords];
            while (!sr.EndOfStream)
            {
                records[lineCount++] = sr.ReadLine();
                if (lineCount > 0 && lineCount % _pageSizeInRecords == 0)
                {
                    return records;
                }
            }
            return records;
        }
    }
}