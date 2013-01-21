using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;

namespace ExternalSort
{
    /// <summary>
    /// The main external sort class.
    /// Uses an iteration system that contains sortedruns produced in each iteration.
    /// Manages a pool of pages - the buffer pool,
    /// by providing B-1 buffer pages for input and one output page.
    /// <remarks>The process will create sorted runs in the system's temporary directory.</remarks>
    /// </summary>
    public class ExternalSort
    {
        private int _numberOfBufferPages;
        private int _pageSizeInRecords;
        private int _numberOfFieldsInRecord;
        private int _fieldToSortBy;
        private int _iteration;
        private Page[] _pool;
        private int[] _pointers;
        private Dictionary<int, List<SortedRun>> _iterationToSortedruns;
        private RecordComparer _recordComparer;
        private int _sortedrunsIndexInLastIteration;
        private int _sortedrunsUsedInCurrentIteration;
        private int _producedSortedruns;
        private int _pageCount = 0;

        public ExternalSort(int bufferPages, int pageSizeInRecords, int fieldCount, int fieldToSortBy)
        {
            Debug.Assert(bufferPages > 1, "Buffer pages must more than one page!");
            _numberOfBufferPages = bufferPages;
            _pageSizeInRecords = pageSizeInRecords;
            _numberOfFieldsInRecord = fieldCount;
            _fieldToSortBy = fieldToSortBy;
            _pool = new Page[_numberOfBufferPages];
            _recordComparer = new RecordComparer(fieldToSortBy);
        }

        public string Sort(string filename, int fieldCount)
        {
            FirstIteration(filename);
            while (_iterationToSortedruns[_iteration - 1].Count > 1)
            {
                Console.WriteLine("\nCommencing iteration {0} \n", _iteration);
                InitializeNewIteration();
                bool finishedMergingSet = false;
                
                // while there are still sortedruns left in previous iteration
                while (!finishedMergingSet)
                {
                    finishedMergingSet = IsPoolsEmpty();
                    if (finishedMergingSet)
                    {
                        ReadNextPages();

                        // check that some sorted runs were read
                        if (_sortedrunsIndexInLastIteration == _sortedrunsUsedInCurrentIteration)
                        {
                            continue;
                        }

                        // if any of the pages read have records, continue
                        if (_pool.Take(_numberOfBufferPages - 1).Any(item => item != null && item.Records != null))
                            finishedMergingSet = false;

                        if (finishedMergingSet)
                            continue;

                        // there're more sorted runs, prepare a new merged sortedrun
                        CreateSortedRun();
                        _pageCount = 0;
                    }
                    ProduceOneMergedPage();

                    // completed another page, write it to the current sortedrun in the current iteration
                    _pool[_numberOfBufferPages - 1].WritePage(_iterationToSortedruns[_iteration][_producedSortedruns].GetWriter());
                    _pageCount++;
                    Console.WriteLine("Wrote page {0} of sorted run #{1}", _pageCount, _producedSortedruns);
                    // reset the buffer page to contains 0 records
                    _pool[_numberOfBufferPages - 1] = new Page(fieldCount, _pageSizeInRecords);
                    _pointers[_numberOfBufferPages - 1] = 0;
                }
                _iteration++;
            }
            return _iterationToSortedruns.Last().Value.Last().Filename;
        }

        private void CreateSortedRun()
        {
            String tempFilename = Path.GetTempFileName();
            _iterationToSortedruns[_iteration].Add(new SortedRun() { Filename = tempFilename });
        }

        private void ReadNextPages()
        {
            // close the latest sortedrun written
            _iterationToSortedruns[_iteration][_producedSortedruns++].Writer.Close();

            // load the first page from B-1 sortedruns of the previous iteration
            // continuing from the next sortedrun which was not read
            _sortedrunsIndexInLastIteration = _sortedrunsUsedInCurrentIteration;
            for (int i = _sortedrunsUsedInCurrentIteration; i < _iterationToSortedruns[_iteration - 1].Count; i++)
            {
                if (i - _sortedrunsIndexInLastIteration == (_numberOfBufferPages - 1)) // Use up to B-1 sortedruns
                {
                    break;
                }
                _pool[i - _sortedrunsIndexInLastIteration] = new Page(ReadPage(_iterationToSortedruns[_iteration - 1][i].GetReader(), _pageSizeInRecords));
                _sortedrunsUsedInCurrentIteration++;
            }
        }

        private void ProduceOneMergedPage()
        {
            Record smallest = null;
            // while the buffer page doesn't exceed
            while (_pointers[_numberOfBufferPages - 1] < _pageSizeInRecords)
            {
                // initialize the lowest records
                smallest = null;
                int firstPageThatIsNotEmpty = 0;
                // find the first page that is not empty and assign the first record
                for (int i = 0; i < _numberOfBufferPages; i++)
                {
                    if (_pool[i] != null && _pool[i].Records != null && _pool[i].Records[_pointers[i]] != null)
                    {
                        // choose the first non-null record as the smallest
                        smallest = new Record(_pool[i].Records[_pointers[i]]);
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
                        && _pool[i].Records[_pointers[i]] != null
                        && _pool[i].Records[_pointers[i]].Fields[_fieldToSortBy] < smallest[_fieldToSortBy])
                    {
                        smallest = _pool[i].Records[_pointers[i]];
                        pageIndexContainingLowestKey = i;
                    }
                }

                // insert the smallest record into the buffer page and increase the page's pointer
                _pool[_numberOfBufferPages - 1].Records[_pointers[_numberOfBufferPages - 1]] = smallest;
                _pointers[_numberOfBufferPages - 1]++;
                _pointers[pageIndexContainingLowestKey]++;

                if (_pointers[pageIndexContainingLowestKey] == _pageSizeInRecords)
                {
                    // get the next page of the sortedrun
                    _pool[pageIndexContainingLowestKey] = new Page(
                        ReadPage(_iterationToSortedruns[_iteration - 1][_sortedrunsIndexInLastIteration + pageIndexContainingLowestKey].GetReader(), _pageSizeInRecords));
                    // and reset its pointer
                    _pointers[pageIndexContainingLowestKey] = 0;
                }
            }
        }

        private bool IsPoolsEmpty()
        {
            bool empty = true;
            for (int i = 0; i < _numberOfBufferPages - 1; i++)
            {
                // check that there aren't any pages containing records
                if (_pool[i] != null && _pool[i].Records != null && _pool[i].Records[_pointers[i]] != null)
                {
                    // there's a page containing a record
                    empty = false;
                    break;
                }
            }
            return empty;
        }

        private void InitializeNewIteration()
        {
            _sortedrunsIndexInLastIteration = 0;
            _sortedrunsUsedInCurrentIteration = 0;
            _pool = new Page[_numberOfBufferPages];

            // load the first page of each of the B-1 sortedruns from the last iteration 
            // and merge sort them using the last page in the buffer pool
            for (int i = 0; i < _iterationToSortedruns[_iteration - 1].Count; i++)
            {
                if (i == _numberOfBufferPages - 1) // Use only B-1 sortedruns
                {
                    _sortedrunsUsedInCurrentIteration = i;
                    break;
                }
                _pool[i] = new Page(ReadPage(_iterationToSortedruns[_iteration - 1][i].GetReader(), _pageSizeInRecords));
            }

            // point to current index in each sortedrun and also the buffer page
            _pointers = new int[_numberOfBufferPages];
            // reset the buffer page to contains 0 records
            _pool[_numberOfBufferPages - 1] = new Page(_numberOfFieldsInRecord, _pageSizeInRecords);

            // initialize the current iteration's pages
            _producedSortedruns = 0;
            _iterationToSortedruns[_iteration] = new List<SortedRun>();
            CreateSortedRun();
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

            allRecordsInPool.Sort(_recordComparer);
            return allRecordsInPool;
        }

        /// <summary>
        /// Creates and initializes the basic structure for holding iterations data.
        /// Reads and sort each B pages from the input file
        /// and writes them as sortedruns to be used in the next iteration.
        /// </summary>
        /// <param name="filename">The input file full path</param>
        private void FirstIteration(string filename)
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
                        _pool[pageCount] = new Page(records);
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
                _pool[pageCount] = new Page(records);
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
        private static String[] ReadPage(StreamReader sr, int numberOfRecords)
        {
            int lineCount = 0;
            String[] records = new string[numberOfRecords];
            while (!sr.EndOfStream)
            {
                records[lineCount++] = sr.ReadLine();
                if (lineCount > 0 && lineCount % numberOfRecords == 0)
                {
                    return records;
                }
            }
            return records;
        }
    }
}