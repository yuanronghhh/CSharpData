using System;
using System.Collections.Generic;
using System.Linq;
using Aspose.Cells;
using CommonLib.SQLTablePackage;
using CommonLib.TableBasePackage;
using Dapper;

namespace CommonLib.DatabaseClient
{
    public struct CellRange
    {
        public CellRange(int startRow = 0, int startCol = 0, int? endRow = -1, int? endCol = -1)
        {
            StartRow = startRow;
            StartCol = startCol;
            EndRow = endRow;
            EndCol = endCol;
        }

        public int StartRow;
        public int StartCol;
        public int? EndRow;
        public int? EndCol;
    }

    public class ExcelHeader
    {
        public ExcelHeader(string name, int index)
        {
            this.Name = name;
            this.Index = index;
        }

        public string Name { get; set; }

        public int Index { get; set; }
    }

    public interface IExcelTableBase: ITableBase
    {
    }

    public abstract class ExcelTableBase : IDisposable
    {
        public Workbook book = null;
        public Worksheet sheet = null;
        public List<ExcelHeader> header = new List<ExcelHeader>();
        public CellRange sheetRange = new CellRange();

        public ExcelTableBase(string path)
        {
            book = GetConnection(path);
            if(book == null || book.Worksheets == null || book.Worksheets.Count == 0) { return; }
        }

        public virtual void Dispose()
        {
            if(book != null)
            {
                book.Dispose();
            }
        }

        public void Commit()
        {
            sheet.CloseAccessCache(AccessCacheOptions.All);
            book.Save(book.FileName);
        }

        public void ChangeSheet(string sheetName, CellRange? range = null)
        {
            if (sheetName == null) {  return; }
            if (sheet != null)
            {
                if (sheet.Name == sheetName) { return; }
                sheet.CloseAccessCache(AccessCacheOptions.All);
                sheet = book.Worksheets[sheetName];
            }
            else
            {
                sheet = book.Worksheets[sheetName];
            }

            if (sheet == null)
            {
                sheet = book.Worksheets.Insert(book.Worksheets.Count + 1, SheetType.Worksheet, sheetName);
                book.Worksheets.RemoveAt("Evaluation Warning");

                sheet.StartAccessCache(AccessCacheOptions.All);
            }

            if (range.HasValue)
            {
                SetSheetRange(range.Value.StartRow, range.Value.StartCol, range.Value.EndRow, range.Value.EndCol);
            }
            else
            {
                SetSheetRange(0, 0, sheet.Cells.MaxDataRow, sheet.Cells.MaxDataColumn);
            }
        }

        /// Set range with header
        public void SetSheetRange(int srow, int scol, int? erow = null, int ? ecol = null)
        {
            if(srow < 0 || scol < 0 || erow < -1 || ecol < -1) { return; }

            sheetRange.StartRow = srow < sheet.Cells.MaxDataRow ? srow : 0;
            sheetRange.StartCol = scol < sheet.Cells.MaxDataColumn ? scol : 0;
            sheetRange.EndRow = GetMaxOrValue(sheet.Cells.MaxDataRow, erow);
            sheetRange.EndCol = GetMaxOrValue(sheet.Cells.MaxDataColumn, ecol); 
        }

        public int GetMaxOrValue(int max, int? val)
        {
            return val.HasValue ? (val.Value > -1) && val.Value < max ? val.Value : max : max;
        }

        public virtual Workbook GetConnection(string connString)
        {
            if (book == null)
            {
                if (string.IsNullOrWhiteSpace(connString))
                {
                    return null;
                }

                book = new Workbook(connString);
            }

            return book;
        }

        public List<ExcelHeader> GetHeader(Cells cs, int startRow, int startCol, int max)
        {
            List<ExcelHeader> header = new List<ExcelHeader>();
            for (int i = 0; i <= max; i++)
            {
                Cell c = cs[startRow, startCol + i];
                if(c == null || c.Value == null) { continue; }

                header.Add(new ExcelHeader(c.Value.ToString(), i));
            }

            if(header.Count == 0) { return null; }

            return header;
        }

        public List<Dictionary<string, object>> GetItemDictList(string sheetName, CellRange? range = null, List<string> columns = null)
        {
            range = range ?? new CellRange(sheetRange.StartRow, sheetRange.StartCol);
            ChangeSheet(sheetName);
            return GetItemDictListInner(range.Value, columns);
        }

        public bool IsRowInRange(int row, CellRange range)
        {
            if (row < range.StartRow || row < sheetRange.StartRow) { return false; }
            if (range.EndRow != -1 && (row > range.EndRow || row > sheetRange.EndRow)) { return false; }

            return true;
        }

        public bool IsColInRange(int col, CellRange range)
        {
            if (col < range.StartCol || col < sheetRange.StartCol) { return false; }
            if (range.EndCol != -1 && (col > range.EndCol || col > sheetRange.EndCol)) { return false; }

            return true;
        }

        private List<Dictionary<string, object>> GetItemDictListInner(CellRange range, List<string> columns = null)
        {
            Cells cs = sheet.Cells;
            header = GetHeader(cs, sheetRange.StartRow, sheetRange.StartCol, cs.MaxDataColumn);

            List<Dictionary<string, object>> list = new List<Dictionary<string, object>>();
            for (int i = sheetRange.StartRow; i <= sheetRange.EndRow; i++)
            {
                if (!IsRowInRange(i, range) || sheetRange.StartRow == i) { continue; }

                Dictionary<string, object> obj = new Dictionary<string, object>();

                for (int j = sheetRange.StartCol; j <= sheetRange.EndCol; j++)
                {
                    if (!IsColInRange(j, range)) { continue; }

                    Cell c = cs[i, j];
                    string name = header[j].Name;

                    if (columns != null && !columns.Contains(name)) { continue; }

                    obj.Add(name, c.Value);
                }

                list.Add(obj);
            }

            return list;
        }

        public bool InsertItemDict(string sheetName, Dictionary<string, object> data, List<string> columns = null, int startRow = 0)
        {
            ChangeSheet(sheetName);
            columns = columns ?? TableClass.GetTableNamesDict(data);

            return InsertItemDictInner(data, columns, startRow);
        }

        private bool InsertItemDictInner(Dictionary<string, object> data, List<string> insertColumns, int? startRow = null, List<string> wHeader = null)
        {
            Cells cs = sheet.Cells;
            header = GetHeader(cs, sheetRange.StartRow, sheetRange.StartCol, cs.MaxDataColumn);
            startRow = startRow ?? sheetRange.EndRow;

            for (int j = 0; j < insertColumns.Count; j++)
            {
                string name = insertColumns[j];
                ExcelHeader hd = header.Find(h => h.Name == name);
                if(hd == null) { continue; }
                int i = hd.Index;
                if (i == -1) { break; }

                Cell c = cs[startRow.Value + 1, i];

                c.PutValue(data[name]);
            }

            return true;
        }

        public bool InsertItemDict(string sheetName, Dictionary<string, object> data)
        {
            List<string> cols = TableClass.GetTableNamesDict(data);

            return InsertItemDict(sheetName, data, cols);
        }
    }

    public abstract class ExcelClientBase : ExcelTableBase
    {
        public ExcelClientBase(string path = null) : base(path)
        {
        }
    }
}