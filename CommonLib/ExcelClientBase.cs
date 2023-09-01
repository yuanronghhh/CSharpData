using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using Aspose.Cells;
using Commonlib.Reflection;
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
                //book.Dispose();
            }
        }

        public void Commit()
        {
            //sheet.CloseAccessCache(AccessCacheOptions.All);
            book.Save(book.FileName);
        }

        public bool ChangeSheet(string sheetName, CellRange? range = null, bool insertNew = false)
        {
            if (sheetName == null) {  return false; }
            if (sheet != null)
            {
                if (sheet.Name == sheetName) { return true; }
                sheet = book.Worksheets[sheetName];
            }
            else
            {
                sheet = book.Worksheets[sheetName];
            }

            if (sheet == null)
            {
                if (insertNew) {
                    sheet = book.Worksheets.Insert(book.Worksheets.Count, SheetType.Worksheet, sheetName);

                } else {
                    throw new Exception("Not Find excel sheet info");
                }
            }

            if (range.HasValue)
            {
                SetSheetRange(range.Value.StartRow, range.Value.StartCol, range.Value.EndRow, range.Value.EndCol);
            }
            else
            {
                SetSheetRange(0, 0, sheet.Cells.MaxDataRow, sheet.Cells.MaxDataColumn);
            }

            return true;
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
                Cell c = cs[startRow - 1, startCol + i];
                if(c == null || c.Value == null) { continue; }

                header.Add(new ExcelHeader(c.Value.ToString(), i));
            }

            if(header.Count == 0) { return null; }

            return header;
        }

        public List<Dictionary<string, object>> GetItemDictList(string sheetName, CellRange? range = null, List<string> columns = null)
        {
            range = range ?? new CellRange(sheetRange.StartRow, sheetRange.StartCol);
            if (!ChangeSheet(sheetName, range))
            {
                return null;
            }

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
            if(!ChangeSheet(sheetName)) { return false; }

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

        public static bool TryConvert(string ev, PropertyInfo p, out object v)
        {
            TypeConverter tc = TypeDescriptor.GetConverter(p.PropertyType);
            v = null;

            if (tc == null)
            {
                return false;
            }

            try
            {
                v = tc.ConvertFromInvariantString(ev);
                return true;
            }
            catch (Exception err)
            {
                return false;
            }
        }

        public List<T> ExportExcelSheetUploadData<T>(Workbook wb, string sheetName, string[] header, int startRow, int startColumn, out string error)
        {
            List<T> list;
            error = "成功";

            Worksheet ws = null;
            if (wb.Worksheets.Count == 0)
            {
                error = "没有Excel表信息";
                return null;
            }

            if (!string.IsNullOrWhiteSpace(sheetName))
            {
                ws = wb.Worksheets[sheetName];
            }
            else
            {
                ws = wb.Worksheets[0];
            }

            if (ws == null)
            {
                error = "没有找到Excel表Sheet信息。";
                return null;
            }

            Cells cells = ws.Cells;
            string h;
            object v;
            string ev;
            PropertyInfo p;
            int dLen = cells.Rows.Count - startRow;
            list = new List<T>();

           

            for (int i = 0; i < dLen; i++)
            {
                 T d = Activator.CreateInstance<T>();
                for (int j = 0; j < header.Length; j++)
                {
                    h = header[j];
                    if (string.IsNullOrWhiteSpace(h))
                    {
                        continue;
                    }

                    Dictionary<string, object> nd = d as Dictionary<string, object>;
                    if(typeof(T) == typeof(Dictionary<string, object>))
                    {
                        v = cells[startRow + i, startColumn + j].Value;
                        nd[h] = v;
                    }
                    else
                    {
                        p = ReflectionCommon.GetProperty<T>(h);
                        if (p == null) { continue; }
                        ev = cells[startRow + i, startColumn + j].StringValue;
                        ev = ev == "/" ? "" : ev;
                        if (!TryConvert(ev, p, out v))
                        {
                            error = string.Format(@"请检查{0}行, {1}列, {2} 是否正确填写。", i + startRow + 1, startColumn + j + 1, ev);
                            return null;
                        }

                        ReflectionCommon.SetValue(d, h, v);
                    }
                }

                list.Add(d);
            }

            return list;
        }

        public void FillExcelTable<T>(List<T> list, List<string> columns, Cells cells, int startRow, int startCol)
        {
            if (list == null || !list.Any())
            {
                return;
            }

            for (int listRow = 0; listRow < list.Count(); listRow++)
            {
                var info = list[listRow];
                for (int i = 0; i < columns.Count(); i++)
                {
                    var value = ReflectionCommon.GetValue(info, columns[i]);
                    cells[startRow + listRow, startCol + i].PutValue(value);
                    cells[startRow + listRow, startCol + i].SetStyle(cells[startRow, startCol + i].GetStyle());
                    cells.SetRowHeight(startRow + listRow, cells.GetRowHeight(startRow));
                }
            }
        }
    }

    public abstract class ExcelClientBase : ExcelTableBase
    {
        public ExcelClientBase(string path = null) : base(path)
        {
        }
    }
}