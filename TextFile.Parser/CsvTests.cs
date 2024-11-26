using BenchmarkDotNet.Attributes;
using System.Formats.Asn1;
using System.Globalization;
using System.Xml.XPath;
using ClosedXML.Excel;
using CsvHelper;
using CsvHelper.Configuration;

namespace TextFile.Parser;

public class CsvTests
{
    public class SortingBenchmark
    {
        private string? _folder = @"D:\\largefiletext\\";

        public void SetInputFilePath(string inputFile)
        {
            InputFile = inputFile;
            _folder = Path.GetDirectoryName(inputFile);
        }

        private static string InputFile = @"D:\\largefiletext\\input_file_2024112549_1.txt";
        private string CsvOutputFile => Path.Combine(_folder, "csv_sorted_output.txt");
        private string ExcelOutputFile => Path.Combine(_folder, "excel_sorted_output.txt");

        private const string TempExcelFile = "temp.xlsx";

        //[Benchmark]
        public void CsvHelperBenchmark()
        {
            SortCsvFile(InputFile, CsvOutputFile);
        }

        [Benchmark]
        public void ClosedXmlBenchmark()
        {
            ConvertTextToExcel(InputFile, TempExcelFile);
            SortExcelFile(TempExcelFile);
            ConvertExcelToText(TempExcelFile, ExcelOutputFile);
            File.Delete(TempExcelFile);
        }

        static void SortCsvFile(string inputFile, string outputFile)
        {
            var records = new List<Record>();

            using (var reader = new StreamReader(inputFile))
            using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = ". ",
                HasHeaderRecord = false
            }))
            {
                records = new List<Record>(csv.GetRecords<Record>());
            }

            records.Sort((x, y) =>
            {
                int textComparison = string.Compare(x.Text, y.Text, StringComparison.Ordinal);
                return textComparison != 0 ? textComparison : x.Number.CompareTo(y.Number);
            });

            using (var writer = new StreamWriter(outputFile))
            using (var csv = new CsvWriter(writer, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = ". ",
                HasHeaderRecord = false
            }))
            {
                csv.WriteRecords(records);
            }
        }

        static void ConvertTextToExcel(string inputFile, string excelFile)
        {
            using (var workbook = new XLWorkbook())
            {
                var worksheet = workbook.Worksheets.Add("Sheet1");
                using (var reader = new StreamReader(inputFile))
                {
                    string line;
                    int row = 1;
                    int sheetNumber = 1;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (row > 1048576)
                        {
                            sheetNumber++;
                            worksheet = workbook.Worksheets.Add($"Sheet{sheetNumber}");
                            row = 1;
                        }

                        var parts = line.Split(new[] { ". " }, 2, StringSplitOptions.None);
                        if (parts.Length == 2 && int.TryParse(parts[0], out int number))
                        {
                            worksheet.Cell(row, 1).Value = number;
                            worksheet.Cell(row, 2).Value = parts[1];
                            row++;
                        }
                    }
                }
                workbook.SaveAs(excelFile);
            }
        }

        static void SortExcelFile(string excelFile)
        {
            using (var workbook = new XLWorkbook(excelFile))
            {
                foreach (var worksheet in workbook.Worksheets)
                {
                    var range = worksheet.RangeUsed();
                    range.Sort("B1", XLSortOrder.Ascending)
                         .Sort("A1", XLSortOrder.Ascending);
                }
                workbook.Save();
            }
        }

        static void ConvertExcelToText(string excelFile, string outputFile)
        {
            using (var workbook = new XLWorkbook(excelFile))
            {
                using (var writer = new StreamWriter(outputFile))
                {
                    foreach (var worksheet in workbook.Worksheets)
                    {
                        foreach (var row in worksheet.RowsUsed())
                        {
                            int number = row.Cell(1).GetValue<int>();
                            string text = row.Cell(2).GetValue<string>();
                            writer.WriteLine($"{number}. {text}");
                        }
                    }
                }
            }
        }
    }

    public class Record
    {
        public int Number { get; set; }
        public string Text { get; set; }
    }
}
