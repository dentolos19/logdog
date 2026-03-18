using System;
using System.IO;
using System.Text.Json;
using System.Collections.Generic;

class LogParser
{
    static void Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: LogParser <input_file> <output_file>");
            Environment.Exit(1);
        }

        string inputFile = args[0];
        string outputFile = args[1];

        if (!File.Exists(inputFile))
        {
            Console.Error.WriteLine($"Error: File not found: {inputFile}");
            Environment.Exit(1);
        }

        try
        {
            string ext = Path.GetExtension(inputFile).ToLower();
            string result = ext switch
            {
                ".evtx" => ParseEvtx(inputFile),
                ".evt" => ParseEvt(inputFile),
                ".bin" or ".dat" or ".blob" => ParseBinary(inputFile),
                _ => ParseBinary(inputFile)
            };

            File.WriteAllText(outputFile, result);
            Console.WriteLine($"Parsed: {inputFile} -> {outputFile}");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            Environment.Exit(1);
        }
    }

    static string ParseEvtx(string filePath)
    {
        var records = new List<object>();
        try
        {
            using (var reader = new System.Diagnostics.Eventing.Reader.EventLogReader(filePath))
            {
                System.Diagnostics.Eventing.Reader.EventRecord record;
                int count = 0;
                while ((record = reader.ReadEvent()) != null && count < 10000)
                {
                    records.Add(new
                    {
                        TimeCreated = record.TimeCreated,
                        Id = record.Id,
                        Level = record.LevelDisplayName,
                        ProviderName = record.ProviderName,
                        Message = record.FormatDescription() ?? ""
                    });
                    count++;
                }
            }
        }
        catch
        {
            return ParseBinary(filePath);
        }
        return JsonSerializer.Serialize(records, new JsonSerializerOptions { WriteIndented = false });
    }

    static string ParseEvt(string filePath)
    {
        return ParseBinary(filePath);
    }

    static string ParseBinary(string filePath)
    {
        var records = new List<string>();
        byte[] buffer = File.ReadAllBytes(filePath);

        // Convert to hex chunks
        for (int i = 0; i < buffer.Length; i += 256)
        {
            int len = Math.Min(256, buffer.Length - i);
            string hex = BitConverter.ToString(buffer, i, len);
            records.Add(hex.Replace("-", ""));
        }

        var output = new
        {
            file = Path.GetFileName(filePath),
            size = buffer.Length,
            chunks = records
        };

        return JsonSerializer.Serialize(output, new JsonSerializerOptions { WriteIndented = false });
    }
}
