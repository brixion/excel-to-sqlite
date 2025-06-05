<?php

namespace Brixion\ExcelToSqlite;

use OpenSpout\Reader\XLSX\Options;
use OpenSpout\Reader\XLSX\Reader;

class Converter
{
    private \SQLite3 $db;
    private string $inputFile;
    private string $inputExtension;

    /** @var string[] */
    private array $acceptedExtensions = ['xlsx', 'xls'];

    /**
     * Converter constructor.
     *
     * @param string $inputFile the path to the input Excel file
     * @param string $outputPath the path to the output directory where the SQLite database will be created
     * @param bool $destroyDb optional, if true, it will destroy the database in the destruct function; default is false
     * @param string|null $inputExtension optional, the file extension of the input file; if null, it will be determined from the file name
     *
     * @throws \InvalidArgumentException if the input file does not exist, or if the output path is not a writable directory, or if the input extension is unsupported
     */
    public function __construct(string $inputFile, string $outputPath, bool $destroyDb = false, ?string $inputExtension = null)
    {
        if (!is_dir($outputPath) || !is_writable($outputPath)) {
            throw new \InvalidArgumentException("Output path is not a writable directory: " . $outputPath);
        }

        $dbFile = $outputPath . '/' . pathinfo($inputFile, PATHINFO_FILENAME) . '.sqlite';

        $this->changeInputFile($inputFile, $inputExtension);
        $this->db = new \SQLite3($dbFile);

        if ($destroyDb) {
            register_shutdown_function(static fn() => @unlink($dbFile));
        }
    }

    /** 
     * Add a new input file to the converter so it can be added to the same SQLite database.
     * This method allows changing the input file and its extension dynamically.
     * 
     * @param string $inputFile the path to the new input file
     * @param string|null $inputExtension the file extension of the new input file; if null, it will be determined from the file name
     * 
     * @throws \InvalidArgumentException if the input file does not exist, or if the input extension is unsupported
     */
    public function changeInputFile(string $inputFile, string $inputExtension): void
    {
        if (!file_exists($inputFile)) {
            throw new \InvalidArgumentException("Input file does not exist: " . $inputFile);
        }

        if (null === $inputExtension) {
            $inputExtension = pathinfo($inputFile, PATHINFO_EXTENSION);
        }

        // filter the input extension to lowercase letters only
        $inputExtension = preg_replace('/[^a-z]/', '', strtolower($inputExtension));

        if (!in_array($inputExtension, $this->acceptedExtensions)) {
            throw new \InvalidArgumentException("Unsupported file extension: " . $inputExtension);
        }

        $this->inputFile = $inputFile;
        $this->inputExtension = $inputExtension;
    }

    /**
     * Gets the input file path.
     *
     * @return string the path to the input file
     */
    public function getInputFile(): string
    {
        return $this->inputFile;
    }

    /**
     * Converts the input file to an SQLite database.
     *
     * This method determines the type of the input file based on its extension and calls the appropriate conversion method.
     *
     * @throws \Exception if there is an error during conversion
     */
    public function convert(): void
    {
        try {
            switch ($this->inputExtension) {
                case 'csv':
                case 'txt':
                    $this->convertText();
                    break;
                case 'xlsx':
                case 'xls':
                    $this->convertExcel();
                    break;
                default:
                    throw new \InvalidArgumentException("Unsupported file extension: " . $this->inputExtension);
            }
        } catch (\Exception $e) {
            throw new \Exception("Error during conversion: " . $e->getMessage(), 0, $e);
        }
    }

    public function __destruct()
    {
        $this->db->close();
    }

    /**
     * Converts the input text file to an SQLite database.
     *
     * This method reads the text file, creates a table in the SQLite database, and inserts the data from each line.
     *
     * @throws \Exception if there is an error during reading or writing to the database
     */
    private function convertText(): void
    {
        $file = fopen($this->inputFile, 'r');
        if (!$file) {
            throw new \Exception("Could not open file: " . $this->inputFile);
        }

        $bom = fread($this->fileHandle, 4);
        rewind($this->fileHandle);

        // Handle UTF-16 BOM and convert to UTF-8
        if ("\xFF\xFE" === substr($bom, 0, 2) || "\xFE\xFF" === substr($bom, 0, 2)) {
            stream_filter_append($this->fileHandle, 'convert.iconv.UTF-16/UTF-8//IGNORE');
        }

        $firstLine = fgets($file);
        if (false !== $firstLine) {
            $delimiter = $this->detectDelimiter($firstLine);
        } else {
            throw new \RuntimeException('Failed to read first line of file');
        }
        rewind($file);

        $tableName = pathinfo($this->inputFile, PATHINFO_FILENAME);
        // first line is the header
        $header = fgetcsv($file, 8192, $delimiter);
        if ($header === false) {
            throw new \RuntimeException('Failed to read header line of file');
        }

        $this->db->exec("CREATE TABLE IF NOT EXISTS " . $tableName . " (id INTEGER PRIMARY KEY, " . implode(", ", array_map(fn($col) => $col . " TEXT", $header)) . ")");

        $stmt = $this->db->prepare("INSERT INTO " . $tableName . " (" . implode(", ", $header) . ") VALUES (" . implode(", ", array_fill(0, count($header), '?')) . ")");
        while (($row = fgetcsv($file, 8192, $delimiter)) !== false) {
            foreach ($row as $index => $value) {
                $stmt->bindValue($index + 1, $this->convertToString($value), SQLITE3_TEXT);
            }
            $stmt->execute();
        }

        fclose($file);
    }

    /**
     * Converts the input Excel file to an SQLite database.
     *
     * This method reads the Excel file, creates tables in the SQLite database based on the sheet names,
     * and inserts the data from each row into the corresponding table.
     *
     * @throws \Exception if there is an error during reading or writing to the database
     */
    private function convertExcel(): void
    {
        $options = new Options();
        $options->SHOULD_PRESERVE_EMPTY_ROWS = true;
        $reader = new Reader($options);
        $reader->open($this->inputFile);

        foreach ($reader->getSheetIterator() as $sheet) {
            $tableName = pathinfo($this->inputFile, PATHINFO_FILENAME) . '_' . $sheet->getName();
            foreach ($sheet->getRowIterator() as $currentRow => $row) {
                if ($currentRow === 1) {
                    $this->db->exec("CREATE TABLE IF NOT EXISTS " . $tableName . " (id INTEGER PRIMARY KEY, " . implode(", ", array_map(fn($cell) => $cell->getValue(), $row->getCells())) . ")");
                } else {
                    $values = array_map(fn($cell) => $cell->getValue(), $row->getCells());
                    $placeholders = implode(", ", array_fill(0, count($values), '?'));
                    $stmt = $this->db->prepare("INSERT INTO " . $tableName . " VALUES (NULL, " . $placeholders . ")");
                    foreach ($values as $index => $value) {
                        $stmt->bindValue($index + 1, $this->convertToString($value), SQLITE3_TEXT);
                    }
                    $stmt->execute();
                }
            }
        }

        $reader->close();
    }

    /**
     * Detects the delimiter used in a line of text.
     *
     * This method checks for common delimiters (comma, semicolon, tab, pipe) and returns the one that appears most frequently.
     *
     * @param string $line the line of text to analyze
     *
     * @return string the detected delimiter
     */
    private function detectDelimiter(string $line): string
    {
        $delimiters = [',', ';', "\t", '|'];
        $counts = [];
        foreach ($delimiters as $delimiter) {
            $counts[$delimiter] = substr_count($line, $delimiter);
        }
        arsort($counts);

        return key($counts);
    }

    /**
     * Converts a value to a string, handling null and empty values.
     *
     * @param mixed $value the value to convert
     *
     * @return string|null the converted value or null if the value is empty
     */
    private function convertToString(mixed $value): ?string
    {
        if (null === $value || '' === $value || [] === $value) {
            return null;
        }

        if ($value instanceof \DateTimeImmutable || $value instanceof \DateTime) {
            return $value->format('Y-m-d H:i:s');
        }

        if (\is_array($value)) {
            $value = implode(' ', $value);
        }

        return (string) $value;
    }
}
