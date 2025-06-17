<?php

namespace Brixion\ExcelToSqlite;

use OpenSpout\Common\Entity\Cell;
use OpenSpout\Reader\XLSX\Options;
use OpenSpout\Reader\XLSX\Reader;

class Converter
{
    private \SQLite3 $db;
    private string $inputFile;
    private string $inputExtension;

    private int $keyRow = 1;

    private ?string $tablePrefix = null;

    /** @var string[] */
    private array $acceptedExtensions = ['txt', 'csv', 'xlsx', 'xls'];

    /**
     * Converter constructor.
     *
     * @param string      $outputFile     the path to the output file where the SQLite database will be created
     * @param string|null $inputFile      the path to the input Excel file
     * @param bool        $destroyDb      optional, if true, it will destroy the database in the destruct function; default is false
     * @param string|null $inputExtension optional, the file extension of the input file; if null, it will be determined from the file name
     *
     * @throws \InvalidArgumentException if the input file does not exist, or if the output path is not a writable directory, or if the input extension is unsupported
     */
    public function __construct(
        string $outputFile,
        bool $destroyDb = false,
        ?string $inputFile = null,
        ?string $inputExtension = null,
    ) {
        if (!is_writable(\dirname($outputFile))) {
            throw new \InvalidArgumentException('Output path is not a writable directory: '.$outputFile);
        }

        if (null !== $inputFile) {
            $this->changeInputFile($inputFile, $inputExtension);
        }

        $this->db = new \SQLite3($outputFile);

        if ($destroyDb) {
            register_shutdown_function(static fn () => @unlink($outputFile));
        }
    }

    /**
     * Add a new input file to the converter so it can be added to the same SQLite database.
     * This method allows changing the input file and its extension dynamically.
     *
     * @param string      $inputFile      the path to the new input file
     * @param string|null $inputExtension the file extension of the new input file; if null, it will be determined from the file name
     *
     * @throws \InvalidArgumentException if the input file does not exist, or if the input extension is unsupported
     */
    public function changeInputFile(string $inputFile, ?string $inputExtension = null, ?string $tablePrefix = null, ?int $keyRow = null): void
    {
        if (!file_exists($inputFile)) {
            throw new \InvalidArgumentException('Input file does not exist: '.$inputFile);
        }

        if (!is_readable($inputFile)) {
            throw new \InvalidArgumentException('Input file is not readable: '.$inputFile);
        }

        if (null !== $tablePrefix) {
            $this->tablePrefix = $tablePrefix;
        }

        $this->keyRow = null !== $keyRow ? $keyRow : 1;

        if (null === $inputExtension) {
            $inputExtension = pathinfo($inputFile, \PATHINFO_EXTENSION);
        }

        // filter the input extension to lowercase letters only
        $inputExtension = preg_replace('/[^a-z]/', '', strtolower($inputExtension));

        if (!\in_array($inputExtension, $this->acceptedExtensions)) {
            throw new \InvalidArgumentException('Unsupported file extension: '.$inputExtension);
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
                    throw new \InvalidArgumentException('Unsupported file extension: '.$this->inputExtension);
            }
        } catch (\Exception $e) {
            throw new \Exception('Error during conversion: '.$e->getMessage(), 0, $e);
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
            throw new \Exception('Could not open file: '.$this->inputFile);
        }

        $bom = fread($file, 4);
        rewind($file);

        // Handle UTF-16 BOM and convert to UTF-8
        if ("\xFF\xFE" === substr($bom, 0, 2) || "\xFE\xFF" === substr($bom, 0, 2)) {
            stream_filter_append($file, 'convert.iconv.UTF-16/UTF-8//IGNORE');
        }

        // Does not work with different keyRow, needs to be fixed if ever needed
        $firstLine = fgets($file);
        if (false !== $firstLine) {
            $delimiter = $this->detectDelimiter($firstLine);
        } else {
            throw new \RuntimeException('Failed to read first line of file');
        }
        rewind($file);

        $tableName = $this->tablePrefix.pathinfo($this->inputFile, \PATHINFO_FILENAME);
        // first line is the header
        $header = fgetcsv($file, 8192, $delimiter);
        if (false === $header) {
            throw new \RuntimeException('Failed to read header line of file');
        }

        $this->db->exec('CREATE TABLE IF NOT EXISTS '.$tableName.' (id INTEGER PRIMARY KEY, '.implode(', ', array_map(fn ($col) => $col.' TEXT', $header)).')');

        $stmt = $this->db->prepare('INSERT INTO '.$tableName.' ('.implode(', ', $header).') VALUES ('.implode(', ', array_fill(0, \count($header), '?')).')');
        while (($row = fgetcsv($file, 8192, $delimiter)) !== false) {
            foreach ($row as $index => $value) {
                $stmt->bindValue($index + 1, $this->convertToString($value), \SQLITE3_TEXT);
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
        $keys = [];

        foreach ($reader->getSheetIterator() as $sheet) {
            $tableName = $this->getTableName($sheet->getName());
            foreach ($sheet->getRowIterator() as $currentRow => $row) {
                if ($this->keyRow === $currentRow) {
                    $keys = $this->getCellsFromRow($row->getCells());
                    $this->db->exec('CREATE TABLE IF NOT EXISTS '.$tableName.' (id INTEGER PRIMARY KEY, '.implode(', ', $keys).')');
                }
                if ($this->keyRow >= $currentRow) {
                    continue;
                }
                $values = $this->makeRowsEqualToKeys(
                    array_map(
                        fn ($cell) => '' === str_replace(' ', '', $cell->getValue()) ? null : $cell->getValue(),
                        $row->getCells()
                    ),
                    $keys
                );
                $placeholders = implode(', ', array_fill(0, \count($values), '?'));
                $stmt = $this->db->prepare('INSERT INTO '.$tableName.' VALUES (NULL, '.$placeholders.')');
                foreach ($values as $index => $value) {
                    $stmt->bindValue($index + 1, $this->convertToString($value), \SQLITE3_TEXT);
                }
                $stmt->execute();
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

    /**
     * Generates a sanitized table name based on the sheet name.
     *
     * This method replaces any unwanted characters in the sheet name with underscores and prefixes it with the table prefix.
     *
     * @param string $sheetName the name of the sheet
     *
     * @return string the sanitized table name
     */
    private function getTableName(string $sheetName): string
    {
        return $this->tablePrefix.'_'.preg_replace('/[^a-zA-Z0-9_]/', '_', $sheetName);
    }

    /**
     * Strips unwanted characters from a string and replaces them with underscores.
     *
     * This method is used to sanitize strings, especially for table names and column names.
     *
     * @param string $string the string to sanitize
     *
     * @return string the sanitized string
     */
    private function getStrippedString(string $string): string
    {
        return preg_replace('/[^a-zA-Z0-9_]/', '_', $string);
    }

    /**
     * Extracts the cells from a row and converts them to strings.
     *
     * This method maps each cell in the row to a string, stripping unwanted characters and filtering out empty values.
     *
     * @param array<Cell> $row the row containing cells
     *
     * @return array<string> the processed cells as strings
     */
    private function getCellsFromRow(array $row): array
    {
        if (empty($row)) {
            return [];
        }
        $cells = array_map(
            fn (Cell $cell) => $this->getStrippedString($this->convertToString($cell->getValue())),
            $row
        );

        $cells = array_filter($cells, fn ($cell) => !empty($cell) && '_' !== $cell && '' !== trim($cell));

        return $cells;
    }

    /**
     * Makes the row equal to the keys by padding or slicing it.
     *
     * If the row has fewer elements than the keys, it pads the row with null values.
     * If the row has more elements than the keys, it slices the row to match the number of keys.
     *
     * @param array<string> $row  the row to modify
     * @param array<string> $keys the keys to match
     *
     * @return array<string> the modified row
     */
    private function makeRowsEqualToKeys(array $row, array $keys): array
    {
        if (\count($row) !== \count($keys)) {
            if (\count($row) < \count($keys)) {
                $row = array_pad($row, \count($keys), null);
            } else {
                $row = \array_slice($row, 0, \count($keys));
            }
        }

        return $row;
    }
}
