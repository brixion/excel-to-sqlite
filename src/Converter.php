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
    private array $acceptedExtensions = ['txt', 'csv', 'xlsx', 'xls', 'xaf'];

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
                case 'xaf':
                    $this->convertXaf();
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
                    $this->db->exec('CREATE TABLE IF NOT EXISTS `'.$tableName.'` (id INTEGER PRIMARY KEY, '.implode(', ', $keys).')');
                }
                if ($this->keyRow >= $currentRow) {
                    continue;
                }
                $values = $this->makeRowsEqualToKeys(
                    array_map(fn ($cell) => $cell->getValue(), $row->getCells()),
                    $keys
                );
                $placeholders = implode(', ', array_fill(0, \count($values), '?'));
                $stmt = $this->db->prepare('INSERT INTO `'.$tableName.'` VALUES (NULL, '.$placeholders.')');
                foreach ($values as $index => $value) {
                    $stmt->bindValue($index + 1, $this->convertToString($value), \SQLITE3_TEXT);
                }
                $stmt->execute();
            }
        }

        $reader->close();
    }

    /**
     * Converts the input XAF file to an SQLite database.
     *
     * This method reads XAF 3.x and 4.x files, parses the XML structure,
     * and creates tables based on the XAF data objects.
     *
     * @throws \Exception if there is an error during reading or writing to the database
     */
    private function convertXaf(): void
    {
        $xmlContent = file_get_contents($this->inputFile);
        if (false === $xmlContent) {
            throw new \Exception('Could not read XAF file: '.$this->inputFile);
        }

        // Load XML content
        $xml = new \DOMDocument();
        libxml_use_internal_errors(true);
        if (!$xml->loadXML($xmlContent)) {
            $errors = libxml_get_errors();
            $errorMessage = 'Failed to parse XAF XML: ';
            foreach ($errors as $error) {
                $errorMessage .= $error->message.' ';
            }
            throw new \Exception($errorMessage);
        }
        $xpath = new \DOMXPath($xml);

        // Register namespace for XAF files
        $xpath->registerNamespace('xaf', 'http://www.auditfiles.nl/XAF/3.1');

        $companies = $xpath->query('//xaf:company');

        if (0 === $companies->length) {
            throw new \Exception('No company elements found in XAF file');
        }

        // Process each company
        foreach ($companies as $company) {
            if (!$company instanceof \DOMElement) {
                continue;
            }
            // Process plural elements that contain multiple child elements within company
            $this->processXafPluralElements($xpath, $company);

            // Process transactions with special handling for trLine elements within company
            $this->processXafTransactions($xpath, $company);
        }
    }

    /**
     * Processes plural elements in XAF files and creates tables for them.
     *
     * @param \DOMXPath   $xpath   the XPath object for querying the XML
     * @param \DOMElement $company the company element to process within
     */
    private function processXafPluralElements(\DOMXPath $xpath, \DOMElement $company): void
    {
        // Find all elements within the company that contain multiple child elements (plural containers).
        // Try both with and without namespace
        $pluralContainers = $xpath->query('./*[count(*) > 1 and local-name(*[1]) = local-name(*[2])]', $company);

        foreach ($pluralContainers as $container) {
            if (!$container instanceof \DOMElement) {
                continue;
            }
            $containerName = $container->nodeName;

            // Skip if this is a transaction container (handled separately)
            if (\in_array($containerName, ['transactions', 'journal'])) {
                continue;
            }

            $tableName = $this->getTableName($containerName);
            $columns = [];
            if (null !== $container->firstElementChild) {
                $columns = $this->extractXmlElementColumns($container->firstElementChild);
            }

            // Create table
            $this->createXafTable($tableName, $columns);

            // Insert data for each child element
            foreach ($container->childNodes as $childElement) {
                if ($childElement instanceof \DOMElement) {
                    $this->insertXafData($tableName, $childElement, $columns);
                }
            }
        }
    }

    /**
     * Processes transactions in XAF files with special handling for trLine elements.
     *
     * @param \DOMXPath   $xpath   the XPath object for querying the XML
     * @param \DOMElement $company the company element to process within
     */
    private function processXafTransactions(\DOMXPath $xpath, \DOMElement $company): void
    {
        // Try both with and without namespace
        $transactions = $xpath->query('.//xaf:transaction', $company);

        if (0 === $transactions->length) {
            return;
        }

        // Create transactions table
        $transactionColumns = [];
        $firstTransaction = $transactions->item(0);
        if ($firstTransaction instanceof \DOMElement) {
            $transactionColumns = $this->extractXmlElementColumns($firstTransaction, ['trLine']);
        }
        $transactionTableName = $this->getTableName('transactions');
        $this->createXafTable($transactionTableName, $transactionColumns);

        // Create trLine table with foreign key reference
        $trLineColumns = ['transaction_id' => 'INTEGER'];
        $trLineElements = $xpath->query('.//xaf:trLine', $company);
        if (0 === $trLineElements->length) {
            $trLineElements = $xpath->query('.//trLine', $company);
        }
        if ($trLineElements->length > 0) {
            $firstTrLine = $trLineElements->item(0);
            if ($firstTrLine instanceof \DOMElement) {
                $trLineColumns = array_merge($trLineColumns, $this->extractXmlElementColumns($firstTrLine));
            }
        }
        $trLineTableName = $this->getTableName('trLines');
        $this->createXafTable($trLineTableName, $trLineColumns);

        // Insert transaction data
        foreach ($transactions as $transactionId => $transaction) {
            if (!$transaction instanceof \DOMElement) {
                continue;
            }
            $transactionData = $this->extractXmlElementData($transaction, ['trLine']);
            $this->insertXafDataWithId(
                $transactionTableName,
                $transactionData,
                $transactionColumns,
                $transactionId + 1
            );

            // Insert trLine data with foreign key reference
            $trLines = $xpath->query('.//xaf:trLine', $transaction);
            if (0 === $trLines->length) {
                $trLines = $xpath->query('.//trLine', $transaction);
            }
            foreach ($trLines as $trLine) {
                if (!$trLine instanceof \DOMElement) {
                    continue;
                }
                $trLineData = $this->extractXmlElementData($trLine);
                $trLineData['transaction_id'] = $transactionId + 1;
                $this->insertXafDataWithValues($trLineTableName, $trLineData, $trLineColumns);
            }
        }
    }

    /**
     * Extracts column names from an XML element.
     *
     * @param \DOMElement   $element         the XML element
     * @param array<string> $excludeElements elements to exclude from extraction
     *
     * @return array<string, string> the column names and types
     */
    private function extractXmlElementColumns(\DOMElement $element, array $excludeElements = []): array
    {
        $columns = [];

        foreach ($element->childNodes as $child) {
            if (\XML_ELEMENT_NODE === $child->nodeType && !\in_array($child->nodeName, $excludeElements)) {
                if ($child->hasChildNodes() && \XML_ELEMENT_NODE === $child->firstChild->nodeType) {
                    // Nested element - flatten it
                    foreach ($child->childNodes as $nestedChild) {
                        if (\XML_ELEMENT_NODE === $nestedChild->nodeType) {
                            $columns[$child->nodeName.'_'.$nestedChild->nodeName] = 'TEXT';
                        }
                    }
                } else {
                    $columns[$child->nodeName] = 'TEXT';
                }
            }
        }

        return $columns;
    }

    /**
     * Extracts data from an XML element.
     *
     * @param \DOMElement   $element         the XML element
     * @param array<string> $excludeElements elements to exclude from extraction
     *
     * @return array<string, string> the extracted data
     */
    private function extractXmlElementData(\DOMElement $element, array $excludeElements = []): array
    {
        $data = [];

        foreach ($element->childNodes as $child) {
            if (\XML_ELEMENT_NODE === $child->nodeType && !\in_array($child->nodeName, $excludeElements)) {
                if ($child->hasChildNodes() && \XML_ELEMENT_NODE === $child->firstChild->nodeType) {
                    // Nested element - flatten it
                    foreach ($child->childNodes as $nestedChild) {
                        if (\XML_ELEMENT_NODE === $nestedChild->nodeType) {
                            $data[$child->nodeName.'_'.$nestedChild->nodeName] = trim($nestedChild->textContent);
                        }
                    }
                } else {
                    $data[$child->nodeName] = trim($child->textContent);
                }
            }
        }

        return $data;
    }

    /**
     * Creates a table for XAF data.
     *
     * @param string                $tableName the name of the table
     * @param array<string, string> $columns   the columns and their types
     */
    private function createXafTable(string $tableName, array $columns): void
    {
        $columnDefinitions = ['id INTEGER PRIMARY KEY'];

        foreach ($columns as $columnName => $columnType) {
            $sanitizedColumnName = $this->getStrippedString($columnName);
            $columnDefinitions[] = "`{$sanitizedColumnName}` {$columnType}";
        }

        $sql = "CREATE TABLE IF NOT EXISTS `{$tableName}` (".implode(', ', $columnDefinitions).')';
        $this->db->exec($sql);
    }

    /**
     * Inserts XAF data into a table.
     *
     * @param string                $tableName the name of the table
     * @param \DOMElement           $element   the XML element containing the data
     * @param array<string, string> $columns   the table columns
     */
    private function insertXafData(string $tableName, \DOMElement $element, array $columns): void
    {
        $data = $this->extractXmlElementData($element);
        $this->insertXafDataWithValues($tableName, $data, $columns);
    }

    /**
     * Inserts XAF data into a table with a specific ID.
     *
     * @param string                $tableName the name of the table
     * @param array<string, mixed>  $data      the data to insert
     * @param array<string, string> $columns   the table columns
     * @param int                   $id        the ID to use
     */
    private function insertXafDataWithId(string $tableName, array $data, array $columns, int $id): void
    {
        $columnNames = ['id'];
        $values = [$id];

        foreach ($columns as $columnName => $columnType) {
            $sanitizedColumnName = $this->getStrippedString($columnName);
            $columnNames[] = "`{$sanitizedColumnName}`";
            $values[] = $data[$columnName] ?? null;
        }

        $placeholders = implode(', ', array_fill(0, \count($values), '?'));
        $sql = "INSERT INTO `{$tableName}` (".implode(', ', $columnNames).") VALUES ({$placeholders})";

        $stmt = $this->db->prepare($sql);
        foreach ($values as $index => $value) {
            $stmt->bindValue($index + 1, $this->convertToString($value), \SQLITE3_TEXT);
        }
        $stmt->execute();
    }

    /**
     * Inserts XAF data into a table with auto-generated ID.
     *
     * @param string                $tableName the name of the table
     * @param array<string, mixed>  $data      the data to insert
     * @param array<string, string> $columns   the table columns
     */
    private function insertXafDataWithValues(string $tableName, array $data, array $columns): void
    {
        $columnNames = [];
        $values = [];

        foreach ($columns as $columnName => $columnType) {
            $sanitizedColumnName = $this->getStrippedString($columnName);
            $columnNames[] = "`{$sanitizedColumnName}`";
            $values[] = $data[$columnName] ?? null;
        }

        $placeholders = implode(', ', array_fill(0, \count($values), '?'));
        $sql = "INSERT INTO `{$tableName}` (".implode(', ', $columnNames).") VALUES ({$placeholders})";

        $stmt = $this->db->prepare($sql);
        foreach ($values as $index => $value) {
            $stmt->bindValue($index + 1, $this->convertToString($value), \SQLITE3_TEXT);
        }
        $stmt->execute();
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
