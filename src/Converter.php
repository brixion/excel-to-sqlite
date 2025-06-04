<?php

namespace Brixion\ExcelToSqlite;

use OpenSpout\Reader\XLSX\Options;
use OpenSpout\Reader\XLSX\Reader;

class Converter
{
    private $inputFile;
    private $db;

    public function __construct(string $inputFile, string $outputPath)
    {
        $this->inputFile = $inputFile;
        $this->db = new \SQLite3($outputPath . '/' . pathinfo($inputFile, PATHINFO_FILENAME) . '.sqlite');
    }

    public function convert(): void
    {
        $options = new Options();
        $options->SHOULD_PRESERVE_EMPTY_ROWS = true;
        $reader = new Reader($options);
        $reader->open($this->inputFile);

        foreach ($reader->getSheetIterator() as $sheet) {
            foreach ($sheet->getRowIterator() as $currentRow => $row) {
                if ($currentRow === 1) {
                    $this->db->exec("CREATE TABLE IF NOT EXISTS " . $sheet->getName() . " (id INTEGER PRIMARY KEY, " . implode(", ", array_map(fn($cell) => $cell->getValue(), $row->getCells())) . ")");
                } else {
                    $values = array_map(fn($cell) => $cell->getValue(), $row->getCells());
                    $placeholders = implode(", ", array_fill(0, count($values), '?'));
                    $stmt = $this->db->prepare("INSERT INTO " . $sheet->getName() . " VALUES (NULL, " . $placeholders . ")");
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
