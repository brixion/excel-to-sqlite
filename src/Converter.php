<?php

namespace Brixion\ExcelToSQLite;

use OpenSpout\Reader\XLSX\Options;
use OpenSpout\Reader\XLSX\Reader;

class Converter {
    private $inputFile;
    private $db;

    public function __construct($inputFile, $outputPath) {
        $this->inputFile = $inputFile;
        $this->db = new \SQLite3($outputPath . '/' . pathinfo($inputFile, PATHINFO_FILENAME) . '.sqlite');

        $this->convert();
    }

    private function convert() {
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
                        $stmt->bindValue($index + 1, $value);
                    }
                    $stmt->execute();
                }
            }
        }

        $reader->close();
    }
}
