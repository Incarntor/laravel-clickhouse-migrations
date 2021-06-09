<?php

namespace incarntor\ClickhouseMigrations\Query;

use Tinderbox\ClickhouseBuilder\Query\Identifier;

class Grammar extends \Tinderbox\ClickhouseBuilder\Query\Grammar
{
    /**
     * @var array
     */
    protected array $options = [];

    protected array $allowedTableTypeList        = ['TABLE', 'VIEW', 'MATERIALIZED VIEW'];
    protected array $allowedModifyColumnTypeList = ['ADD', 'DROP', 'CLEAR', 'COMMENT', 'MODIFY'];

    /**
     * Grammar constructor.
     *
     * @param array $options
     */
    public function __construct(array $options = [])
    {
        $this->options = $options;
    }

    /**
     * @param Identifier|string $tableName
     * @param string            $columnName
     * @param string            $type
     *
     * @return string
     */
    public function compileAddColumn(Identifier|string $tableName, string $columnName, string $type): string
    {
        return $this->processColumn($tableName, 'ADD', $columnName, $type);
    }

    /**
     * @param Identifier|string $tableName
     * @param string            $columnName
     *
     * @return string
     */
    public function compileDropColumn(Identifier|string $tableName, string $columnName): string
    {
        return $this->processColumn($tableName, 'DROP', $columnName);
    }

    /**
     * @param Identifier|string $tableName
     * @param string            $modifyType
     * @param string            $columnName
     * @param string            $columnType
     *
     * @return string
     */
    protected function processColumn(Identifier|string $tableName, string $modifyType, string $columnName, string $columnType = ''): string
    {
        $tableName = $this->compileTableName($tableName);
        if ( in_array($modifyType, $this->allowedModifyColumnTypeList, true) === false ) {
            throw new \InvalidArgumentException('Invalid modify type. allowed: ' . implode(', ', $this->allowedModifyColumnTypeList));
        }

        $sql = "ALTER TABLE {$tableName} ";

        $sql .= $this->compileOnClusterQuery($sql);

        return $sql . " $modifyType COLUMN {$columnName} {$columnType}";
    }

    /**
     * @param        $tableName
     * @param string $engine
     * @param array  $structure
     * @param false  $ifNotExists
     *
     * @return string
     */
    public function compileCreateTable($tableName, string $engine, array $structure, $ifNotExists = false): string
    {
        $tableName = $this->compileTableName($tableName);
        $sql = "CREATE TABLE " . ($ifNotExists ? 'IF NOT EXISTS ' : '') . "{$tableName} ";

        $sql .= $this->compileOnClusterQuery($sql);

        return $sql . "({$this->compileTableStructure($structure)}) ENGINE = {$engine}";
    }

    /**
     * @param        $viewName
     * @param string $query
     * @param bool   $ifNotExists
     * @param bool   $isMaterialized
     *
     * @return string
     */
    public function compileCreateView($viewName, string $query, bool $ifNotExists = false, bool $isMaterialized = false): string
    {
        $viewName = $this->compileTableName($viewName);
        $sql = 'CREATE ' . (($isMaterialized === true) ? 'MATERIALIZED ' : '') . 'VIEW ' . ($ifNotExists ? 'IF NOT EXISTS ' : '') . "{$viewName} ";

        $sql .= $this->compileOnClusterQuery($sql);

        return $sql . " AS {$query}";
    }

    /**
     * @param      $tableName
     * @param bool $ifExists
     *
     * @return string
     */
    public function compileDropTable($tableName, $ifExists = false): string
    {
        $tableName = $this->compileTableName($tableName);
        $sql = "DROP TABLE " . ($ifExists ? 'IF EXISTS ' : '') . "{$tableName} ";

        $sql .= $this->compileOnClusterQuery($sql);

        return $sql;
    }

    /**
     * @param      $viewName
     * @param bool $ifExists
     *
     * @return string
     */
    public function compileDropView($viewName, bool $ifExists = false): string
    {
        $viewName = $this->compileTableName($viewName);
        $sql = "DROP VIEW " . ($ifExists ? 'IF EXISTS ' : '') . "{$viewName} ";

        $sql .= $this->compileOnClusterQuery($sql);

        return $sql;
    }

    /**
     * @param Identifier|string $tableName
     *
     * @return string
     */
    private function compileTableName(Identifier|string $tableName): string
    {
        if ( $tableName instanceof Identifier ) {
            $tableName = (string) $tableName;
        }

        return $tableName;
    }

    /**
     * @param string $sql
     *
     * @return string
     */
    private function compileOnClusterQuery(string $sql): string
    {
        if ( $this->options['onCluster'] !== null ) {
            $sql .= " ON CLUSTER {$this->options['onCluster']} ";
        }

        return $sql;
    }
}
