<?php

namespace lexxkn\ClickhouseMigrations\Query;

use Tinderbox\ClickhouseBuilder\Query\Identifier;

class Grammar extends \Tinderbox\ClickhouseBuilder\Query\Grammar
{
    /**
     * @var array
     */
    protected array $options = [];

    protected array $allowedTableTypeList = ['table', 'view', 'materialized view'];

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
     * @param        $tableName
     * @param string $engine
     * @param array  $structure
     * @param bool   $ifNotExists
     *
     * @return string
     */
    public function compileCreateTable($tableName, string $engine, array $structure, $ifNotExists = false): string
    {
        if ( $tableName instanceof Identifier ) {
            $tableName = (string) $tableName;
        }

        $sql = "CREATE TABLE " . ($ifNotExists ? 'IF NOT EXISTS ' : '') . "{$tableName} ";

        if ( $this->options['onCluster'] !== null ) {
            $sql .= " ON CLUSTER {$this->options['onCluster']}";
        }

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
        if ( $viewName instanceof Identifier ) {
            $viewName = (string) $viewName;
        }

        $sql = 'CREATE ' . (($isMaterialized === true) ? 'MATERIALIZED ' : '') . 'VIEW '. ($ifNotExists ? 'IF NOT EXISTS ' : '') . "{$viewName} ";

        if ( $this->options['onCluster'] !== null ) {
            $sql .= " ON CLUSTER {$this->options['onCluster']}";
        }

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
        if ( $tableName instanceof Identifier ) {
            $tableName = (string) $tableName;
        }

        $sql = "DROP TABLE " . ($ifExists ? 'IF EXISTS ' : '') . "{$tableName} ";

        if ( $this->options['onCluster'] !== null ) {
            $sql .= " ON CLUSTER {$this->options['onCluster']}";
        }

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
        if ( $viewName instanceof Identifier ) {
            $viewName = (string) $viewName;
        }

        $sql = "DROP VIEW " . ($ifExists ? 'IF EXISTS ' : '') . "{$viewName} ";

        if ( $this->options['onCluster'] !== null ) {
            $sql .= " ON CLUSTER {$this->options['onCluster']}";
        }

        return $sql;
    }
}
