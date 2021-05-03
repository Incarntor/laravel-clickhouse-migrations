<?php

namespace lexxkn\ClickhouseMigrations;

use Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection;

class Builder extends \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Builder
{
    /**
     * Builder constructor.
     *
     * @param \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection $connection
     */
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;

        $options = [];
        $clusters = $this->connection->getConfig()['clusters'] ?? [];
        if (count($clusters) > 0) {
            $cluster = array_key_first($clusters);
            if (is_string($cluster)) {
                $options['onCluster'] = $cluster;
            }
        }

        $this->grammar = new Query\Grammar($options);
    }

    /**
     * Writes data using raw SQL
     *
     * @param string $sql
     * @return bool
     */
    public function writeOne(string $sql): bool
    {
        return $this->connection->getClient()->writeOne($sql);
    }

    /**
     * Reads data using raw SQL
     *
     * @param string $sql
     * @return \Tinderbox\Clickhouse\Query\Result
     */
    public function readOne(string $sql): \Tinderbox\Clickhouse\Query\Result
    {
        return $this->connection->getClient()->readOne($sql);
    }

    /**
     * Performs insert query.
     *
     * @param array $values
     *
     * @return bool
     */
    public function batchInsert(array $values)
    {
        if (empty($values)) {
            return false;
        }

        if (!is_array(reset($values))) {
            $chunks = [[$values]];
        } /*
         * Here, we will sort the insert keys for every record so that each insert is
         * in the same order for the record. We need to make sure this is the case
         * so there are not any errors or problems when inserting these records.
         */
        else {
            $chunks = [];
            foreach ($values as $key => $value) {
                ksort($value);
                $chunks[json_encode(array_keys($value))][] = $value;
            }
        }

        foreach ($chunks as $chunk) {
            $result = $this->connection->insert(
                (new Grammar())->compileBatchInsert($this, $chunk),
                array_flatten($chunk)
            );
            if (!$result) {
                return $result;
            }
        }
        return true;
    }

    /**
     * Executes query to create table
     *
     * @param        $tableName
     * @param string $engine
     * @param array  $structure
     *
     * @return bool
     */
    public function createTable($tableName, string $engine, array $structure)
    {
        return $this->connection->getClient()->writeOne($this->grammar->compileCreateTable($tableName, $engine, $structure));
    }

    /**
     * Executes query to create table if table does not exists
     *
     * @param        $tableName
     * @param string $engine
     * @param array  $structure
     *
     * @return bool
     */
    public function createTableIfNotExists($tableName, string $engine, array $structure)
    {
        return $this->connection->getClient()->writeOne($this->grammar->compileCreateTable($tableName, $engine, $structure, true));
    }

    public function dropTable($tableName)
    {
        return $this->connection->getClient()->writeOne($this->grammar->compileDropTable($tableName));
    }

    public function dropTableIfExists($tableName)
    {
        return $this->connection->getClient()->writeOne($this->grammar->compileDropTable($tableName, true));
    }
}

