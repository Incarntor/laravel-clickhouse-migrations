<?php

namespace lexxkn\ClickhouseMigrations\Migrations;

use Tinderbox\ClickhouseBuilder\Query\Builder;

class ClickhouseModel
{
    /**
     *
     * @var string
     */
    protected $tableName = 'migrations';

    /**
     *
     * @var string|null
     */
    protected $clusterName = null;

    /**
     * ClickhouseModel constructor.
     *
     * @param string|null $tableName
     * @param string|null $clusterName
     */
    public function __construct(string $tableName = null, string $clusterName = null)
    {
        !is_null($tableName) && $this->tableName = $tableName;
        !is_null($clusterName) && $this->clusterName = $clusterName;
    }

    /**
     *
     * Creating migration table if not exists
     */
    public function createMigrationTable()
    {
        $clusterQueryPart = ((is_string($this->clusterName)) ? 'ON CLUSTER ' . $this->clusterName . '' : '');
        $this->getBuilder()->getConnection()->getClient()->writeOne('
            CREATE TABLE IF NOT EXISTS '. $this->tableName . ' ' . $clusterQueryPart  .' (
                version String,
                apply_time DateTime DEFAULT NOW()
            ) ENGINE = ReplacingMergeTree()
                ORDER BY
                    (version)
        ');
    }

    /**
     *
     * @return string|null
     */
    public function getLastAppliedMigration()
    {
        $client = $this->getBuilder()->getConnection()->getClient();
        $query = '
            SELECT
                version
            FROM
                ' . $this->tableName . ' m
            ORDER BY
                apply_time DESC
            LIMIT
                1
        ';

        if ($client->readOne($query)->count() > 0) {
            return $client->readOne($query)->current()['version'] ?? null;
        }

        return null;
    }

    /**
     *
     * @return \Illuminate\Support\Collection
     */
    public function getAppliedMigrations(): \Illuminate\Support\Collection
    {
        return new \Illuminate\Support\Collection(
            $this->getBuilder()->select()->from($this->tableName)->get()
        );
    }

    /**
     *
     * @param string $version
     * @return bool
     */
    public function addMigration(string $version): bool
    {
        return $this->getBuilder()->from($this->tableName)->insert(['version' => $version]);
    }

    /**
     *
     * @param string $version
     * @return bool
     */
    public function removeMigration(string $version): bool
    {
        return $this->getBuilder()
            ->from($this->tableName)
            ->where(
                'version',
                '=' ,
                $version
            )->delete();
    }

    /**
     *
     * @return \lexxkn\ClickhouseMigrations\Builder
     */
    protected function getBuilder(): \lexxkn\ClickhouseMigrations\Builder
    {
        return \lexxkn\ClickhouseMigrations\Clickhouse::builder();
    }
}
