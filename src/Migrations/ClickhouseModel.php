<?php

namespace incarntor\ClickhouseMigrations\Migrations;

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
            CREATE TABLE IF NOT EXISTS ' . $this->tableName . ' ' . $clusterQueryPart . ' (
                version String,
                apply_time DateTime DEFAULT NOW(),
                batch UInt16 DEFAULT 0
            ) ENGINE = ReplacingMergeTree()
                ORDER BY
                    (version)
        ');
    }

    /**
     * @return array
     */
    public function getLastAppliedMigrations(): array
    {
        $client = $this->getBuilder()->getConnection()->getClient();
        $query = '
            SELECT
                version
            FROM
                ' . $this->tableName . ' m
            WHERE batch=(SELECT max(batch) FROM ' . $this->tableName . ')
            ORDER BY
                apply_time DESC
        ';

        if ( $client->readOne($query)->count() > 0 ) {
            return array_column($client->readOne($query)->getRows(), 'version');
        }

        return [];
    }

    /**
     * @return array
     */
    public function getLastBatch(): int
    {
        $client = $this->getBuilder()->getConnection()->getClient();
        $query = 'SELECT max(batch) as max_batch FROM ' . $this->tableName . ' m';

        if ( $client->readOne($query)->count() > 0 ) {
            return $client->readOne($query)->current()['max_batch'] ?? 0;
        }

        return 0;
    }

    /**
     * @return string|null
     * @deprecated
     *
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

        if ( $client->readOne($query)->count() > 0 ) {
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
     * @param int    $batch
     *
     * @return bool
     */
    public function addMigration(string $version, int $batch = 0): bool
    {
        return $this->getBuilder()->from($this->tableName)->insert(['version' => $version, 'batch' => $batch]);
    }

    /**
     * @param string $version
     *
     * @return bool
     * @deprecated
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
     * @param array $versions
     *
     * @return bool
     */
    public function removeMigrations(array $versions): bool
    {
        return $this->getBuilder()
                    ->from($this->tableName)
                    ->whereIn(
                        'version',
                        $versions
                    )->delete();
    }

    /**
     *
     * @return \incarntor\ClickhouseMigrations\Builder
     */
    protected function getBuilder(): \incarntor\ClickhouseMigrations\Builder
    {
        return \incarntor\ClickhouseMigrations\Clickhouse::builder();
    }
}
