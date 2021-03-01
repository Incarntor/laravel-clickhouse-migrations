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
     * @param string $tableName
     */
    public function __construct(string $tableName = null)
    {
        !is_null($tableName) && $this->tableName = $tableName;
    }

    /**
     *
     * Creating migration table if not exists
     */
    public function createMigrationTable()
    {
        $this->getBuilder()->getConnection()->getClient()->writeOne('
            CREATE TABLE IF NOT EXISTS ' . $this->tableName . ' (
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
        return $this->getBuilder()->getConnection()->getClient()->readOne('
            SELECT
                version
            FROM
                ' . $this->tableName . ' m
            ORDER BY
                apply_time DESC
            LIMIT
                1
        ')->current()['version'] ?? null;
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
