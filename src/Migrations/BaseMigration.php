<?php

namespace lexxkn\ClickhouseMigrations\Migrations;

abstract class BaseMigration implements MigrationInterface
{
    /**
     *
     * @return \lexxkn\ClickhouseMigrations\Builder
     */
    protected function getBuilder(): \lexxkn\ClickhouseMigrations\Builder
    {
        return \lexxkn\ClickhouseMigrations\Clickhouse::builder();
    }

}
