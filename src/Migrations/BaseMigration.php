<?php

namespace incarntor\ClickhouseMigrations\Migrations;

abstract class BaseMigration implements MigrationInterface
{
    /**
     *
     * @return \incarntor\ClickhouseMigrations\Builder
     */
    protected function getBuilder(): \incarntor\ClickhouseMigrations\Builder
    {
        return \incarntor\ClickhouseMigrations\Clickhouse::builder();
    }

}
