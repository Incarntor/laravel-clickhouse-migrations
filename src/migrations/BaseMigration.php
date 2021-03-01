<?php

namespace lexxkn\ClickhouseMigrations\Migrations;

abstract class BaseMigration implements MigrationInterface
{
    
    /**
     * 
     * @return \ClickHouseDB\Client
     */
    protected function getClient(): \ClickHouseDB\Client
    {
        return \lexxkn\ClickhouseMigrations\Clickhouse::client();
    }
    
}
