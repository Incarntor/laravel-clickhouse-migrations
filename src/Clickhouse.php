<?php

namespace incarntor\ClickhouseMigrations;

use Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection;
use Tinderbox\ClickhouseBuilder\Query\Builder;
use incarntor\ClickhouseMigrations\Builder as ClickhouseBuilder;

class Clickhouse
{
    /**
     *
     * @var ClickhouseBuilder
     */
    private static $builder;

    /**
     *
     * @return ClickhouseBuilder
     * @throws \Tinderbox\Clickhouse\Exceptions\ServerProviderException
     */
    public static function builder(): ClickhouseBuilder
    {
        if (is_null(self::$builder)) {
            $config = app()->make('config')->get('database.connections.clickhouse', []);

            if (!isset($config['connection'])) {
                $connection = new Connection($config);
            } else {
                if (!class_exists($config['connection'])) {
                    throw new \Exception('Clickhouse connection not exists.');
                }

                $connection = new $config['connection']($config);
            }

            self::$builder = new ClickhouseBuilder($connection);
        }
        return self::$builder;
    }

    /**
     *
     * Close constructor
     */
    public function __construct()
    {

    }

    /**
     *
     * Close clone
     */
    public function __clone()
    {

    }

}
