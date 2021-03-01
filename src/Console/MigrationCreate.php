<?php

namespace lexxkn\ClickhouseMigrations\Console;

class MigrationCreate extends \Illuminate\Console\Command
{
    
    /**
     *
     * @var string
     */
    protected $signature = 'clickhouse:migration:create {name}';
    
    /**
     *
     * @var string
     */
    protected $description = 'Create clickhouse migrations';

    /**
     * 
     * Create migration handle
     */
    public function handle()
    {
        $migrationService = new \lexxkn\ClickhouseMigrations\Migrations\MigrationService();
        if ($migrationService->create($this->argument('name'))) {
            $this->info('Migration created successfully');
        }
    }
    
}
