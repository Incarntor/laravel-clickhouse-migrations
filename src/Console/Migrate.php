<?php

namespace incarntor\ClickhouseMigrations\Console;

class Migrate extends \Illuminate\Console\Command
{

    /**
     *
     * @var string
     */
    protected $signature = 'clickhouse:migrate {--down} {--y}';

    /**
     *
     * @var string
     */
    protected $description = 'Clickhouse migrations';

    /**
     *
     * @var \lexxkn\ClickhouseMigrations\Migrations\MigrationService
     */
    protected $migrationService;

    /**
     *
     * Class constructor
     */
    public function __construct()
    {
        parent::__construct();
        $this->migrationService = new \incarntor\ClickhouseMigrations\Migrations\MigrationService();
    }

    /**
     *
     * @return bool
     */
    public function handle(): bool
    {
        return $this->option('down') ? $this->down() : $this->up();
    }

    /**
     *
     * @return bool
     */
    protected function up(): bool
    {
        $nonAppliedMigrations = $this->migrationService->getNonAppliedMigrations();
        $nextBatch = $this->migrationService->getLastBatch() + 1;

        if ($nonAppliedMigrations->isEmpty()) {
            $this->info('There are no new migrations');
            return true;
        }
        $this->info(
                'Migrations:' . "\n\t" .
                $nonAppliedMigrations->implode("\n\t")
        );
        if (!$this->option('y') && !$this->confirm('Do you wish to apply?')) {
            return true;
        }
        foreach ($nonAppliedMigrations as $nonAppliedMigration) {
            if ($this->migrationService->up($nonAppliedMigration, $nextBatch)) {
                $this->info('Migration ' . $nonAppliedMigration . ' applied');
            }
        }
        return true;
    }

    /**
     *
     * @return bool
     */
    protected function down(): bool
    {
        $lastAppliedMigrations = $this->migrationService->getLastAppliedMigrations();

        if (count($lastAppliedMigrations) === 0) {
            $this->info('There are no applied migrations');
            return true;
        }

        $this->info(
            'Migrations:' . "\n\t" .
            implode("\n\t", $lastAppliedMigrations)
        );

        if (!$this->option('y') && !$this->confirm('Are you sure?')) {
            return true;
        }

        $this->migrationService->down($lastAppliedMigrations);

        return true;
    }

}
