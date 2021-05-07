<?php

namespace incarntor\ClickhouseMigrations\Migrations;

class MigrationService
{
    
    /**
     *
     * @var array
     */
    protected $config;
    
    /**
     *
     * @var incarntor\ClickhouseMigrations\Migrations\ClickhouseModel
     */
    protected $clickhouseModel;
    
    /**
     *
     * @var incarntor\ClickhouseMigrations\Migrations\FileModel
     */
    protected $fileModel;

    /**
     * 
     * Class constructor
     */
    public function __construct()
    {
        $this->fileModel = new FileModel();
        $this->clickhouseModel = new ClickhouseModel($this->getConfig()['table'], $this->getClusterName());
    }

    /**
     *
     * @return int
     */
    public function getLastBatch(): int
    {
        return $this->clickhouseModel->getLastBatch();
    }

    /**
     * 
     * @return array
     */
    public function getNonAppliedMigrations(): \Illuminate\Support\Collection
    {
        $appliedMigrations = $this->getAppliedMigrations();
        $migrations = $this->getMigrations();
        return $migrations->diff($appliedMigrations);
        
    }
    
    /**
     * 
     * @return string|null
     */
    public function getLastAppliedMigration()
    {
        $this->createMigrationTable();
        return $this->clickhouseModel->getLastAppliedMigration();
    }

    /**
     *
     * @return Collection
     */
    public function getLastAppliedMigrations(): array
    {
        $this->createMigrationTable();
        return $this->clickhouseModel->getLastAppliedMigrations();
    }

    /**
     * 
     * @param string $file
     * @param int $batch
     *
     * @return bool
     */
    public function up(string $file, int $batch = 0): bool
    {
        $this->getMigrationObject($file)->up();
        $this->clickhouseModel->addMigration($file, $batch);
        return true;
    }
    
    /**
     *
     * @param array $files
     * @return bool
     */
    public function down(array $files): bool
    {
        foreach ($files as $file) {
            $this->getMigrationObject($file)->down();
        }

        $this->clickhouseModel->removeMigrations($files);
        return true;
    }
    
    /**
     * 
     * @param string $name
     * @return bool
     */
    public function create(string $name): bool
    {
        $fileName = 'migration' . '__' . date('Y_m_d__H_i_s') . '__' . $name;
        $template = $this->fileModel->read($this->getConfig()['template']);
        $this->fileModel->put($this->getConfig()['dir'] . $fileName . '.php', str_replace([
            '{FILE_NAME}',
        ], [
            $fileName,
        ], $template));
        return true;
    }

    /**
     * 
     * Create migration table
     */
    protected function createMigrationTable()
    {
        $this->clickhouseModel->createMigrationTable();
    }
    
    /**
     * 
     * @return \Illuminate\Support\Collection
     */
    protected function getMigrations(): \Illuminate\Support\Collection
    {
        $files = \Illuminate\Support\Collection::make($this->fileModel->files($this->getConfig()['dir']))
                ->transform(function(\Symfony\Component\Finder\SplFileInfo $fileInfo) {
                    return $fileInfo->getFileName();
                })
                ->filter(function($file) {
                    return preg_match('/.+\.php$/i', $file);
                });
        return $files;
    }

    /**
     * 
     * @return \Illuminate\Support\Collection
     */
    protected function getAppliedMigrations(): \Illuminate\Support\Collection
    {
        $this->createMigrationTable();
        return $this->clickhouseModel->getAppliedMigrations()->pluck('version');
    }
    
    /**
     * 
     * @param string $file
     * @return \incarntor\ClickhouseMigrations\Migrations\MigrationInterface
     */
    protected function getMigrationObject(string $file): MigrationInterface
    {
        $filePath = $this->fileModel->getFullPath($this->getConfig()['dir'] . $file);
        require_once $filePath;
        $className = str_replace('.php', '', $file);
        return new $className();
    }

    /**
     * 
     * @return array
     */
    protected function getConfig(): array
    {
        if (is_null($this->config)) {
            $this->config = app()->make('config')->get('database.clickhouse-migrations', []) + [
                'table' => 'migrations',
                'template' => str_replace($this->fileModel->getPathPrefix(), '', __DIR__) . '/clickhouse-migration.template.php.example',
                'dir' => 'database/clickhouse-migrations/',
            ];
        }
        return $this->config;
    }

    /**
     *
     * @return string|null
     */
    protected function getClusterName(): ?string
    {
        return app()->make('config')->get('clickhouse.clusterName');
    }
}
