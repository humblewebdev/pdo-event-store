<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace Prooph\EventStore\Pdo\Projection;

use OutOfRangeException;
use PDO;
use PDOException;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception\OutOfRangeException;
use Prooph\EventStore\Exception\ProjectionNotFound;
use Prooph\EventStore\Pdo\Exception;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Projection\ProjectionManager;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\Projection\Projector;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector;

final class PostgresProjectionManager implements ProjectionManager
{
    /**
     * @var EventStore
     */
    private $eventStore;

    /**
     * @var PDO
     */
    private $connection;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var string
     */
    private $projectionsTable;

    /**
     * PostgresProjectionManager constructor.
     * @param EventStore $eventStore
     * @param PDO $connection
     * @param string $eventStreamsTable
     * @param string $projectionsTable
     * @throws \Prooph\EventStore\Pdo\Exception\InvalidArgumentException
     */
    public function __construct(
        EventStore $eventStore,
        PDO $connection,
        $eventStreamsTable = 'event_streams',
        $projectionsTable = 'projections'
    ) {
        $this->eventStore = $eventStore;
        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->projectionsTable = $projectionsTable;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof PostgresEventStore) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    /**
     * @param array $options
     * @return PdoEventStoreQuery
     */
    public function createQuery(array $options = [])
    {
        return new PdoEventStoreQuery(
            $this->eventStore,
            $this->connection,
            $this->eventStreamsTable,
            $options[Query::OPTION_PCNTL_DISPATCH] ?? Query::DEFAULT_PCNTL_DISPATCH
        );
    }

    /**
     * @param string $name
     * @param array $options
     * @return PdoEventStoreProjector
     */
    public function createProjection(
        $name,
        array $options = []
    ) {
        return new PdoEventStoreProjector(
            $this->eventStore,
            $this->connection,
            $name,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[PdoEventStoreProjector::OPTION_LOCK_TIMEOUT_MS] ?? PdoEventStoreProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[PdoEventStoreProjector::OPTION_CACHE_SIZE] ?? PdoEventStoreProjector::DEFAULT_CACHE_SIZE,
            $options[PdoEventStoreProjector::OPTION_PERSIST_BLOCK_SIZE] ?? PdoEventStoreProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[PdoEventStoreProjector::OPTION_SLEEP] ?? PdoEventStoreProjector::DEFAULT_SLEEP,
            $options[PdoEventStoreProjector::OPTION_PCNTL_DISPATCH] ?? PdoEventStoreProjector::DEFAULT_PCNTL_DISPATCH,
            $options[PdoEventStoreProjector::OPTION_UPDATE_LOCK_THRESHOLD] ?? PdoEventStoreProjector::DEFAULT_UPDATE_LOCK_THRESHOLD
        );
    }

    /**
     * @param string $name
     * @param ReadModel $readModel
     * @param array $options
     * @return PdoEventStoreReadModelProjector
     */
    public function createReadModelProjection(
        $name,
        ReadModel $readModel,
        array $options = []
    ) {
        return new PdoEventStoreReadModelProjector(
            $this->eventStore,
            $this->connection,
            $name,
            $readModel,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[PdoEventStoreReadModelProjector::OPTION_LOCK_TIMEOUT_MS] ?? PdoEventStoreReadModelProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[PdoEventStoreReadModelProjector::OPTION_PERSIST_BLOCK_SIZE] ?? PdoEventStoreReadModelProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[PdoEventStoreReadModelProjector::OPTION_SLEEP] ?? PdoEventStoreReadModelProjector::DEFAULT_SLEEP,
            $options[PdoEventStoreReadModelProjector::OPTION_PCNTL_DISPATCH] ?? PdoEventStoreReadModelProjector::DEFAULT_PCNTL_DISPATCH,
            $options[PdoEventStoreReadModelProjector::OPTION_UPDATE_LOCK_THRESHOLD] ?? PdoEventStoreReadModelProjector::DEFAULT_UPDATE_LOCK_THRESHOLD
        );
    }

    /**
     * @param string $name
     * @param bool $deleteEmittedEvents
     */
    public function deleteProjection($name,$deleteEmittedEvents)
    {
        $sql = <<<EOT
UPDATE "$this->projectionsTable" SET status = ? WHERE name = ?;
EOT;

        if ($deleteEmittedEvents) {
            $status = ProjectionStatus::DELETING_INCL_EMITTED_EVENTS()->getValue();
        } else {
            $status = ProjectionStatus::DELETING()->getValue();
        }

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                $status,
                $name,
            ]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw Exception\RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (0 === $statement->rowCount()) {
            throw ProjectionNotFound::withName($name);
        }
    }

    /**
     * @param string $name
     */
    public function resetProjection($name)
    {
        $sql = <<<EOT
UPDATE "$this->projectionsTable" SET status = ? WHERE name = ?;
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                ProjectionStatus::RESETTING()->getValue(),
                $name,
            ]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw Exception\RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (0 === $statement->rowCount()) {
            throw ProjectionNotFound::withName($name);
        }
    }

    /**
     * @param string $name
     */
    public function stopProjection($name)
    {
        $sql = <<<EOT
UPDATE "$this->projectionsTable" SET status = ? WHERE name = ?;
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                ProjectionStatus::STOPPING()->getValue(),
                $name,
            ]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw Exception\RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (0 === $statement->rowCount()) {
            throw ProjectionNotFound::withName($name);
        }
    }

    /**
     * @param null|string $filter
     * @param int $limit
     * @param int $offset
     * @return array
     */
    public function fetchProjectionNames($filter,$limit = 20,$offset = 0)
    {
        if (1 > $limit) {
            throw new OutOfRangeException(
                'Invalid limit "'.$limit.'" given. Must be greater than 0.'
            );
        }

        if (0 > $offset) {
            throw new OutOfRangeException(
                'Invalid offset "'.$offset.'" given. Must be greater or equal than 0.'
            );
        }

        $values = [];
        $whereCondition = '';

        if (null !== $filter) {
            $values[':filter'] = $filter;

            $whereCondition = 'WHERE name = :filter';
        }

        $query = <<<SQL
SELECT name FROM "$this->projectionsTable"
$whereCondition
ORDER BY name ASC
LIMIT $limit OFFSET $offset
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute($values);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new Exception\RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $projectionNames = [];

        foreach ($result as $projectionName) {
            $projectionNames[] = $projectionName->name;
        }

        return $projectionNames;
    }

    /**
     * @param string $filter
     * @param int $limit
     * @param int $offset
     * @return array
     */
    public function fetchProjectionNamesRegex($filter,$limit = 20,$offset = 0)
    {
        if (1 > $limit) {
            throw new OutOfRangeException(
                'Invalid limit "'.$limit.'" given. Must be greater than 0.'
            );
        }

        if (0 > $offset) {
            throw new OutOfRangeException(
                'Invalid offset "'.$offset.'" given. Must be greater or equal than 0.'
            );
        }

        $values[':filter'] = $filter;

        $whereCondition = 'WHERE name ~ :filter';
        $query = <<<SQL
SELECT name FROM "$this->projectionsTable"
$whereCondition
ORDER BY name ASC
LIMIT $limit OFFSET $offset
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute($values);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() === '2201B') {
            throw new Exception\InvalidArgumentException('Invalid regex pattern given');
        } elseif ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new Exception\RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $projectionNames = [];

        foreach ($result as $projectionName) {
            $projectionNames[] = $projectionName->name;
        }

        return $projectionNames;
    }

    /**
     * @param string $name
     * @return ProjectionStatus
     */
    public function fetchProjectionStatus($name)
    {
        $query = <<<SQL
SELECT status FROM "$this->projectionsTable"
WHERE name = ?
LIMIT 1
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute([$name]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw Exception\RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $result = $statement->fetch();

        if (false === $result) {
            throw ProjectionNotFound::withName($name);
        }

        return ProjectionStatus::byValue($result->status);
    }

    /**
     * @param string $name
     * @return array
     */
    public function fetchProjectionStreamPositions($name)
    {
        $query = <<<SQL
SELECT position FROM "$this->projectionsTable"
WHERE name = ?
LIMIT 1
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute([$name]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw Exception\RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $result = $statement->fetch();

        if (false === $result) {
            throw ProjectionNotFound::withName($name);
        }

        return json_decode($result->position, true);
    }

    /**
     * @param string $name
     * @return array
     */
    public function fetchProjectionState($name)
    {
        $query = <<<SQL
SELECT state FROM "$this->projectionsTable"
WHERE name = ?
LIMIT 1
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute([$name]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw Exception\RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $result = $statement->fetch();

        if (false === $result) {
            throw ProjectionNotFound::withName($name);
        }

        return json_decode($result->state, true);
    }
}
