<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Prooph\EventStore\Pdo;

use EmptyIterator;
use Iterator;
use PDO;
use PDOException;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Exception\TransactionAlreadyStarted;
use Prooph\EventStore\Exception\TransactionNotStarted;
use Prooph\EventStore\Metadata\FieldType;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\Exception\ExtensionNotLoaded;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prooph\EventStore\TransactionalEventStore;
use Prooph\EventStore\Util\Assertion;

final class PostgresEventStore implements PdoEventStore, TransactionalEventStore
{
    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var PDO
     */
    private $connection;

    /**
     * @var PersistenceStrategy
     */
    private $persistenceStrategy;

    /**
     * @var int
     */
    private $loadBatchSize;

    /**
     * @var string
     */
    private $eventStreamsTable;

    /**
     * @var bool
     */
    private $disableTransactionHandling;

    /**
     * @throws ExtensionNotLoaded
     */
    public function __construct(
        MessageFactory $messageFactory,
        PDO $connection,
        PersistenceStrategy $persistenceStrategy,
        $loadBatchSize = 10000,
        $eventStreamsTable = 'event_streams',
        $disableTransactionHandling = false
    ) {
        if (! extension_loaded('pdo_pgsql')) {
            throw ExtensionNotLoaded::with('pdo_pgsql');
        }

        Assertion::min($loadBatchSize, 1);

        $this->messageFactory = $messageFactory;
        $this->connection = $connection;
        $this->persistenceStrategy = $persistenceStrategy;
        $this->loadBatchSize = $loadBatchSize;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->disableTransactionHandling = $disableTransactionHandling;
    }

    /**
     * @param StreamName $streamName
     * @return array
     */
    public function fetchStreamMetadata(StreamName $streamName)
    {
        $sql = <<<EOT
SELECT metadata FROM "$this->eventStreamsTable"
WHERE real_stream_name = :streamName; 
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute(['streamName' => $streamName->toString()]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $stream = $statement->fetch(PDO::FETCH_OBJ);

        if (! $stream) {
            throw StreamNotFound::with($streamName);
        }

        return json_decode($stream->metadata, true);
    }

    /**
     * @param StreamName $streamName
     * @param array $newMetadata
     */
    public function updateStreamMetadata(StreamName $streamName, array $newMetadata)
    {
        $eventStreamsTable = $this->eventStreamsTable;

        $sql = <<<EOT
UPDATE "$eventStreamsTable"
SET metadata = :metadata
WHERE real_stream_name = :streamName; 
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                'streamName' => $streamName->toString(),
                'metadata' => json_encode($newMetadata),
            ]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (1 !== $statement->rowCount()) {
            throw StreamNotFound::with($streamName);
        }
    }

    /**
     * @param StreamName $streamName
     * @return bool
     */
    public function hasStream(StreamName $streamName)
    {
        $sql = <<<EOT
SELECT COUNT(1) FROM "$this->eventStreamsTable"
WHERE real_stream_name = :streamName;
EOT;

        $statement = $this->connection->prepare($sql);

        try {
            $statement->execute(['streamName' => $streamName->toString()]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        return 1 === $statement->fetchColumn();
    }

    /**
     * @param Stream $stream
     * @throws \Exception
     */
    public function create(Stream $stream)
    {
        $streamName = $stream->streamName();

        $this->addStreamToStreamsTable($stream);

        try {
            $tableName = $this->persistenceStrategy->generateTableName($streamName);
            $this->createSchemaFor($tableName);
        } catch (RuntimeException $exception) {
            $this->connection->exec('DROP TABLE IF EXISTS "$tableName";');
            $this->removeStreamFromStreamsTable($streamName);

            throw $exception;
        }

        $this->appendTo($streamName, $stream->streamEvents());
    }

    /**
     * @param StreamName $streamName
     * @param Iterator $streamEvents
     */
    public function appendTo(StreamName $streamName, Iterator $streamEvents)
    {
        $data = $this->persistenceStrategy->prepareData($streamEvents);

        if (empty($data)) {
            return;
        }

        $countEntries = iterator_count($streamEvents);
        $columnNames = $this->persistenceStrategy->columnNames();

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $rowPlaces = '(' . implode(', ', array_fill(0, count($columnNames), '?')) . ')';
        $allPlaces = implode(', ', array_fill(0, $countEntries, $rowPlaces));

        $sql = 'INSERT INTO "' . $tableName . '" (' . implode(', ', $columnNames) . ') VALUES ' . $allPlaces;

        $statement = $this->connection->prepare($sql);

        try {
            $statement->execute($data);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorInfo()[0] === '42P01') {
            throw StreamNotFound::with($streamName);
        }

        if (in_array($statement->errorCode(), ['23000', '23505'], true)) {
            throw new ConcurrencyException();
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }
    }

    /**
     * @param StreamName $streamName
     * @param int $fromNumber
     * @param int|null $count
     * @param MetadataMatcher|null $metadataMatcher
     * @return Iterator
     */
    public function load(
        StreamName $streamName,
        $fromNumber = 1,
        $count = null,
        MetadataMatcher $metadataMatcher = null
    ) {
        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $query = "SELECT stream_name FROM \"$this->eventStreamsTable\" WHERE stream_name = ?";
        $statement = $this->connection->prepare($query);
        $statement->execute([$tableName]);

        if ($statement->rowCount() === 0) {
            throw StreamNotFound::with($streamName);
        }
        [$where, $values] = $this->createWhereClause($metadataMatcher);
        $where[] = 'no >= :fromNumber';

        $whereCondition = 'WHERE ' . implode(' AND ', $where);

        if (null === $count) {
            $limit = $this->loadBatchSize;
        } else {
            $limit = min($count, $this->loadBatchSize);
        }

        $query = <<<EOT
SELECT * FROM "$tableName"
$whereCondition
ORDER BY no ASC
LIMIT :limit;
EOT;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);

        $statement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $statement->bindValue(':limit', $limit, PDO::PARAM_INT);

        foreach ($values as $parameter => $value) {
            $statement->bindValue($parameter, $value, is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        try {
            $statement->execute();
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() === '42703') {
            throw new \UnexpectedValueException('Unknown field given in metadata matcher');
        }

        if ($statement->errorCode() !== '00000') {
            throw StreamNotFound::with($streamName);
        }

        if (0 === $statement->rowCount()) {
            return new EmptyIterator();
        }

        return new PdoStreamIterator(
            $statement,
            $this->messageFactory,
            $this->loadBatchSize,
            $fromNumber,
            $count,
            true
        );
    }

    /**
     * @param StreamName $streamName
     * @param int|null $fromNumber
     * @param int|null $count
     * @param MetadataMatcher|null $metadataMatcher
     * @return Iterator
     */
    public function loadReverse(
        StreamName $streamName,
        $fromNumber = null,
        $count = null,
        MetadataMatcher $metadataMatcher = null
    ) {
        if (null === $fromNumber) {
            $fromNumber = PHP_INT_MAX;
        }
        [$where, $values] = $this->createWhereClause($metadataMatcher);
        $where[] = 'no <= :fromNumber';

        $whereCondition = 'WHERE ' . implode(' AND ', $where);

        if (null === $count) {
            $limit = $this->loadBatchSize;
        } else {
            $limit = min($count, $this->loadBatchSize);
        }

        $tableName = $this->persistenceStrategy->generateTableName($streamName);

        $query = <<<EOT
SELECT * FROM "$tableName"
$whereCondition
ORDER BY no DESC
LIMIT :limit;
EOT;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);

        $statement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $statement->bindValue(':limit', $limit, PDO::PARAM_INT);

        foreach ($values as $parameter => $value) {
            $statement->bindValue($parameter, $value, is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
        }

        try {
            $statement->execute();
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw StreamNotFound::with($streamName);
        }

        if (0 === $statement->rowCount()) {
            return new EmptyIterator();
        }

        return new PdoStreamIterator(
            $statement,
            $this->messageFactory,
            $this->loadBatchSize,
            $fromNumber,
            $count,
            false
        );
    }

    /**
     * @param StreamName $streamName
     */
    public function delete(StreamName $streamName)
    {
        $this->removeStreamFromStreamsTable($streamName);

        $encodedStreamName = $this->persistenceStrategy->generateTableName($streamName);
        $deleteEventStreamSql = <<<EOT
DROP TABLE IF EXISTS "$encodedStreamName";
EOT;

        $statement = $this->connection->prepare($deleteEventStreamSql);
        try {
            $statement->execute();
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }
    }

    public function beginTransaction()
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        try {
            $this->connection->beginTransaction();
        } catch (PDOException $exception) {
            throw new TransactionAlreadyStarted();
        }
    }

    public function commit()
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        try {
            $this->connection->commit();
        } catch (PDOException $exception) {
            throw new TransactionNotStarted();
        }
    }

    public function rollback()
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        try {
            $this->connection->rollBack();
        } catch (PDOException $exception) {
            throw new TransactionNotStarted();
        }
    }

    public function inTransaction()
    {
        return $this->connection->inTransaction();
    }

    /**
     * @param callable $callable
     * @return bool
     * @throws \Throwable
     */
    public function transactional(callable $callable)
    {
        $this->beginTransaction();

        try {
            $result = $callable($this);
            $this->commit();
        } catch (\Throwable $e) {
            $this->rollback();
            throw $e;
        }

        return $result ?: true;
    }

    /**
     * @param null|string $filter
     * @param null|MetadataMatcher $metadataMatcher
     * @param int $limit
     * @param int $offset
     * @return array
     */
    public function fetchStreamNames(
        $filter,
        MetadataMatcher $metadataMatcher,
        $limit = 20,
        $offset = 0
    ) {
        [$where, $values] = $this->createWhereClause($metadataMatcher);

        if (null !== $filter) {
            $where[] = 'real_stream_name = :filter';
            $values[':filter'] = $filter;
        }

        $whereCondition = implode(' AND ', $where);

        if (! empty($whereCondition)) {
            $whereCondition = 'WHERE ' . $whereCondition;
        }

        $query = <<<SQL
SELECT real_stream_name FROM "$this->eventStreamsTable"
$whereCondition
ORDER BY real_stream_name ASC
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

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $streamNames = [];

        foreach ($result as $streamName) {
            $streamNames[] = new StreamName($streamName->real_stream_name);
        }

        return $streamNames;
    }

    /**
     * @param string $filter
     * @param null|MetadataMatcher $metadataMatcher
     * @param int $limit
     * @param int $offset
     * @return array
     */
    public function fetchStreamNamesRegex(
        $filter,
        MetadataMatcher $metadataMatcher,
        $limit = 20,
        $offset = 0
    ) {
        list($where, $values) = $this->createWhereClause($metadataMatcher);

        $where[] = 'real_stream_name ~ :filter';
        $values[':filter'] = $filter;

        $whereCondition = 'WHERE ' . implode(' AND ', $where);

        $query = <<<SQL
SELECT real_stream_name FROM "$this->eventStreamsTable"
$whereCondition
ORDER BY real_stream_name ASC
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

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $streamNames = [];

        foreach ($result as $streamName) {
            $streamNames[] = new StreamName($streamName->real_stream_name);
        }

        return $streamNames;
    }

    /**
     * @param null|string $filter
     * @param int $limit
     * @param int $offset
     * @return array
     * @throws \Prooph\EventStore\Pdo\Exception\RuntimeException
     */
    public function fetchCategoryNames($filter, $limit = 20, $offset = 0)
    {
        $values = [];

        if (null !== $filter) {
            $whereCondition = 'WHERE category = :filter AND category IS NOT NULL';
            $values[':filter'] = $filter;
        } else {
            $whereCondition = 'WHERE category IS NOT NULL';
        }

        $query = <<<SQL
SELECT category FROM "$this->eventStreamsTable"
$whereCondition
GROUP BY category
ORDER BY category ASC
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

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $categoryNames = [];

        foreach ($result as $categoryName) {
            $categoryNames[] = $categoryName->category;
        }

        return $categoryNames;
    }

    /**
     * @param string $filter
     * @param int $limit
     * @param int $offset
     * @return array
     * @throws \Prooph\EventStore\Pdo\Exception\InvalidArgumentException
     * @throws \Prooph\EventStore\Pdo\Exception\RuntimeException
     */
    public function fetchCategoryNamesRegex($filter,$limit = 20,$offset = 0)
    {
        $values[':filter'] = $filter;

        $whereCondition = 'WHERE category ~ :filter AND category IS NOT NULL';

        $query = <<<SQL
SELECT category FROM "$this->eventStreamsTable"
$whereCondition
GROUP BY category
ORDER BY category ASC
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

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $categoryNames = [];

        foreach ($result as $categoryName) {
            $categoryNames[] = $categoryName->category;
        }

        return $categoryNames;
    }

    /**
     * @param null|MetadataMatcher $metadataMatcher
     * @return array
     */
    private function createWhereClause(MetadataMatcher $metadataMatcher)
    {
        $where = [];
        $values = [];

        if (! $metadataMatcher) {
            return [
                $where,
                $values,
            ];
        }

        foreach ($metadataMatcher->data() as $key => $match) {
            /** @var FieldType $fieldType */
            $fieldType = $match['fieldType'];
            $field = $match['field'];
            /** @var Operator $operator */
            $operator = $match['operator'];
            $value = $match['value'];
            $parameters = [];

            if (is_array($value)) {
                foreach ($value as $k => &$v) {
                    $parameters[] = ':metadata_' . $key . '_' . $k;
                    $v = (string) $v;
                }
            } else {
                $parameters = [':metadata_' . $key];
            }

            $parameterString = implode(', ', $parameters);

            $operatorStringEnd = '';

            if ($operator->is(Operator::REGEX())) {
                $operatorString = '~';
            } elseif ($operator->is(Operator::IN())) {
                $operatorString = 'IN (';
                $operatorStringEnd = ')';
            } elseif ($operator->is(Operator::NOT_IN())) {
                $operatorString = 'NOT IN (';
                $operatorStringEnd = ')';
            } else {
                $operatorString = $operator->getValue();
            }

            if ($fieldType->is(FieldType::METADATA())) {
                if (is_bool($value)) {
                    $where[] = "metadata->>'$field' $operatorString '" . var_export($value, true) . "' $operatorStringEnd";
                    continue;
                }
                if (is_int($value)) {
                    $where[] = "CAST(metadata->>'$field' AS INT) $operatorString $parameterString $operatorStringEnd";
                } else {
                    $where[] = "metadata->>'$field' $operatorString $parameterString $operatorStringEnd";
                }
            } else {
                if (is_bool($value)) {
                    $where[] = "$field $operatorString '" . var_export($value, true) . "' $operatorStringEnd";
                    continue;
                }

                $where[] = "$field $operatorString $parameterString $operatorStringEnd";
            }

            $value = (array) $value;
            foreach ($value as $k => $v) {
                $values[$parameters[$k]] = $v;
            }
        }

        return [
            $where,
            $values,
        ];
    }

    /**
     * @param Stream $stream
     * @throws \Prooph\EventStore\Pdo\Exception\RuntimeException
     */
    private function addStreamToStreamsTable(Stream $stream)
    {
        $realStreamName = $stream->streamName()->toString();

        $pos = strpos($realStreamName, '-');

        if (false !== $pos && $pos > 0) {
            $category = substr($realStreamName, 0, $pos);
        } else {
            $category = null;
        }

        $streamName = $this->persistenceStrategy->generateTableName($stream->streamName());
        $metadata = json_encode($stream->metadata());

        $sql = <<<EOT
INSERT INTO "$this->eventStreamsTable" (real_stream_name, stream_name, metadata, category)
VALUES (:realStreamName, :streamName, :metadata, :category);
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $result = $statement->execute([
                ':realStreamName' => $realStreamName,
                ':streamName' => $streamName,
                ':metadata' => $metadata,
                ':category' => $category,
            ]);
        } catch (PDOException $exception) {
            $result = false;
        }

        if (! $result) {
            if (in_array($statement->errorCode(), ['23000', '23505'], true)) {
                throw StreamExistsAlready::with($stream->streamName());
            }

            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }
    }

    /**
     * @param StreamName $streamName
     * @throws \Prooph\EventStore\Pdo\Exception\RuntimeException
     */
    private function removeStreamFromStreamsTable(StreamName $streamName)
    {
        $deleteEventStreamTableEntrySql = <<<EOT
DELETE FROM "$this->eventStreamsTable" WHERE real_stream_name = ?;
EOT;

        $statement = $this->connection->prepare($deleteEventStreamTableEntrySql);
        try {
            $statement->execute([$streamName->toString()]);
        } catch (PDOException $exception) {
            // ignore and check error code
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (1 !== $statement->rowCount()) {
            throw StreamNotFound::with($streamName);
        }
    }

    /**
     * @param string $tableName
     * @throws \Prooph\EventStore\Pdo\Exception\RuntimeException
     */
    private function createSchemaFor($tableName)
    {
        $schema = $this->persistenceStrategy->createSchema($tableName);

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            try {
                $result = $statement->execute();
            } catch (PDOException $exception) {
                $result = false;
            }

            if (! $result) {
                throw new RuntimeException('Error during createSchemaFor: ' . implode('; ', $statement->errorInfo()));
            }
        }
    }
}
