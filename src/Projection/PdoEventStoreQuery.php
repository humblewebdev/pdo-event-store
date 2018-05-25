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

use Closure;
use Iterator;
use PDO;
use PDOException;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\EventStoreDecorator;
use Prooph\EventStore\Exception;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\PdoEventStore;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\StreamName;

final class PdoEventStoreQuery implements Query
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
     * @var array
     */
    private $streamPositions = [];

    /**
     * @var array
     */
    private $state = [];

    /**
     * @var callable|null
     */
    private $initCallback;

    /**
     * @var Closure|null
     */
    private $handler;

    /**
     * @var array
     */
    private $handlers = [];

    /**
     * @var boolean
     */
    private $isStopped = false;

    /**
     * @var ?string
     */
    private $currentStreamName = null;

    /**
     * @var array|null
     */
    private $query;

    /**
     * @var string
     */
    private $vendor;

    /**
     * @var bool
     */
    private $triggerPcntlSignalDispatch;

    /**
     * PdoEventStoreQuery constructor.
     * @param EventStore $eventStore
     * @param PDO $connection
     * @param string $eventStreamsTable
     * @param bool $triggerPcntlSignalDispatch
     */
    public function __construct(
        EventStore $eventStore,
        PDO $connection,
        $eventStreamsTable,
        $triggerPcntlSignalDispatch = false)
    {
        $this->eventStore = $eventStore;
        $this->connection = $connection;
        $this->eventStreamsTable = $eventStreamsTable;
        $this->vendor = $this->connection->getAttribute(PDO::ATTR_DRIVER_NAME);
        $this->triggerPcntlSignalDispatch = $triggerPcntlSignalDispatch;

        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof PdoEventStore) {
            throw new Exception\InvalidArgumentException('Unknown event store instance given');
        }
    }

    /**
     * @param Closure $callback
     * @return Query
     */
    public function init(Closure $callback)
    {
        if (null !== $this->initCallback) {
            throw new Exception\RuntimeException('Projection already initialized');
        }

        $callback = Closure::bind($callback, $this->createHandlerContext($this->currentStreamName));

        $result = $callback();

        if (is_array($result)) {
            $this->state = $result;
        }

        $this->initCallback = $callback;

        return $this;
    }

    /**
     * @param string $streamName
     * @return Query
     */
    public function fromStream($streamName)
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['streams'][] = $streamName;

        return $this;
    }

    /**
     * @param \string[] ...$streamNames
     * @return Query
     */
    public function fromStreams($streamNames)
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($streamNames as $streamName) {
            $this->query['streams'][] = $streamName;
        }

        return $this;
    }

    /**
     * @param string $name
     * @return Query
     */
    public function fromCategory($name)
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['categories'][] = $name;

        return $this;
    }

    /**
     * @param \string[] ...$names
     * @return Query
     */
    public function fromCategories($names)
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        foreach ($names as $name) {
            $this->query['categories'][] = $name;
        }

        return $this;
    }

    /**
     * @return Query
     */
    public function fromAll()
    {
        if (null !== $this->query) {
            throw new Exception\RuntimeException('From was already called');
        }

        $this->query['all'] = true;

        return $this;
    }

    /**
     * @param array $handlers
     * @return Query
     */
    public function when(array $handlers)
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        foreach ($handlers as $eventName => $handler) {
            if (! is_string($eventName)) {
                throw new Exception\InvalidArgumentException('Invalid event name given, string expected');
            }

            if (! $handler instanceof Closure) {
                throw new Exception\InvalidArgumentException('Invalid handler given, Closure expected');
            }

            $this->handlers[$eventName] = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));
        }

        return $this;
    }

    /**
     * @param Closure $handler
     * @return Query
     */
    public function whenAny(Closure $handler)
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new Exception\RuntimeException('When was already called');
        }

        $this->handler = Closure::bind($handler, $this->createHandlerContext($this->currentStreamName));

        return $this;
    }

    public function reset()
    {
        $this->streamPositions = [];

        $callback = $this->initCallback;

        if (is_callable($callback)) {
            $result = $callback();

            if (is_array($result)) {
                $this->state = $result;

                return;
            }
        }

        $this->state = [];
    }

    public function stop()
    {
        $this->isStopped = true;
    }

    /**
     * @return array
     */
    public function getState()
    {
        return $this->state;
    }

    public function run()
    {
        if (null === $this->query
            || (null === $this->handler && empty($this->handlers))
        ) {
            throw new Exception\RuntimeException('No handlers configured');
        }

        $singleHandler = null !== $this->handler;

        $this->isStopped = false;
        $this->prepareStreamPositions();

        foreach ($this->streamPositions as $streamName => $position) {
            try {
                $streamEvents = $this->eventStore->load(new StreamName($streamName), $position + 1);
            } catch (Exception\StreamNotFound $e) {
                // ignore
                continue;
            }

            if ($singleHandler) {
                $this->handleStreamWithSingleHandler($streamName, $streamEvents);
            } else {
                $this->handleStreamWithHandlers($streamName, $streamEvents);
            }

            if ($this->isStopped) {
                break;
            }
            if ($this->triggerPcntlSignalDispatch) {
                pcntl_signal_dispatch();
            }
        }
    }

    /**
     * @param string $streamName
     * @param Iterator $events
     */
    private function handleStreamWithSingleHandler($streamName, Iterator $events)
    {
        $this->currentStreamName = $streamName;
        $handler = $this->handler;

        foreach ($events as $key => $event) {
            if ($this->triggerPcntlSignalDispatch) {
                pcntl_signal_dispatch();
            }
            /* @var Message $event */
            $this->streamPositions[$streamName] = $key;

            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    /**
     * @param string $streamName
     * @param Iterator $events
     */
    private function handleStreamWithHandlers($streamName, Iterator $events)
    {
        $this->currentStreamName = $streamName;

        foreach ($events as $key => $event) {
            if ($this->triggerPcntlSignalDispatch) {
                pcntl_signal_dispatch();
            }
            /* @var Message $event */
            $this->streamPositions[$streamName] = $key;

            if (! isset($this->handlers[$event->messageName()])) {
                continue;
            }

            $handler = $this->handlers[$event->messageName()];
            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            if ($this->isStopped) {
                break;
            }
        }
    }

    /**
     * @param null|string $streamName
     * @return mixed
     */
    private function createHandlerContext( &$streamName)
    {
        return new class($this, $streamName) {
            /**
             * @var Query
             */
            private $query;

            /**
             * @var ?string
             */
            private $streamName;

            public function __construct(Query $query, ?string &$streamName)
            {
                $this->query = $query;
                $this->streamName = &$streamName;
            }

            public function stop(): void
            {
                $this->query->stop();
            }

            public function streamName(): ?string
            {
                return $this->streamName;
            }
        };
    }

    private function prepareStreamPositions()
    {
        $streamPositions = [];

        if (isset($this->query['all'])) {
            $eventStreamsTable = $this->quoteTableName($this->eventStreamsTable);
            $sql = <<<EOT
SELECT real_stream_name FROM $eventStreamsTable WHERE real_stream_name NOT LIKE '$%';
EOT;
            $statement = $this->connection->prepare($sql);
            try {
                $statement->execute();
            } catch (PDOException $exception) {
                // ignore and check error code
            }

            if ($statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
            }

            while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
                $streamPositions[$row->real_stream_name] = 0;
            }

            $this->streamPositions = array_merge($streamPositions, $this->streamPositions);

            return;
        }

        if (isset($this->query['categories'])) {
            $rowPlaces = implode(', ', array_fill(0, count($this->query['categories']), '?'));

            $eventStreamsTable = $this->quoteTableName($this->eventStreamsTable);
            $sql = <<<EOT
SELECT real_stream_name FROM $eventStreamsTable WHERE category IN ($rowPlaces);
EOT;
            $statement = $this->connection->prepare($sql);

            try {
                $statement->execute($this->query['categories']);
            } catch (PDOException $exception) {
                // ignore and check error code
            }

            if ($statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
            }

            while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
                $streamPositions[$row->real_stream_name] = 0;
            }

            $this->streamPositions = array_merge($streamPositions, $this->streamPositions);

            return;
        }

        // stream names given
        foreach ($this->query['streams'] as $streamName) {
            $streamPositions[$streamName] = 0;
        }

        $this->streamPositions = array_merge($streamPositions, $this->streamPositions);
    }

    /**
     * @param string $tableName
     * @return string
     */
    private function quoteTableName($tableName)
    {
        switch ($this->vendor) {
            case 'pgsql':
                return '"'.$tableName.'"';
            default:
                return "`$tableName`";
        }
    }
}
