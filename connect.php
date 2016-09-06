<?php
/**
 * Description: connect impala
 * @author tairyao
 * Date: 2016/9/6 17:02
 */

require_once './lib/Thrift/ClassLoader/ThriftClassLoader.php';
use Thrift\ClassLoader\ThriftClassLoader;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Exception\TException;

$GEN_DIR = realpath(dirname(__FILE__)).'/gen-php';
$loader = new ThriftClassLoader();
$loader->registerNamespace('Thrift', './lib');
$loader->registerDefinition('impala', $GEN_DIR);
$loader->registerDefinition('beeswax', $GEN_DIR);
$loader->register();

try {
    $socket = new TSocket( '192.168.1.100', '21000' );
    $socket->setRecvTimeout(15000);

    $transport = new TBufferedTransport($socket, 1024, 1024);
    $protocol = new TBinaryProtocol($transport);
    $client = new \impala\ImpalaServiceClient($protocol);
    $transport->open();

    $query = new \beeswax\Query(array('query' => 'use test'));
    $client->query($query);

    $sql = "select * from test 10";

    $query = new \beeswax\Query(array('query' => $sql));
    $handle = $client->query($query);

    $metadata = $client->get_results_metadata($handle);
    $fields = $metadata->schema->fieldSchemas;

    $data = array();
    $dataPiece = $client->fetch($handle, false, 1024);
    $data = array_merge($data, parseField($dataPiece, $fields));
    while ($dataPiece->has_more){
        $dataPiece = $client->fetch($handle, false, 1024);
        $data = array_merge($data, parseField($dataPiece, $fields));
    }
    $transport->close();

    print_r($data);

} catch (\Exception $e) {
    print $e->getMessage() . "\n";
}

function parseField($data, $fields) {
    $result = array();
    $fieldCount = count($fields);
    foreach ($data->data as $row => $rawValues) {
        $values = explode("\t", $rawValues, $fieldCount);
        foreach ($fields as $k => $fieldObj) {
            $result[$row][$fieldObj->name] = $values[$k];
        }
    }
    return $result;
}