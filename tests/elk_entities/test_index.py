import unittest
from collections import OrderedDict

from unittest.mock import MagicMock
import numpy as np

from dask_elk.elk_entities.index import IndexRegistry, Index
from dask_elk.elk_entities.node import Node, NodeRegistry


class TestIndexRegistry(unittest.TestCase):
    def setUp(self):
        self.__node = Node(node_id='E43TT5r9Q9-VWckuAXLf0Q',
                           publish_address='1.1.1.1:9200')

    def test_get_indices_from_elasticsearch_shards(self):
        elk_client = MagicMock()
        elk_client.indices.get_mapping.return_value = \
            self.__get_indices_mapping()

        elk_client.search_shards.return_value = self.__mocked_search_shards()
        elk_client.cat.shards.return_value = self.__mocked_shards()
        node_registry = MagicMock(spec_set=NodeRegistry)
        node_registry.get_node_by_id.return_value = self.__node

        index_registry = IndexRegistry(nodes_registry=node_registry)
        index_registry.get_indices_from_elasticsearch(elk_client,
                                                      index='index-2018.08.20',
                                                      doc_type='_doc')

        self.assertIn('index-2018.08.20', index_registry.indices)
        index = index_registry.indices['index-2018.08.20']
        index_shards = index.shards
        self.assertEqual(5, len(index_shards))

        for i in range(0, 4):
            self.assertIsNotNone(index.get_shard_by_id(i))
            shard = index.get_shard_by_id(i)
            self.assertEqual(shard.node, self.__node)

    def test_get_indices_from_elasticsearch_mappings(self):
        elk_client = MagicMock()
        elk_client.indices.get_mapping.return_value = \
            self.__get_indices_mapping()

        elk_client.search_shards.return_value = self.__mocked_search_shards()
        elk_client.cat.shards.return_value = self.__mocked_shards()
        node_registry = MagicMock(spec_set=NodeRegistry)
        node_registry.get_node_by_id.return_value = self.__node

        index_registry = IndexRegistry(nodes_registry=node_registry)
        index_registry.get_indices_from_elasticsearch(elk_client,
                                                      index='index-2018.08.20',
                                                      doc_type='_doc')

        expected_mapping = {
            'activity_earliest': np.dtype('datetime64[ns]'),
            'activity_latest': np.dtype('datetime64[ns]'),
            'raised_at': np.dtype('datetime64[ns]'),
            'syslog_description': np.dtype(object),
            'description': np.dtype(object),
            'dst': np.dtype(object),
            'dst_port': np.dtype('float64'),
            'id': np.dtype(object),
            'score': np.dtype('float64'),
            'updated_at': np.dtype('datetime64[ns]'),
            'username': np.dtype(object),
            '_id': np.dtype(object)

        }

        index = index_registry.indices['index-2018.08.20']
        self.assertDictEqual(expected_mapping, index.mapping)

    def test_calculate_meta_same_meta(self):
        node_registry = MagicMock(spec_set=NodeRegistry)
        node_registry.get_node_by_id.return_value = self.__node
        index_registry = IndexRegistry(nodes_registry=node_registry)

        # 2 indices the same mapping
        index1 = Index(name='index1', mapping={'col1': np.dtype(object),
                                               'col2': np.dtype('float64')})

        index2 = Index(name='index2', mapping={'col1': np.dtype(object),
                                               'col2': np.dtype('float64')})

        index_registry.indices = {index1.name: index1, index2.name: index2}
        meta = index_registry.calculate_meta()

        self.assertEqual(len(meta.columns), 2)
        self.assertListEqual(['col1', 'col2'], meta.columns.tolist())
        self.assertEqual(np.dtype(object), meta['col1'].dtype)
        self.assertEqual(np.dtype('float64'), meta['col2'].dtype)

    def test_calculate_meta_different_meta(self):
        node_registry = MagicMock(spec_set=NodeRegistry)
        node_registry.get_node_by_id.return_value = self.__node
        index_registry = IndexRegistry(nodes_registry=node_registry)

        # 2 indices the same mapping
        index1 = Index(name='index1', mapping={'col1': np.dtype(object)})

        index2 = Index(name='index2', mapping={'col2': np.dtype('float64')})

        index_registry.indices = {index1.name: index1, index2.name: index2}
        meta = index_registry.calculate_meta()

        self.assertEqual(len(meta.columns), 2)
        self.assertListEqual(['col1', 'col2'], meta.columns.tolist())
        self.assertEqual(np.dtype(object), meta['col1'].dtype)
        self.assertEqual(np.dtype('float64'), meta['col2'].dtype)

    def test_calculate_meta_same_column_different_type(self):
        node_registry = MagicMock(spec_set=NodeRegistry)
        node_registry.get_node_by_id.return_value = self.__node
        index_registry = IndexRegistry(nodes_registry=node_registry)

        # 2 indices the same mapping
        index1 = Index(name='index1', mapping={'col1': np.dtype(object)})

        index2 = Index(name='index2', mapping={'col1': np.dtype('float64')})
        indices = OrderedDict()
        indices[index1.name] = index1
        indices[index2.name] = index2

        index_registry.indices = indices
        meta = index_registry.calculate_meta()

        self.assertEqual(len(meta.columns), 1)
        self.assertListEqual(['col1', ], meta.columns.tolist())
        self.assertEqual(np.dtype('float64'), meta['col1'].dtype)

    @staticmethod
    def __get_indices_mapping():
        return {
            "index-2018.08.20": {
                "mappings": {
                    "_doc": {
                        "properties": {
                            "activity_earliest": {
                                "type": "date"
                            },
                            "activity_latest": {
                                "type": "date"
                            },
                            "description": {
                                "type": "keyword"
                            },
                            "dst": {
                                "type": "ip"
                            },
                            "dst_port": {
                                "type": "integer"
                            },
                            "id": {
                                "type": "keyword"
                            },
                            "raised_at": {
                                "type": "date"
                            },
                            "score": {
                                "type": "integer"
                            },
                            "syslog_description": {
                                "type": "keyword"
                            },
                            "updated_at": {
                                "type": "date"
                            },
                            "username": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            }
        }

    @staticmethod
    def __mocked_search_shards():
        return {
            "nodes": {
                "E43TT5r9Q9-VWckuAXLf0Q": {
                    "name": "E43TT5r",
                    "ephemeral_id": "RPOsajztT0eTGmsR6vYAOw",
                    "transport_address": "172.18.0.2:9300",
                    "attributes": {}
                }
            },
            "indices": {
                "index-2018.08.20": {}
            },
            "shards": [
                [
                    {
                        "state": "STARTED",
                        "primary": True,
                        "node": "E43TT5r9Q9-VWckuAXLf0Q",
                        "relocating_node": None,
                        "shard": 0,
                        "index": "index-2018.08.20",
                        "allocation_id": {
                            "id": "7gsIfTB5RTm8BoR29Sff7g"
                        }
                    }
                ],
                [
                    {
                        "state": "STARTED",
                        "primary": True,
                        "node": "E43TT5r9Q9-VWckuAXLf0Q",
                        "relocating_node": None,
                        "shard": 1,
                        "index": "index-2018.08.20",
                        "allocation_id": {
                            "id": "53hs_j-nRbioSiah3hiYAw"
                        }
                    }
                ],
                [
                    {
                        "state": "STARTED",
                        "primary": True,
                        "node": "E43TT5r9Q9-VWckuAXLf0Q",
                        "relocating_node": None,
                        "shard": 2,
                        "index": "index-2018.08.20",
                        "allocation_id": {
                            "id": "7LiMF80YSOmCT_e0ZWYjrg"
                        }
                    }
                ],
                [
                    {
                        "state": "STARTED",
                        "primary": True,
                        "node": "E43TT5r9Q9-VWckuAXLf0Q",
                        "relocating_node": None,
                        "shard": 3,
                        "index": "index-2018.08.20",
                        "allocation_id": {
                            "id": "P_vAnbMcQU20yB3Stmvb5A"
                        }
                    }
                ],
                [
                    {
                        "state": "STARTED",
                        "primary": True,
                        "node": "E43TT5r9Q9-VWckuAXLf0Q",
                        "relocating_node": None,
                        "shard": 4,
                        "index": "index-2018.08.20",
                        "allocation_id": {
                            "id": "8fwaA0kWTCeI01tUxW-K1Q"
                        }
                    }
                ]
            ]
        }

    @staticmethod
    def __mocked_shards():
        return [
            {
                "index": "index-2018.08.20",
                "shard": "1",
                "prirep": "p",
                "docs": "8"
            },
            {
                "index": "index-2018.08.20",
                "shard": "1",
                "prirep": "r",
                "docs": None
            },
            {
                "index": "index-2018.08.20",
                "shard": "3",
                "prirep": "p",
                "docs": "6"
            },
            {
                "index": "index-2018.08.20",
                "shard": "3",
                "prirep": "r",
                "docs": None
            },
            {
                "index": "index-2018.08.20",
                "shard": "4",
                "prirep": "p",
                "docs": "1"
            },
            {
                "index": "index-2018.08.20",
                "shard": "4",
                "prirep": "r",
                "docs": None
            },
            {
                "index": "index-2018.08.20",
                "shard": "2",
                "prirep": "p",
                "docs": "6"
            },
            {
                "index": "index-2018.08.20",
                "shard": "2",
                "prirep": "r",
                "docs": None
            },
            {
                "index": "index-2018.08.20",
                "shard": "0",
                "prirep": "p",
                "docs": "5"
            },
            {
                "index": "index-2018.08.20",
                "shard": "0",
                "prirep": "r",
                "docs": None
            }
        ]
