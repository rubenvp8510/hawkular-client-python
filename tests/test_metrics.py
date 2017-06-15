"""
   Copyright 2015-2016 Red Hat, Inc. and/or its affiliates
   and other contributors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
from __future__ import unicode_literals

import unittest
import uuid
from  hawkular.metrics import *
import os
import base64
from datetime import datetime, timedelta

try:
    import mock
    # from mock import patch, MagicMock
except ImportError:
    import unittest.mock as mock
    # from unittest.mock import patch, MagicMock

from tests import base


class TestMetricFunctionsBase(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_tenant = str(uuid.uuid4())
        self.client = HawkularMetricsClient(tenant_id=self.test_tenant, port=8080, authtoken='secret')


class TenantTestCase(TestMetricFunctionsBase):
    """
    Test creating and fetching tenants. Each creation test should also
    fetch the tenants to test that functionality also
    """

    def test_tenant_creation(self):
        tenant = str(uuid.uuid4())
        self.client.create_tenant(tenant)
        tenants = self.client.query_tenants()

        expect = Tenant({'id': tenant})
        self.assertIn(expect, tenants)

    def test_tenant_creation_with_retentions(self):
        tenant = str(uuid.uuid4())
        retentions = {'gauge': 8, 'availability': 30}
        self.client.create_tenant(tenant, retentions)

        expect = Tenant({'id': tenant, 'retentions': {'gauge': 8, 'availability': 30}})
        self.assertIn(expect, self.client.query_tenants())


class MetricsMockUpCase(unittest.TestCase):

    @mock.patch('hawkular.client.urlopen', autospec=True)
    @mock.patch('hawkular.client.HawkularBaseClient.query_status')
    def test_verify(self, m_query_status, m_urlopen):
        m_query_status.return_value = {'Implementation-Version': '0.23.0'}
        c = HawkularMetricsClient(tenant_id='aa', username='a', password='b')

        c.query_tenants()
        req = m_urlopen.call_args[0][0]
        authr = req.get_header('Authorization')
        self.assertEqual('Basic', authr[:5])
        self.assertEqual('YTpi', authr[6:])

        c = HawkularMetricsClient(tenant_id='aa', token='AABBCCDD', authtoken='EEFFGGHH')
        c.query_tenants()
        req = m_urlopen.call_args[0][0]

        self.assertEqual('Bearer AABBCCDD', req.get_header('Authorization'))
        # Incorrect capitalization due to urllib2
        self.assertEqual('EEFFGGHH', req.get_header('Hawkular-admin-token'))

class MetricsTestCase(TestMetricFunctionsBase):
    """
    Test metric functionality, both adding definition and querying for definition, 
    as well as adding new gauge and availability metrics. 

    Metric definition creation should also test fetching the definition, while
    metric inserts should test also fetching the metric data.
    """

    def setUp(self):
        self.maxDiff = None
        self.test_tenant = str(uuid.uuid4())
        self.client = HawkularMetricsClient(tenant_id=self.test_tenant, port=8080, authtoken='secret',
                                            use_dictionaries=False)

    def find_metric(self, name, definitions):
        for defin in definitions:
            if defin.id == name:
                return defin

    def test_gauge_creation(self):
        """
        Test creating gauge metric definitions with different tags and definition.
        """
        # Create gauge metrics with empty details and added details
        id_name = 'test.create.gauge.{}'
        metric = Metric()
        metric.type = MetricType.Gauge

        metric.id = id_name.format('1')

        md1 = self.client.create_metric_definition(metric)

        metric.id = id_name.format('2')
        metric.data_retention = 90

        md2 = self.client.create_metric_definition(metric)
        metric.tags = {'units': 'bytes', 'env': 'qa'}
        metric.id = id_name.format('3')

        md3 = self.client.create_metric_definition(metric)
        self.assertTrue(md1)
        self.assertTrue(md2)
        self.assertTrue(md3)

        # Fetch metrics definition and check that the ones we created appeared also
        m = self.client.query_metric_definitions(MetricType.Gauge)
        self.assertEqual(3, len(m))
        self.assertEqual(self.test_tenant, m[0].tenant_id)
        expected_m1 = Metric()
        expected_m1.data_retention = 7
        expected_m1.type = MetricType.Gauge
        expected_m1.id = 'test.create.gauge.1'
        expected_m1.tenant_id = self.test_tenant

        expected_m2 = Metric()
        expected_m2.data_retention = 90
        expected_m2.type = MetricType.Gauge
        expected_m2.id = 'test.create.gauge.3'
        expected_m2.tenant_id = self.test_tenant
        expected_m2.tags = {'units': 'bytes', 'env': 'qa'}

        expected_m3 = Metric()
        expected_m3.data_retention = 90
        expected_m3.type = MetricType.Gauge
        expected_m3.id = 'test.create.gauge.2'
        expected_m3.tenant_id = self.test_tenant

        self.assertIn(expected_m1, m)
        self.assertIn(expected_m2, m)
        self.assertIn(expected_m3, m)
        md3_f = self.find_metric(id_name.format('3'), m)
        self.assertEqual('bytes', md3_f.tags['units'])

        # Lets try creating a duplicate metric
        m4 = Metric()
        m4.id = id_name.format('1')
        m4.type = MetricType.Gauge
        md4 = self.client.create_metric_definition(m4)
        self.assertFalse(md4, 'Should have received an exception, metric with the same name was already created')

    def test_availability_creation(self):
        id_name = 'test.create.avail.{}'
        # Create availability metric
        # Fetch mterics and check that it did appear
        metric = Metric()
        metric.type = MetricType.Availability
        metric.id = id_name.format('1')
        self.client.create_metric_definition(metric)
        metric.data_retention = 90
        metric.id = id_name.format('2')

        self.client.create_metric_definition(metric)
        metric.data_retention = 94
        metric.id = id_name.format('3')
        metric.tags = {'env': 'qa'}

        self.client.create_metric_definition(metric)
        # Fetch metrics and check that it did appear
        m = self.client.query_metric_definitions(MetricType.Availability)
        self.assertEqual(3, len(m))

        avail_3 = self.find_metric(id_name.format('3'), m)
        self.assertEqual(94, avail_3.data_retention)

    def test_tags_modifications(self):
        # Create metric without tags
        metric_id = 'test.create.tags.1'
        m = Metric()
        m.id = metric_id
        m.type = MetricType.Gauge
        self.client.create_metric_definition(m)
        e = self.client.query_metric_tags(MetricType.Gauge, metric_id)
        self.assertIsNotNone(e)
        self.assertEqual({}, e)
        # Add tags
        self.client.update_metric_tags(MetricType.Gauge, metric_id, hostname='machine1', a='b')
        # Fetch metric - check for tags
        tags = self.client.query_metric_tags(MetricType.Gauge, metric_id)
        self.assertEqual(2, len(tags))
        self.assertEqual("b", tags['a'])
        # Delete some metric tags
        self.client.delete_metric_tags(MetricType.Gauge, metric_id, a='b', hostname='machine1')
        # Fetch metric - check that tags were deleted
        tags_2 = self.client.query_metric_tags(MetricType.Gauge, metric_id)
        self.assertEqual(0, len(tags_2))

    def test_tags_queries(self):
        m = Metric()
        for i in range(1, 9):
            m.id = 'test.query.tags.{}'.format(i)
            hostname = 'host{}'.format(i)
            m.tags = {'hostname': hostname, 'env': 'qa'}
            m.type = MetricType.Counter
            self.client.create_metric_definition(m)

        mds = self.client.query_metric_definitions(hostname='host[123]')
        self.assertEqual(3, len(mds))

        values = self.client.query_tag_values(MetricType.Counter, hostname='host.*', env='qa')
        self.assertEqual(2, len(values))

    def test_add_gauge_single(self):
        # Normal way
        value = float(4.35)
        datapoint = DataPoint()
        datapoint.timestamp = time_millis()
        datapoint.value = float(4.35)
        metric = Metric()
        metric.type = MetricType.Gauge
        metric.id = 'test.gauge./'
        metric.add_datapoint(datapoint)
        self.client.put(metric)

        # Fetch results
        data = self.client.query_metric(MetricType.Gauge, 'test.gauge./')
        self.assertEqual(float(data[0].value), value)

        # Shortcut method with tags
        self.client.push(MetricType.Gauge, 'test.gauge.single.tags', value)

        # Fetch results
        now = datetime.utcnow()
        data = self.client.query_metric(MetricType.Gauge, 'test.gauge.single.tags', start=now - timedelta(minutes=1),
                                        end=now)
        self.assertEqual(value, float(data[0].value))

    def test_add_availability_single(self):
        self.client.push(MetricType.Availability, 'test.avail.1', Availability.Up)
        self.client.push(MetricType.Availability, 'test.avail.2', 'down')

        up = self.client.query_metric(MetricType.Availability, 'test.avail.1')
        self.assertEqual(up[0].value, 'up')

        down = self.client.query_metric(MetricType.Availability, 'test.avail.2')
        self.assertEqual(down[0].value, Availability.Down)

    @unittest.skipIf(base.version != 'latest' and base.major_version == 0 and base.minor_version <= 15,
                     'Not supported in ' + base.version + ' version')
    def test_add_string_single(self):
        self.client.push(MetricType.String, 'test.string.1', "foo")
        data = self.client.query_metric(MetricType.String, 'test.string.1')
        self.assertEqual(data[0].value, 'foo')

    def test_add_gauge_multi_datapoint(self):
        metric_1v = DataPoint()
        metric_1v.value = float(1.45)
        metric_1v.timestamp = time_millis()
        metric_2v = DataPoint()
        metric_2v.value = float(2.00)
        metric_2v.timestamp = time_millis() - 2000
        metric = Metric()
        metric.type = MetricType.Gauge
        metric.id = 'test.gauge.multi'
        metric.add_datapoint(metric_1v)
        metric.add_datapoint(metric_2v)
        self.client.put(metric)
        data = self.client.query_metric(MetricType.Gauge, 'test.gauge.multi')
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0].value, float(1.45))
        self.assertEqual(data[1].value, float(2.00))

    def test_add_availability_multi_datapoint(self):
        t = time_millis()
        up = DataPoint({'value': Availability.Up, 'timestamp': (t - 2000)})
        down = DataPoint({'value': Availability.Down, 'timestamp': t})
        m = Metric({'type': MetricType.Availability, 'id': 'test.avail.multi'})
        m.add_datapoint(up)
        m.add_datapoint(down)
        self.client.put(m)
        data = self.client.query_metric(MetricType.Availability, 'test.avail.multi')

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0].value, Availability.Down)
        self.assertEqual(data[1].value, Availability.Up)

    def test_add_mixed_metrics_and_datapoints(self):
        t = time_millis()
        metric1 = DataPoint({'value': float(1.45), 'timestamp': (t - 2000)})
        metric1_2 = DataPoint({'value': float(2.00), 'timestamp': t})
        metric_multi = Metric({'type': MetricType.Gauge, 'id': 'test.multi.gauge.1'})
        metric_multi.add_datapoints(metric1, metric1_2)
        metric2_multi = Metric({'type': MetricType.Availability, 'id': 'test.multi.gauge.2'})
        metric2 = DataPoint({'value': Availability.Up, 'timestamp': t})
        metric2_multi.add_datapoint(metric2)
        self.client.put([metric_multi, metric2_multi])
        # Check that both were added correctly..
        metric1_data = self.client.query_metric(MetricType.Gauge, 'test.multi.gauge.1')
        metric2_data = self.client.query_metric(MetricType.Availability, 'test.multi.gauge.2')

        self.assertEqual(2, len(metric1_data))
        self.assertEqual(1, len(metric2_data))

    def test_query_options(self):
        # Create metric with two values
        t = datetime.utcnow()
        v1 = DataPoint({'value': float(1.45), 'timestamp': t})
        v2 = DataPoint({'value': float(2.00), 'timestamp': t - timedelta(seconds=2)})
        m = Metric({'type': MetricType.Gauge, 'id': 'test.query.gauge.1'})
        m.add_datapoints(v1, v2)
        self.client.put(m)
        d = self.client.query_metric(MetricType.Gauge, 'test.query.gauge.1')
        self.assertEqual(2, len(d))
        d = self.client.query_metric(MetricType.Gauge, 'test.query.gauge.1', start=(t - timedelta(seconds=1)))
        self.assertEqual(1, len(d))

    def test_stats_queries(self):
        m1 = Metric({'type': MetricType.Gauge, 'id': 'test.buckets.1',
                               'tags': {'units': 'bytes', 'env': 'unittest'}})
        m2 = Metric({'type': MetricType.Gauge, 'id': 'test.buckets.2',
                               'tags': {'units': 'bytes', 'env': 'unittest'}})

        self.client.create_metric_definition(m1)
        self.client.create_metric_definition(m2)

        t = time_millis()
        for i in range(0, 10):
            t = t - 1000
            val = 1.45 * i
            m1.add_datapoint(DataPoint({'value':val, 'timestamp': t}))

        self.client.put(m1)
        m2.add_datapoint(DataPoint({'value': 2.4}))

        self.client.put(m2)
        bp = self.client.query_metric_stats(MetricType.Gauge, 'test.buckets.1', buckets=1, tags=create_tags_filter(units='bytes', env='unittest'), percentiles=create_percentiles_filter(90.0, 99.0))

        self.assertEqual(1, len(bp), "Only one bucket was requested")
        self.assertEqual(10, bp[0].samples)
        self.assertEqual(2, len(bp[0].percentiles))

        now = datetime.utcfromtimestamp(t/1000)

        bp = self.client.query_metric_stats(MetricType.Gauge, 'test.buckets.1', bucketDuration=timedelta(seconds=2), start=now-timedelta(seconds=10), end=now, distinct=True)
        self.assertEqual(5, len(bp), "Single bucket is two seconds")

    def test_tenant_changing(self):
        m1 = Metric({'type': MetricType.Availability, 'id': 'test.tenant.avail.1'})
        self.client.create_metric_definition(m1)
        # Fetch metrics and check that it did appear
        m = self.client.query_metric_definitions(MetricType.Availability)
        self.assertEqual(1, len(m))

        tenant_old = self.client.tenant_id
        self.client.tenant(str(uuid.uuid4()))
        m = self.client.query_metric_definitions(MetricType.Availability)
        self.assertEqual(0, len(m))

    def test_query_semantic_version(self):
        major, minor = self.client.query_semantic_version()
        self.assertTrue(0 <= major <= 1000)
        self.assertTrue(0 <= minor <= 1000)

    def test_query_status(self):
        status = self.client.query_status()
        self.assertTrue('Implementation-Version' in status)

    def test_get_metrics_raw_url(self):
        self.client.legacy_api = True
        url = self.client._get_metrics_raw_url('some.key')
        self.assertEqual('some.key/data', url)

        self.client.legacy_api = False
        url = self.client._get_metrics_raw_url('some.key')
        self.assertEqual('some.key/raw', url)

    def test_get_metrics_stats_url(self):
        self.client.legacy_api = True
        url = self.client._get_metrics_stats_url('some.key')
        self.assertEqual('some.key/data', url)

        self.client.legacy_api = False
        url = self.client._get_metrics_stats_url('some.key')
        self.assertEqual('some.key/stats', url)

    def test_timedelta_to_duration_string(self):
        s = timedelta_to_duration(timedelta(hours=1, minutes=3, seconds=4))
        self.assertEqual('3784s', s)

        s = timedelta_to_duration(timedelta(hours=1, seconds=4))
        self.assertEqual('3604s', s)

        s = timedelta_to_duration(timedelta(days=4))
        self.assertEqual('345600s', s)


class MetricsTestCaseDictionaries(TestMetricFunctionsBase):
    """
    Test metric functionality, both adding definition and querying for definition,
    as well as adding new gauge and availability metrics.
    Metric definition creation should also test fetching the definition, while
    metric inserts should test also fetching the metric data.
    """

    def find_metric(self, name, definitions):
        for defin in definitions:
            if defin['id'] == name:
                return defin

    def test_gauge_creation(self):
        """
        Test creating gauge metric definitions with different tags and definition.
        """
        # Create gauge metrics with empty details and added details
        id_name = 'test.create.gauge.{}'

        md1 = self.client.create_metric_definition(MetricType.Gauge, id_name.format('1'))
        md2 = self.client.create_metric_definition(MetricType.Gauge, id_name.format('2'), dataRetention=90)
        md3 = self.client.create_metric_definition(MetricType.Gauge, id_name.format('3'), dataRetention=90,
                                                   units='bytes', env='qa')
        self.assertTrue(md1)
        self.assertTrue(md2)
        self.assertTrue(md3)

        # Fetch metrics definition and check that the ones we created appeared also
        m = self.client.query_metric_definitions(MetricType.Gauge)
        self.assertEqual(3, len(m))
        self.assertEqual(self.test_tenant, m[0]['tenantId'])

        md3_f = self.find_metric(id_name.format('3'), m)

        self.assertEqual('bytes', md3_f['tags']['units'])

        # This is what the returned dict should look like
        expect = [
            {'dataRetention': 7, 'type': 'gauge', 'id': 'test.create.gauge.1',
             'tenantId': self.test_tenant},
            {'dataRetention': 90, 'type': 'gauge', 'id': 'test.create.gauge.2', 'tenantId': self.test_tenant},
            {'tags': {'units': 'bytes', 'env': 'qa'},
             'id': 'test.create.gauge.3', 'dataRetention': 90, 'type': 'gauge', 'tenantId': self.test_tenant}]

        for e in expect:
            self.assertIn(e, m)

        # Lets try creating a duplicate metric
        md4 = self.client.create_metric_definition(MetricType.Gauge, id_name.format('1'))
        self.assertFalse(md4, 'Should have received an exception, metric with the same name was already created')

    def test_availability_creation(self):
        id_name = 'test.create.avail.{}'
        # Create availability metric
        # Fetch mterics and check that it did appear
        self.client.create_metric_definition(MetricType.Availability, id_name.format('1'))
        self.client.create_metric_definition(MetricType.Availability, id_name.format('2'), dataRetention=90)
        self.client.create_metric_definition(MetricType.Availability, id_name.format('3'), dataRetention=94, env='qa')
        # Fetch metrics and check that it did appear
        m = self.client.query_metric_definitions(MetricType.Availability)
        self.assertEqual(3, len(m))

        avail_3 = self.find_metric(id_name.format('3'), m)
        self.assertEqual(94, avail_3['dataRetention'])

    def test_tags_modifications(self):
        m = 'test.create.tags.1'
        # Create metric without tags
        self.client.create_metric_definition(MetricType.Gauge, m)
        e = self.client.query_metric_tags(MetricType.Gauge, m)
        self.assertIsNotNone(e)
        self.assertEqual({}, e)
        # Add tags
        self.client.update_metric_tags(MetricType.Gauge, m, hostname='machine1', a='b')
        # Fetch metric - check for tags
        tags = self.client.query_metric_tags(MetricType.Gauge, m)
        self.assertEqual(2, len(tags))
        self.assertEqual("b", tags['a'])
        # Delete some metric tags
        self.client.delete_metric_tags(MetricType.Gauge, m, a='b', hostname='machine1')
        # Fetch metric - check that tags were deleted
        tags_2 = self.client.query_metric_tags(MetricType.Gauge, m)
        self.assertEqual(0, len(tags_2))

    def test_tags_queries(self):
        for i in range(1, 9):
            m_id = 'test.query.tags.{}'.format(i)
            hostname = 'host{}'.format(i)
            self.client.create_metric_definition(MetricType.Counter, m_id, hostname=hostname, env='qa')

        mds = self.client.query_metric_definitions(hostname='host[123]')
        self.assertEqual(3, len(mds))

        values = self.client.query_tag_values(MetricType.Counter, hostname='host.*', env='qa')
        self.assertEqual(2, len(values))

    def test_add_gauge_single(self):
        # Normal way
        value = float(4.35)
        datapoint = create_datapoint(value, time_millis())
        metric = create_metric(MetricType.Gauge, 'test.gauge./', datapoint)
        self.client.put(metric)

        # Fetch results
        data = self.client.query_metric(MetricType.Gauge, 'test.gauge./')
        self.assertEqual(float(data[0]['value']), value)

        # Shortcut method with tags
        self.client.push(MetricType.Gauge, 'test.gauge.single.tags', value)

        # Fetch results
        now = datetime.utcnow()
        data = self.client.query_metric(MetricType.Gauge, 'test.gauge.single.tags', start=now-timedelta(minutes = 1), end=now)
        self.assertEqual(value, float(data[0]['value']))

    def test_add_availability_single(self):
        self.client.push(MetricType.Availability, 'test.avail.1', Availability.Up)
        self.client.push(MetricType.Availability, 'test.avail.2', 'down')

        up = self.client.query_metric(MetricType.Availability, 'test.avail.1')
        self.assertEqual(up[0]['value'], 'up')

        down = self.client.query_metric(MetricType.Availability, 'test.avail.2')
        self.assertEqual(down[0]['value'], Availability.Down)

    @unittest.skipIf(base.version != 'latest' and base.major_version == 0 and base.minor_version <= 15,
                     'Not supported in ' + base.version + ' version')
    def test_add_string_single(self):
        self.client.push(MetricType.String, 'test.string.1', "foo")
        data = self.client.query_metric(MetricType.String, 'test.string.1')
        self.assertEqual(data[0]["value"], 'foo')

    def test_add_gauge_multi_datapoint(self):
        metric_1v = create_datapoint(float(1.45))
        metric_2v = create_datapoint(float(2.00), (time_millis() - 2000))

        metric = create_metric(MetricType.Gauge, 'test.gauge.multi', [metric_1v, metric_2v])
        self.client.put(metric)

        data = self.client.query_metric(MetricType.Gauge, 'test.gauge.multi')
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['value'], float(1.45))
        self.assertEqual(data[1]['value'], float(2.00))

    def test_add_availability_multi_datapoint(self):
        t = time_millis()
        up = create_datapoint('up', (t - 2000))
        down = create_datapoint('down', t)

        m = create_metric(MetricType.Availability, 'test.avail.multi', [up, down])

        self.client.put(m)
        data = self.client.query_metric(MetricType.Availability, 'test.avail.multi')

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['value'], 'down')
        self.assertEqual(data[1]['value'], 'up')

    def test_add_mixed_metrics_and_datapoints(self):
        metric1 = create_datapoint(float(1.45))
        metric1_2 = create_datapoint(float(2.00), (time_millis() - 2000))

        metric_multi = create_metric(MetricType.Gauge, 'test.multi.gauge.1', [metric1, metric1_2])

        metric2 = create_datapoint(Availability.Up)
        metric2_multi = create_metric(MetricType.Availability, 'test.multi.gauge.2', [metric2])

        self.client.put([metric_multi, metric2_multi])

        # Check that both were added correctly..
        metric1_data = self.client.query_metric(MetricType.Gauge, 'test.multi.gauge.1')
        metric2_data = self.client.query_metric(MetricType.Availability, 'test.multi.gauge.2')

        self.assertEqual(2, len(metric1_data))
        self.assertEqual(1, len(metric2_data))

    def test_query_options(self):
        # Create metric with two values
        t = datetime.utcnow()
        v1 = create_datapoint(float(1.45), t)
        v2 = create_datapoint(float(2.00), (t - timedelta(seconds=2)))

        m = create_metric(MetricType.Gauge, 'test.query.gauge.1', [v1, v2])
        self.client.put(m)

        # Query first without limitations
        d = self.client.query_metric(MetricType.Gauge, 'test.query.gauge.1')
        self.assertEqual(2, len(d))

        # Query for data which has start time limitation
        d = self.client.query_metric(MetricType.Gauge, 'test.query.gauge.1', start=(t - timedelta(seconds=1)))
        self.assertEqual(1, len(d))

    def test_stats_queries(self):
        self.client.create_metric_definition(MetricType.Gauge, 'test.buckets.1', units='bytes', env='unittest')
        self.client.create_metric_definition(MetricType.Gauge, 'test.buckets.2', units='bytes', env='unittest')

        t = time_millis()
        dps = []

        for i in range(0, 10):
            t = t - 1000
            val = 1.45 * i
            dps.append(create_datapoint(val, timestamp=t))

        self.client.put(create_metric(MetricType.Gauge, 'test.buckets.1', dps))
        self.client.put(create_metric(MetricType.Gauge, 'test.buckets.2', [create_datapoint(2.4)]))

        # Read single stats bucket
        bp = self.client.query_metric_stats(MetricType.Gauge, 'test.buckets.1', buckets=1, tags=create_tags_filter(units='bytes', env='unittest'), percentiles=create_percentiles_filter(90.0, 99.0))

        self.assertEqual(1, len(bp), "Only one bucket was requested")
        self.assertEqual(10, bp[0]['samples'])
        self.assertEqual(2, len(bp[0]['percentiles']))

        now = datetime.utcfromtimestamp(t/1000)

        bp = self.client.query_metric_stats(MetricType.Gauge, 'test.buckets.1', bucketDuration=timedelta(seconds=2), start=now-timedelta(seconds=10), end=now, distinct=True)
        self.assertEqual(5, len(bp), "Single bucket is two seconds")

    def test_tenant_changing(self):
        self.client.create_metric_definition(MetricType.Availability, 'test.tenant.avail.1')
        # Fetch metrics and check that it did appear
        m = self.client.query_metric_definitions(MetricType.Availability)
        self.assertEqual(1, len(m))

        tenant_old = self.client.tenant_id
        self.client.tenant(str(uuid.uuid4()))
        m = self.client.query_metric_definitions(MetricType.Availability)
        self.assertEqual(0, len(m))

    def test_query_semantic_version(self):
        major, minor = self.client.query_semantic_version()
        self.assertTrue(0 <= major <= 1000)
        self.assertTrue(0 <= minor <= 1000)

    def test_query_status(self):
        status = self.client.query_status()
        self.assertTrue('Implementation-Version' in status)

    def test_get_metrics_raw_url(self):
        self.client.legacy_api = True
        url = self.client._get_metrics_raw_url('some.key')
        self.assertEqual('some.key/data', url)

        self.client.legacy_api = False
        url = self.client._get_metrics_raw_url('some.key')
        self.assertEqual('some.key/raw', url)

    def test_get_metrics_stats_url(self):
        self.client.legacy_api = True
        url = self.client._get_metrics_stats_url('some.key')
        self.assertEqual('some.key/data', url)

        self.client.legacy_api = False
        url = self.client._get_metrics_stats_url('some.key')
        self.assertEqual('some.key/stats', url)

    def test_timedelta_to_duration_string(self):
        s = timedelta_to_duration(timedelta(hours=1, minutes=3, seconds=4))
        self.assertEqual('3784s', s)

        s = timedelta_to_duration(timedelta(hours=1, seconds=4))
        self.assertEqual('3604s', s)

        s = timedelta_to_duration(timedelta(days=4))
        self.assertEqual('345600s', s)

if __name__ == '__main__':
    unittest.main()
