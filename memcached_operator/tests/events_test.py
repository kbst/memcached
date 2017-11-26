from unittest.mock import patch, call, MagicMock
from copy import deepcopy

from ..memcached_operator.events import (event_switch, add, modify, delete)
from ..memcached_operator.kubernetes_resources import (
    get_mcrouter_service_object,
    get_memcached_service_object)

class TestEvents():
    def setUp(self):
        self.base_event = {'type': '', 'object': {}}
        self.name = 'testname123'
        self.namespace = 'testnamespace456'
        self.cluster_object = {'metadata':{'name': self.name,
                                           'namespace': self.namespace}}

    @patch('memcached_operator.memcached_operator.events.delete')
    @patch('memcached_operator.memcached_operator.events.modify')
    @patch('memcached_operator.memcached_operator.events.add')
    @patch('memcached_operator.memcached_operator.events.logging')
    def test_malformed_event(self, mock_logging, mock_add, mock_modify, mock_delete):
        event = {}

        event_switch(event)

        mock_logging.warning.assert_called_once_with('malformed event: {}')
        assert mock_add.called is False
        assert mock_modify.called is False
        assert mock_delete.called is False

    @patch('memcached_operator.memcached_operator.events.delete')
    @patch('memcached_operator.memcached_operator.events.modify')
    @patch('memcached_operator.memcached_operator.events.add')
    def test_add_event(self, mock_add, mock_modify, mock_delete):
        event = deepcopy(self.base_event)
        event['type'] = 'ADDED'

        event_switch(event)

        mock_add.assert_called_once_with({})
        assert mock_modify.called is False
        assert mock_delete.called is False

    @patch('memcached_operator.memcached_operator.events.delete')
    @patch('memcached_operator.memcached_operator.events.modify')
    @patch('memcached_operator.memcached_operator.events.add')
    def test_modify_event(self, mock_add, mock_modify, mock_delete):
        event = deepcopy(self.base_event)
        event['type'] = 'MODIFIED'

        event_switch(event)

        assert mock_add.called is False
        mock_modify.assert_called_once_with({})
        assert mock_delete.called is False

    @patch('memcached_operator.memcached_operator.events.delete')
    @patch('memcached_operator.memcached_operator.events.modify')
    @patch('memcached_operator.memcached_operator.events.add')
    def test_delete_event(self, mock_add, mock_modify, mock_delete):
        event = deepcopy(self.base_event)
        event['type'] = 'DELETED'

        event_switch(event)

        assert mock_add.called is False
        assert mock_modify.called is False
        mock_delete.assert_called_once_with({})

    @patch('memcached_operator.memcached_operator.events.delete')
    @patch('memcached_operator.memcached_operator.events.modify')
    @patch('memcached_operator.memcached_operator.events.add')
    def test_unknown_event(self, mock_add, mock_modify, mock_delete):
        event = deepcopy(self.base_event)
        event['type'] = 'UNKNOWN'

        event_switch(event)

        assert mock_add.called is False
        assert mock_modify.called is False
        assert mock_delete.called is False

    @patch('memcached_operator.memcached_operator.events.create_memcached_deployment')
    @patch('memcached_operator.memcached_operator.events.create_mcrouter_deployment')
    @patch('memcached_operator.memcached_operator.events.create_service')
    def test_add(self, mock_create_service, mock_create_mcrouter_deployment, mock_create_memcached_deployment):
        add(self.cluster_object)

        create_service_calls = [
            call(get_mcrouter_service_object(self.cluster_object)),
            call(get_memcached_service_object(self.cluster_object))]
        mock_create_service.assert_has_calls(create_service_calls)
        mock_create_memcached_deployment.assert_called_once_with(self.cluster_object)
        mock_create_mcrouter_deployment.assert_called_once_with(self.cluster_object)

    @patch('memcached_operator.memcached_operator.events.update_memcached_deployment')
    @patch('memcached_operator.memcached_operator.events.update_mcrouter_deployment')
    @patch('memcached_operator.memcached_operator.events.update_service')
    def test_modify(self, mock_update_service, mock_update_mcrouter_deployment, mock_update_memcached_deployment):
        modify(self.cluster_object)

        update_service_calls = [
            call(get_mcrouter_service_object(self.cluster_object)),
            call(get_memcached_service_object(self.cluster_object))]
        mock_update_service.assert_has_calls(update_service_calls)
        mock_update_memcached_deployment.assert_called_once_with(self.cluster_object)
        mock_update_mcrouter_deployment.assert_called_once_with(self.cluster_object)

    @patch('memcached_operator.memcached_operator.events.delete_deployment')
    @patch('memcached_operator.memcached_operator.events.delete_service')
    def test_delete(self, mock_delete_service, mock_delete_deployment):
        delete(self.cluster_object)

        delete_service_calls = [
            call(self.name, self.namespace),
            call('{}-backend'.format(self.name), self.namespace)]
        mock_delete_service.assert_has_calls(delete_service_calls)
        delete_deployment_calls = [
            call(self.name, self.namespace),
            call('{}-router'.format(self.name), self.namespace)]
        mock_delete_deployment.assert_has_calls(delete_deployment_calls)
