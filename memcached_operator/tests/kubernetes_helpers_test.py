from unittest.mock import patch, call
from copy import deepcopy
from random import randint

from kubernetes import client

from ..memcached_operator.kubernetes_helpers import (
    create_service,
    update_service,
    delete_service,
    create_memcached_deployment,
    create_mcrouter_deployment,
    update_memcached_deployment,
    update_mcrouter_deployment,
    delete_deployment,
    reap_deployment,
    delete_replica_set)
from ..memcached_operator.kubernetes_resources import (
    get_mcrouter_service_object,
    get_memcached_service_object,
    get_default_label_selector,
    get_memcached_deployment_object,
    get_mcrouter_deployment_object)

BASE_CLUSTER_OBJECT = {'metadata': {'name': 'testname123',
                                       'namespace': 'testnamespace456'}}

class TestCreateService():
    def setUp(self):
        self.cluster_object = BASE_CLUSTER_OBJECT
        self.name = self.cluster_object['metadata']['name']
        self.namespace = self.cluster_object['metadata']['namespace']

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.CoreV1Api.create_namespaced_service', return_value=client.V1Service())
    def test_success(self, mock_create_namespaced_service, mock_logging):
        service_object = get_mcrouter_service_object(self.cluster_object)

        service = create_service(service_object)

        mock_create_namespaced_service.assert_called_once_with(
            self.namespace, service_object)
        mock_logging.info.assert_called_once_with(
            'created svc/{} in ns/{}'.format(self.name, self.namespace))
        assert isinstance(service, client.V1Service)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.CoreV1Api.create_namespaced_service', side_effect=client.rest.ApiException(status=409))
    def test_already_exists(self, mock_create_namespaced_service, mock_logging):
        service_object = get_mcrouter_service_object(self.cluster_object)

        service = create_service(service_object)

        mock_logging.debug.assert_called_once_with(
            'svc/{} in ns/{} already exists'.format(self.name, self.namespace))
        assert service is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.CoreV1Api.create_namespaced_service', side_effect=client.rest.ApiException(status=500))
    def test_other_rest_exception(self, mock_create_namespaced_service, mock_logging):
        service_object = get_mcrouter_service_object(self.cluster_object)

        service = create_service(service_object)

        assert mock_logging.exception.called is True
        assert service is False

class TestUpdateService():
    def setUp(self):
        self.cluster_object = BASE_CLUSTER_OBJECT
        self.name = self.cluster_object['metadata']['name']
        self.namespace = self.cluster_object['metadata']['namespace']

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.CoreV1Api.patch_namespaced_service', return_value=client.V1Service())
    def test_success(self, mock_patch_namespaced_service, mock_logging):
        service_object = get_mcrouter_service_object(self.cluster_object)

        service = update_service(service_object)

        mock_patch_namespaced_service.assert_called_once_with(
            self.name, self.namespace, service_object)
        mock_logging.info.assert_called_once_with(
            'updated svc/{} in ns/{}'.format(self.name, self.namespace))
        assert isinstance(service, client.V1Service)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.CoreV1Api.patch_namespaced_service', side_effect=client.rest.ApiException(status=500))
    def test_rest_exception(self, mock_patch_namespaced_service, mock_logging):
        service_objects = [
            get_mcrouter_service_object(self.cluster_object),
            get_memcached_service_object(self.cluster_object)]

        for service_object in service_objects:
            service = update_service(service_object)

            assert mock_logging.exception.called is True
            assert service is False

class TestDeleteService():
    def setUp(self):
        self.cluster_object = BASE_CLUSTER_OBJECT
        self.name = self.cluster_object['metadata']['name']
        self.namespace = self.cluster_object['metadata']['namespace']

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.CoreV1Api.delete_namespaced_service')
    def test_success(self, mock_delete_namespaced_service, mock_logging):
        service = delete_service(self.name, self.namespace)

        mock_delete_namespaced_service.assert_called_once_with(
            self.name, self.namespace)
        mock_logging.info.assert_called_once_with(
            'deleted svc/{} from ns/{}'.format(self.name, self.namespace))
        assert service is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.CoreV1Api.delete_namespaced_service', side_effect=client.rest.ApiException(status=500))
    def test_rest_exception(self, mock_delete_namespaced_service, mock_logging):
        service = delete_service(self.name, self.namespace)

        assert mock_logging.exception.called is True
        assert service is False


DEPLOYMENT_CLUSTER_OBJECT = {'metadata': {'name': 'testname123',
                                          'namespace': 'testnamespace456'},
                             'memcached': {'replicas': 2},
                             'mcrouter': {'replicas': 2}
                            }


class TestCreateMemcachedDeployment():
    def setUp(self):
        self.cluster_object = DEPLOYMENT_CLUSTER_OBJECT
        self.name = self.cluster_object['metadata']['name']
        self.namespace = self.cluster_object['metadata']['namespace']
        self.replicas = self.cluster_object['memcached']['replicas']

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.create_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    def test_success(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_memcached_deployment(self.cluster_object)

        body = get_memcached_deployment_object(self.cluster_object)
        mock_create_namespaced_deployment.assert_called_once_with(
            self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'created deploy/{} in ns/{}'.format(self.name, self.namespace))
        assert isinstance(deployment, client.AppsV1beta1Deployment)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.create_namespaced_deployment', side_effect=client.rest.ApiException(status=409))
    def test_already_exists(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_memcached_deployment(self.cluster_object)

        mock_logging.debug.assert_called_once_with(
            'deploy/{} in ns/{} already exists'.format(self.name, self.namespace))
        assert deployment is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.create_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    def test_other_rest_exception(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_memcached_deployment(self.cluster_object)

        assert mock_logging.exception.called is True
        assert deployment is False


class TestCreateMcrouterDeployment():
    def setUp(self):
        self.cluster_object = DEPLOYMENT_CLUSTER_OBJECT
        self.name = self.cluster_object['metadata']['name']
        self.namespace = self.cluster_object['metadata']['namespace']
        self.replicas = self.cluster_object['mcrouter']['replicas']

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.create_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    def test_success(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_mcrouter_deployment(self.cluster_object)

        body = get_mcrouter_deployment_object(self.cluster_object)
        mock_create_namespaced_deployment.assert_called_once_with(
            self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'created deploy/{}-router in ns/{}'.format(self.name, self.namespace))
        assert isinstance(deployment, client.AppsV1beta1Deployment)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.create_namespaced_deployment', side_effect=client.rest.ApiException(status=409))
    def test_already_exists(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_mcrouter_deployment(self.cluster_object)

        mock_logging.debug.assert_called_once_with(
            'deploy/{}-router in ns/{} already exists'.format(self.name, self.namespace))
        assert deployment is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.create_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    def test_other_rest_exception(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_mcrouter_deployment(self.cluster_object)

        assert mock_logging.exception.called is True
        assert deployment is False


class TestUpdateMemcachedDeployment():
    def setUp(self):
        self.cluster_object = DEPLOYMENT_CLUSTER_OBJECT
        self.name = self.cluster_object['metadata']['name']
        self.namespace = self.cluster_object['metadata']['namespace']
        self.replicas = self.cluster_object['memcached']['replicas']

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    def test_success(self, mock_patch_namespaced_deployment, mock_logging):
        deployment = update_memcached_deployment(self.cluster_object)

        body = get_memcached_deployment_object(self.cluster_object)
        mock_patch_namespaced_deployment.assert_called_once_with(
            self.name, self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'updated deploy/{} in ns/{}'.format(self.name, self.namespace))
        assert isinstance(deployment, client.AppsV1beta1Deployment)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    def test_rest_exception(self, mock_patch_namespaced_deployment, mock_logging):
        deployment = update_memcached_deployment(self.cluster_object)

        assert mock_logging.exception.called is True
        assert deployment is False


class TestUpdateMcrouterDeployment():
    def setUp(self):
        self.cluster_object = DEPLOYMENT_CLUSTER_OBJECT
        self.name = self.cluster_object['metadata']['name']
        self.namespace = self.cluster_object['metadata']['namespace']
        self.replicas = self.cluster_object['mcrouter']['replicas']

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    def test_success(self, mock_patch_namespaced_deployment, mock_logging):
        deployment = update_mcrouter_deployment(self.cluster_object)

        body = get_mcrouter_deployment_object(self.cluster_object)
        mock_patch_namespaced_deployment.assert_called_once_with(
            '{}-router'.format(self.name), self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'updated deploy/{}-router in ns/{}'.format(self.name, self.namespace))
        assert isinstance(deployment, client.AppsV1beta1Deployment)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    def test_rest_exception(self, mock_patch_namespaced_deployment, mock_logging):
        deployment = update_mcrouter_deployment(self.cluster_object)

        assert mock_logging.exception.called is True
        assert deployment is False


class TestDeleteDeployment():
    def setUp(self):
        self.name = 'testdeployment123'
        self.namespace = 'testnamespace456'

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.delete_namespaced_deployment')
    def test_success(self, mock_delete_namespaced_deployment, mock_logging):
        response = delete_deployment(self.name, self.namespace)

        body = client.V1DeleteOptions()
        mock_delete_namespaced_deployment.assert_called_once_with(
            self.name, self.namespace, body, orphan_dependents=False)
        mock_logging.info.assert_called_once_with(
            'deleted deploy/{} from ns/{}'.format(self.name, self.namespace))
        assert response is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.delete_namespaced_deployment', side_effect=client.rest.ApiException(status=404))
    def test_nonexistent(self, mock_delete_namespaced_service, mock_logging):
        response = delete_deployment(self.name, self.namespace)

        mock_logging.debug.assert_called_once_with(
            'not deleting nonexistent deploy/{} from ns/{}'.format(self.name, self.namespace))
        assert response is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.delete_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    def test_rest_exception(self, mock_delete_namespaced_service, mock_logging):
        response = delete_deployment(self.name, self.namespace)

        assert mock_logging.exception.called is True
        assert response is False


class TestReapDeployment():
    def setUp(self):
        self.name = 'testdeployment123'
        self.namespace = 'testnamespace456'

        rs = client.V1beta1ReplicaSet()
        rs.metadata = client.V1ObjectMeta(
            name='{}-1234567890'.format(self.name), namespace=self.namespace)
        rs_list = client.V1beta1ReplicaSetList(items=[rs])
        self.correct_list_replica_set = rs_list

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.read_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_success(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_read_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_logging):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        mock_list_namespaced_replica_set.return_value = list_return_value

        # Mock read return value
        read_return_value = deepcopy(list_return_value.items[0])
        read_return_value.status = client.V1beta1ReplicaSetStatus(replicas=0)
        mock_read_namespaced_replica_set.return_value = read_return_value

        # Mock delete replica set return value
        mock_delete_replica_set.return_value = True

        # Mock delete deployment return value
        mock_delete_deployment.return_value = True

        response = reap_deployment(self.name, self.namespace)

        body = {'spec': {
                    'replicas': 0,
                    'revision_history_limit': 0,
                    'paused': True}}
        mock_patch_namespaced_deployment.assert_called_once_with(
            self.name, self.namespace, body)

        label_selector = get_default_label_selector(name=self.name)
        mock_list_namespaced_replica_set.assert_called_once_with(
            self.namespace, label_selector=label_selector)

        rs_name = self.correct_list_replica_set.items[0].metadata.name
        mock_read_namespaced_replica_set.assert_called_once_with(
            rs_name, self.namespace)

        mock_delete_replica_set.assert_called_once_with(rs_name, self.namespace)
        mock_delete_deployment.assert_called_once_with(self.name, self.namespace)

        assert response is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.read_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_patch_404(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_read_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_logging):
        # Mock patch deployment side effect
        mock_patch_namespaced_deployment.side_effect = client.rest.ApiException(status=404)

        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        mock_list_namespaced_replica_set.return_value = list_return_value

        # Mock read return value
        read_return_value = deepcopy(list_return_value.items[0])
        read_return_value.status = client.V1beta1ReplicaSetStatus(replicas=0)
        mock_read_namespaced_replica_set.return_value = read_return_value

        # Mock delete replica set return value
        mock_delete_replica_set.return_value = True

        # Mock delete deployment return value
        mock_delete_deployment.return_value = True

        response = reap_deployment(self.name, self.namespace)

        mock_logging.debug.assert_called_once_with('can not gracefully delete nonexistent deploy/{} from ns/{}'.format(self.name, self.namespace))

        rs_name = self.correct_list_replica_set.items[0].metadata.name
        mock_delete_replica_set.assert_called_once_with(rs_name, self.namespace)

        mock_delete_deployment.assert_called_once_with(self.name, self.namespace)

        assert response is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_patch_500(self, mock_patch_namespaced_deployment, mock_delete_replica_set, mock_delete_deployment, mock_logging):
        # Mock patch deployment side effect
        mock_patch_namespaced_deployment.side_effect = client.rest.ApiException(status=500)

        response = reap_deployment(self.name, self.namespace)

        assert mock_logging.exception.called is True
        assert response is False
        assert mock_delete_replica_set.called is False
        assert mock_delete_deployment.called is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_no_related_replicasets(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_logging):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        list_return_value.items = []
        mock_list_namespaced_replica_set.return_value = list_return_value

        response = reap_deployment(self.name, self.namespace)

        mock_logging.warning.assert_called_once_with('found {} replicasets. Refusing to reap deploy/{} from ns/{}'.format(0, self.name, self.namespace))
        assert response is False
        assert mock_delete_replica_set.called is False
        assert mock_delete_deployment.called is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_many_related_replicasets(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_logging):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        item_count = randint(2, 7)
        list_return_value.items = [None] * item_count
        mock_list_namespaced_replica_set.return_value = list_return_value

        response = reap_deployment(self.name, self.namespace)

        mock_logging.warning.assert_called_once_with('found {} replicasets. Refusing to reap deploy/{} from ns/{}'.format(item_count, self.name, self.namespace))
        assert response is False
        assert mock_delete_replica_set.called is False
        assert mock_delete_deployment.called is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.sleep')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.read_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_no_more_retries(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_read_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_sleep):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        mock_list_namespaced_replica_set.return_value = list_return_value

        # Mock read return value
        read_return_value = deepcopy(list_return_value.items[0])
        read_return_value.status = client.V1beta1ReplicaSetStatus(replicas=0)
        mock_read_namespaced_replica_set.return_value = read_return_value

        # Mock delete replica set return value
        mock_delete_replica_set.return_value = False

        response = reap_deployment(self.name, self.namespace)

        sleep_calls = [call(0), call(2), call(4), call(6), call(8)]
        mock_sleep.assert_has_calls(sleep_calls)

        assert response is False
        assert mock_delete_replica_set.call_count == 5
        assert mock_delete_deployment.called is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.sleep')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.read_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_read_replica_set_404(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_read_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_sleep):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        mock_list_namespaced_replica_set.return_value = list_return_value

        # Mock read side effect
        mock_read_namespaced_replica_set.side_effect = client.rest.ApiException(status=404)

        response = reap_deployment(self.name, self.namespace)

        assert response is True
        assert mock_delete_replica_set.called is False
        assert mock_delete_deployment.called is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.sleep')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.read_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_read_replica_set_500(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_read_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_sleep, mock_logging):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        mock_list_namespaced_replica_set.return_value = list_return_value

        # Mock read side effect
        mock_read_namespaced_replica_set.side_effect = client.rest.ApiException(status=500)

        response = reap_deployment(self.name, self.namespace)

        assert response is False
        assert mock_logging.exception.called is True
        assert mock_delete_replica_set.called is False
        assert mock_delete_deployment.called is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.sleep')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.read_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_rs_delete_only_if_zero_replicas(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_read_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_sleep, mock_logging):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        mock_list_namespaced_replica_set.return_value = list_return_value

        # Mock read return value
        read_return_value = deepcopy(list_return_value.items[0])
        # Set number of replicas != 0
        read_return_value.status = client.V1beta1ReplicaSetStatus(replicas=1)

        mock_read_namespaced_replica_set.return_value = read_return_value

        response = reap_deployment(self.name, self.namespace)

        assert response is False
        assert mock_delete_replica_set.called is False
        assert mock_delete_deployment.called is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.sleep')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.read_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_delete_deploy_only_after_rs(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_read_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_sleep, mock_logging):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        mock_list_namespaced_replica_set.return_value = list_return_value

        # Mock read return value
        read_return_value = deepcopy(list_return_value.items[0])
        read_return_value.status = client.V1beta1ReplicaSetStatus(replicas=0)
        mock_read_namespaced_replica_set.return_value = read_return_value

        # Mock delete replica set return value
        mock_delete_replica_set.return_value = False

        response = reap_deployment(self.name, self.namespace)

        assert response is False
        assert mock_delete_replica_set.called is True
        assert mock_delete_deployment.called is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.sleep')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_deployment')
    @patch('memcached_operator.memcached_operator.kubernetes_helpers.delete_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.read_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.list_namespaced_replica_set')
    @patch('kubernetes.client.AppsV1beta2Api.patch_namespaced_deployment')
    def test_both_deletes_end_loop(self, mock_patch_namespaced_deployment, mock_list_namespaced_replica_set, mock_read_namespaced_replica_set, mock_delete_replica_set, mock_delete_deployment, mock_sleep, mock_logging):
        # Mock list return value
        list_return_value = deepcopy(self.correct_list_replica_set)
        mock_list_namespaced_replica_set.return_value = list_return_value

        # Mock read return value
        read_return_value = deepcopy(list_return_value.items[0])
        read_return_value.status = client.V1beta1ReplicaSetStatus(replicas=0)
        mock_read_namespaced_replica_set.return_value = read_return_value

        # Mock delete replica set return value
        mock_delete_replica_set.return_value = True

        # Mock delete deployment return value
        mock_delete_replica_set.return_value = True

        response = reap_deployment(self.name, self.namespace)

        mock_sleep.assert_called_once_with(0)
        assert response is True
        assert mock_delete_replica_set.called is True
        assert mock_delete_deployment.called is True


class TestDeleteReplicaSet():
    def setUp(self):
        self.name = 'testdeployment123-1234567890'
        self.namespace = 'testnamespace456'

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.delete_namespaced_replica_set')
    def test_success(self, mock_delete_namespaced_deployment, mock_logging):
        response = delete_replica_set(self.name, self.namespace)

        body = client.V1DeleteOptions()
        mock_delete_namespaced_deployment.assert_called_once_with(
            self.name, self.namespace, body, orphan_dependents=False)
        mock_logging.info.assert_called_once_with(
            'deleted rs/{} from ns/{}'.format(self.name, self.namespace))
        assert response is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.delete_namespaced_replica_set', side_effect=client.rest.ApiException(status=404))
    def test_nonexistent(self, mock_delete_namespaced_service, mock_logging):
        response = delete_replica_set(self.name, self.namespace)

        mock_logging.debug.assert_called_once_with(
            'not deleting nonexistent rs/{} from ns/{}'.format(self.name, self.namespace))
        assert response is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta2Api.delete_namespaced_replica_set', side_effect=client.rest.ApiException(status=500))
    def test_rest_exception(self, mock_delete_namespaced_service, mock_logging):
        response = delete_replica_set(self.name, self.namespace)

        assert mock_logging.exception.called is True
        assert response is False
