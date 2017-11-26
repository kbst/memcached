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
    delete_deployment)
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
    @patch('kubernetes.client.AppsV1beta1Api.create_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    def test_success(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_memcached_deployment(self.cluster_object)

        body = get_memcached_deployment_object(self.cluster_object)
        mock_create_namespaced_deployment.assert_called_once_with(
            self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'created deploy/{} in ns/{}'.format(self.name, self.namespace))
        assert isinstance(deployment, client.AppsV1beta1Deployment)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.create_namespaced_deployment', side_effect=client.rest.ApiException(status=409))
    def test_already_exists(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_memcached_deployment(self.cluster_object)

        mock_logging.debug.assert_called_once_with(
            'deploy/{} in ns/{} already exists'.format(self.name, self.namespace))
        assert deployment is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.create_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
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
    @patch('kubernetes.client.AppsV1beta1Api.create_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    def test_success(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_mcrouter_deployment(self.cluster_object)

        body = get_mcrouter_deployment_object(self.cluster_object)
        mock_create_namespaced_deployment.assert_called_once_with(
            self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'created deploy/{}-router in ns/{}'.format(self.name, self.namespace))
        assert isinstance(deployment, client.AppsV1beta1Deployment)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.create_namespaced_deployment', side_effect=client.rest.ApiException(status=409))
    def test_already_exists(self, mock_create_namespaced_deployment, mock_logging):
        deployment = create_mcrouter_deployment(self.cluster_object)

        mock_logging.debug.assert_called_once_with(
            'deploy/{}-router in ns/{} already exists'.format(self.name, self.namespace))
        assert deployment is False

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.create_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
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
    @patch('kubernetes.client.AppsV1beta1Api.patch_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    def test_success(self, mock_patch_namespaced_deployment, mock_logging):
        deployment = update_memcached_deployment(self.cluster_object)

        body = get_memcached_deployment_object(self.cluster_object)
        mock_patch_namespaced_deployment.assert_called_once_with(
            self.name, self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'updated deploy/{} in ns/{}'.format(self.name, self.namespace))
        assert isinstance(deployment, client.AppsV1beta1Deployment)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.patch_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
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
    @patch('kubernetes.client.AppsV1beta1Api.patch_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    def test_success(self, mock_patch_namespaced_deployment, mock_logging):
        deployment = update_mcrouter_deployment(self.cluster_object)

        body = get_mcrouter_deployment_object(self.cluster_object)
        mock_patch_namespaced_deployment.assert_called_once_with(
            '{}-router'.format(self.name), self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'updated deploy/{}-router in ns/{}'.format(self.name, self.namespace))
        assert isinstance(deployment, client.AppsV1beta1Deployment)

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.patch_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    def test_rest_exception(self, mock_patch_namespaced_deployment, mock_logging):
        deployment = update_mcrouter_deployment(self.cluster_object)

        assert mock_logging.exception.called is True
        assert deployment is False


class TestDeleteDeployment():
    def setUp(self):
        self.name = 'testdeployment123'
        self.namespace = 'testnamespace456'

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.delete_namespaced_deployment')
    def test_success(self, mock_delete_namespaced_deployment, mock_logging):
        response = delete_deployment(self.name, self.namespace)

        body = client.V1DeleteOptions(propagation_policy='Background')
        mock_delete_namespaced_deployment.assert_called_once_with(
            self.name, self.namespace, body)
        mock_logging.info.assert_called_once_with(
            'deleted deploy/{} from ns/{}'.format(self.name, self.namespace))
        assert response is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.delete_namespaced_deployment', side_effect=client.rest.ApiException(status=404))
    def test_nonexistent(self, mock_delete_namespaced_service, mock_logging):
        response = delete_deployment(self.name, self.namespace)

        mock_logging.debug.assert_called_once_with(
            'not deleting nonexistent deploy/{} from ns/{}'.format(self.name, self.namespace))
        assert response is True

    @patch('memcached_operator.memcached_operator.kubernetes_helpers.logging')
    @patch('kubernetes.client.AppsV1beta1Api.delete_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    def test_rest_exception(self, mock_delete_namespaced_service, mock_logging):
        response = delete_deployment(self.name, self.namespace)

        assert mock_logging.exception.called is True
        assert response is False
